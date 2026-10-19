package consumer

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type receiveResult struct {
	msgs []types.Message
	err  error
}

// fakeAPI replays scripted receive results, then cancels the run and blocks like a long poll.
type fakeAPI struct {
	mu        sync.Mutex
	script    []receiveResult
	cancel    context.CancelFunc
	inputs    []*sqs.ReceiveMessageInput
	deletes   [][]types.DeleteMessageBatchRequestEntry
	deleteErr error
	failIDs   []string
}

func (f *fakeAPI) ReceiveMessage(ctx context.Context, in *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	f.mu.Lock()
	f.inputs = append(f.inputs, in)
	if len(f.script) > 0 {
		r := f.script[0]
		f.script = f.script[1:]
		f.mu.Unlock()
		return &sqs.ReceiveMessageOutput{Messages: r.msgs}, r.err
	}
	f.mu.Unlock()
	if f.cancel != nil {
		f.cancel()
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

func (f *fakeAPI) DeleteMessageBatch(ctx context.Context, in *sqs.DeleteMessageBatchInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageBatchOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	f.deletes = append(f.deletes, in.Entries)
	if f.deleteErr != nil {
		return nil, f.deleteErr
	}
	out := &sqs.DeleteMessageBatchOutput{}
	for _, e := range in.Entries {
		failed := false
		for _, id := range f.failIDs {
			failed = failed || aws.ToString(e.Id) == id
		}
		if failed {
			out.Failed = append(out.Failed, types.BatchResultErrorEntry{Id: e.Id, Code: aws.String("ReceiptHandleIsInvalid")})
			continue
		}
		out.Successful = append(out.Successful, types.DeleteMessageBatchResultEntry{Id: e.Id})
	}
	return out, nil
}

func (f *fakeAPI) deletedHandles() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, batch := range f.deletes {
		for _, e := range batch {
			out = append(out, aws.ToString(e.ReceiptHandle))
		}
	}
	return out
}

type readyCounter struct{ n atomic.Int32 }

func (r *readyCounter) MarkReady() { r.n.Add(1) }

func msg(id, body string) types.Message {
	return types.Message{MessageId: aws.String(id), Body: aws.String(body), ReceiptHandle: aws.String("rh-" + id)}
}

func testOptions() Options {
	return Options{QueueURL: "https://sqs.us-east-1.amazonaws.com/123456789012/demo", WaitTimeSeconds: 20,
		MaxMessages: 10, Concurrency: 2, ShutdownTimeout: 5 * time.Second}
}

func newTest(api API, opts Options, h Handler, logs io.Writer) (*Consumer, *readyCounter) {
	ready := &readyCounter{}
	c := New(api, opts, h, ready, slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	c.backoff = func(int) time.Duration { return 0 }
	return c, ready
}

func runWithTimeout(t *testing.T, ctx context.Context, c *Consumer) {
	t.Helper()
	done := make(chan struct{})
	go func() { c.Run(ctx); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return")
	}
}

func TestRun(t *testing.T) {
	tests := []struct {
		name        string
		script      []receiveResult
		deleteErr   error
		failIDs     []string
		wantDeleted []string
		wantReady   bool
		wantLog     string
	}{
		{
			name:        "deletes processed messages",
			script:      []receiveResult{{msgs: []types.Message{msg("a", "hello"), msg("b", "world")}}},
			wantDeleted: []string{"rh-a", "rh-b"},
			wantReady:   true,
			wantLog:     "message processed",
		},
		{
			name:        "never deletes a failed message",
			script:      []receiveResult{{msgs: []types.Message{msg("a", "ok"), msg("b", FailBody)}}},
			wantDeleted: []string{"rh-a"},
			wantReady:   true,
			wantLog:     "left on the queue",
		},
		{
			name:      "no delete call when every message failed",
			script:    []receiveResult{{msgs: []types.Message{msg("a", FailBody)}}},
			wantReady: true,
		},
		{
			name:      "empty receive marks ready without deleting",
			script:    []receiveResult{{}},
			wantReady: true,
			wantLog:   "no messages",
		},
		{
			name:        "receive error is retried and readiness waits for success",
			script:      []receiveResult{{err: errors.New("throttled")}, {msgs: []types.Message{msg("a", "ok")}}},
			wantDeleted: []string{"rh-a"},
			wantReady:   true,
			wantLog:     "receive failed",
		},
		{
			name:      "receive error only never marks ready",
			script:    []receiveResult{{err: errors.New("access denied")}},
			wantReady: false,
			wantLog:   "receive failed",
		},
		{
			name:        "delete call error is logged",
			script:      []receiveResult{{msgs: []types.Message{msg("a", "ok")}}},
			deleteErr:   errors.New("boom"),
			wantDeleted: []string{"rh-a"},
			wantReady:   true,
			wantLog:     "delete failed, processed messages will be redelivered",
		},
		{
			name:        "partial delete failure is logged per message",
			script:      []receiveResult{{msgs: []types.Message{msg("a", "ok"), msg("b", "ok")}}},
			failIDs:     []string{"1"},
			wantDeleted: []string{"rh-a", "rh-b"},
			wantReady:   true,
			wantLog:     `delete failed for message" message_id=b`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			api := &fakeAPI{script: tt.script, cancel: cancel, deleteErr: tt.deleteErr, failIDs: tt.failIDs}
			var logs bytes.Buffer
			c, ready := newTest(api, testOptions(), DemoHandler(time.Millisecond), &logs)

			runWithTimeout(t, ctx, c)

			got := api.deletedHandles()
			if strings.Join(got, ",") != strings.Join(tt.wantDeleted, ",") {
				t.Errorf("deleted %v, want %v", got, tt.wantDeleted)
			}
			if (ready.n.Load() > 0) != tt.wantReady {
				t.Errorf("ready marked %d times, want ready=%v", ready.n.Load(), tt.wantReady)
			}
			if tt.wantLog != "" && !strings.Contains(logs.String(), tt.wantLog) {
				t.Errorf("logs missing %q:\n%s", tt.wantLog, logs.String())
			}
		})
	}
}

func TestReceiveInput(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	api := &fakeAPI{cancel: cancel}
	opts := testOptions()
	opts.WaitTimeSeconds, opts.MaxMessages, opts.VisibilityTimeout = 5, 3, 60
	c, _ := newTest(api, opts, DemoHandler(0), io.Discard)

	runWithTimeout(t, ctx, c)

	in := api.inputs[0]
	if aws.ToString(in.QueueUrl) != opts.QueueURL || in.WaitTimeSeconds != 5 || in.MaxNumberOfMessages != 3 || in.VisibilityTimeout != 60 {
		t.Errorf("unexpected receive input: %+v", in)
	}
	if len(in.MessageSystemAttributeNames) != 1 {
		t.Errorf("expected ApproximateReceiveCount to be requested, got %v", in.MessageSystemAttributeNames)
	}
}

func TestShutdown(t *testing.T) {
	tests := []struct {
		name            string
		shutdownTimeout time.Duration
		work            time.Duration
		wantDeleted     int
	}{
		{name: "in-flight message finishes and is deleted", shutdownTimeout: 5 * time.Second, work: 100 * time.Millisecond, wantDeleted: 1},
		{name: "message unfinished at the timeout stays on the queue", shutdownTimeout: 50 * time.Millisecond, work: time.Hour, wantDeleted: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			api := &fakeAPI{}
			started := make(chan struct{})
			handler := func(ctx context.Context, m types.Message) error {
				close(started)
				return DemoHandler(tt.work)(ctx, m)
			}
			opts := testOptions()
			opts.ShutdownTimeout = tt.shutdownTimeout
			api.script = []receiveResult{{msgs: []types.Message{msg("a", "ok")}}}
			c, _ := newTest(api, opts, handler, io.Discard)

			go func() { <-started; cancel() }()
			start := time.Now()
			runWithTimeout(t, ctx, c)

			if got := len(api.deletedHandles()); got != tt.wantDeleted {
				t.Errorf("deleted %d messages, want %d", got, tt.wantDeleted)
			}
			if elapsed := time.Since(start); elapsed > tt.shutdownTimeout+2*time.Second {
				t.Errorf("shutdown took %s, longer than the timeout %s", elapsed, tt.shutdownTimeout)
			}
		})
	}
}

func TestShutdownDuringBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	api := &fakeAPI{script: []receiveResult{{err: errors.New("unreachable")}}}
	c, _ := newTest(api, testOptions(), DemoHandler(0), io.Discard)
	c.backoff = func(int) time.Duration { return time.Hour }
	time.AfterFunc(50*time.Millisecond, cancel)

	runWithTimeout(t, ctx, c)
}

func TestConcurrencyLimit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	msgs := make([]types.Message, 0, 6)
	for _, id := range []string{"a", "b", "c", "d", "e", "f"} {
		msgs = append(msgs, msg(id, "ok"))
	}
	api := &fakeAPI{script: []receiveResult{{msgs: msgs}}, cancel: cancel}
	var active, peak atomic.Int32
	handler := func(ctx context.Context, m types.Message) error {
		n := active.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		defer active.Add(-1)
		return DemoHandler(20*time.Millisecond)(ctx, m)
	}
	opts := testOptions()
	opts.Concurrency = 3
	c, _ := newTest(api, opts, handler, io.Discard)

	runWithTimeout(t, ctx, c)

	if p := peak.Load(); p != 3 {
		t.Errorf("peak concurrency %d, want 3", p)
	}
	if got := len(api.deletedHandles()); got != 6 {
		t.Errorf("deleted %d messages, want 6", got)
	}
}

func TestDemoHandler(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	tests := []struct {
		name    string
		ctx     context.Context
		body    string
		wantErr error
	}{
		{name: "regular body succeeds", ctx: context.Background(), body: `{"order":1}`},
		{name: "fail body fails", ctx: context.Background(), body: FailBody, wantErr: ErrSimulatedFailure},
		{name: "fail body must match exactly", ctx: context.Background(), body: `{"fail": true}`},
		{name: "cancelled context stops the work", ctx: cancelled, body: "ok", wantErr: context.Canceled},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			delay := time.Millisecond
			if tt.wantErr == context.Canceled {
				delay = time.Hour
			}
			err := DemoHandler(delay)(tt.ctx, msg("a", tt.body))
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("got %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestDefaultBackoff(t *testing.T) {
	tests := []struct {
		failures int
		want     time.Duration
	}{
		{1, time.Second}, {2, 2 * time.Second}, {5, 16 * time.Second}, {6, 30 * time.Second}, {50, 30 * time.Second},
	}
	for _, tt := range tests {
		if got := defaultBackoff(tt.failures); got != tt.want {
			t.Errorf("defaultBackoff(%d) = %s, want %s", tt.failures, got, tt.want)
		}
	}
}

func TestNewClampsConcurrency(t *testing.T) {
	c := New(&fakeAPI{}, Options{}, DemoHandler(0), &readyCounter{}, slog.New(slog.DiscardHandler))
	if c.opts.Concurrency != 1 {
		t.Errorf("concurrency %d, want 1", c.opts.Concurrency)
	}
}
