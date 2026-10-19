// Package consumer runs the SQS receive, process and delete loop.
package consumer

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// API is the subset of the SQS client the consumer uses.
type API interface {
	ReceiveMessage(ctx context.Context, in *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessageBatch(ctx context.Context, in *sqs.DeleteMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageBatchOutput, error)
}

// ReadyMarker is told when a receive call first succeeds.
type ReadyMarker interface {
	MarkReady()
}

// Handler processes one message. A non-nil error leaves the message on the queue.
type Handler func(ctx context.Context, msg types.Message) error

// Options configures a Consumer.
type Options struct {
	QueueURL          string
	WaitTimeSeconds   int32
	MaxMessages       int32
	VisibilityTimeout int32
	Concurrency       int
	ShutdownTimeout   time.Duration
}

// FailBody is the message body DemoHandler rejects, to demo redelivery and a DLQ.
const FailBody = `{"fail":true}`

// ErrSimulatedFailure is returned by DemoHandler for FailBody.
var ErrSimulatedFailure = errors.New("simulated processing failure")

// deleteTimeout bounds the delete call, which runs detached from shutdown so finished work is not redelivered.
const deleteTimeout = 5 * time.Second

// Consumer polls one queue until its context is cancelled.
type Consumer struct {
	api     API
	opts    Options
	handler Handler
	ready   ReadyMarker
	log     *slog.Logger
	backoff func(failures int) time.Duration
}

// New returns a Consumer. Concurrency below 1 is treated as 1.
func New(api API, opts Options, handler Handler, ready ReadyMarker, log *slog.Logger) *Consumer {
	if opts.Concurrency < 1 {
		opts.Concurrency = 1
	}
	return &Consumer{api: api, opts: opts, handler: handler, ready: ready, log: log, backoff: defaultBackoff}
}

// DemoHandler simulates work by sleeping delay, and fails on FailBody.
func DemoHandler(delay time.Duration) Handler {
	return func(ctx context.Context, msg types.Message) error {
		if aws.ToString(msg.Body) == FailBody {
			return ErrSimulatedFailure
		}
		t := time.NewTimer(delay)
		defer t.Stop()
		select {
		case <-t.C:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func defaultBackoff(failures int) time.Duration {
	d := time.Second << min(failures-1, 5)
	return min(d, 30*time.Second)
}

// Run receives batches until ctx is cancelled, then waits for the batch in flight.
// That batch gets ShutdownTimeout to finish; unfinished messages stay on the queue.
func (c *Consumer) Run(ctx context.Context) {
	work, cancelWork := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelWork()
	go c.cancelAfterGrace(ctx, work, cancelWork)

	c.log.Info("consumer started", "queue_url", c.opts.QueueURL,
		"max_messages", c.opts.MaxMessages, "wait_time_seconds", c.opts.WaitTimeSeconds,
		"concurrency", c.opts.Concurrency)

	failures := 0
	for ctx.Err() == nil {
		out, err := c.api.ReceiveMessage(ctx, c.receiveInput())
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			failures++
			wait := c.backoff(failures)
			c.log.Error("receive failed", "error", err, "retry_in", wait.String())
			sleep(ctx, wait)
			continue
		}
		failures = 0
		c.ready.MarkReady()
		if len(out.Messages) == 0 {
			c.log.Debug("no messages")
			continue
		}
		c.handleBatch(work, out.Messages)
	}
	c.log.Info("consumer stopped")
}

func (c *Consumer) cancelAfterGrace(ctx, work context.Context, cancelWork context.CancelFunc) {
	select {
	case <-ctx.Done():
	case <-work.Done():
		return
	}
	t := time.NewTimer(c.opts.ShutdownTimeout)
	defer t.Stop()
	select {
	case <-t.C:
		c.log.Warn("shutdown timeout reached, abandoning in-flight messages",
			"shutdown_timeout", c.opts.ShutdownTimeout.String())
		cancelWork()
	case <-work.Done():
	}
}

func (c *Consumer) receiveInput() *sqs.ReceiveMessageInput {
	return &sqs.ReceiveMessageInput{
		QueueUrl:                    aws.String(c.opts.QueueURL),
		MaxNumberOfMessages:         c.opts.MaxMessages,
		WaitTimeSeconds:             c.opts.WaitTimeSeconds,
		VisibilityTimeout:           c.opts.VisibilityTimeout,
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameApproximateReceiveCount},
	}
}

func (c *Consumer) handleBatch(ctx context.Context, msgs []types.Message) {
	done := make([]bool, len(msgs))
	sem := make(chan struct{}, c.opts.Concurrency)
	var wg sync.WaitGroup
	for i, msg := range msgs {
		sem <- struct{}{}
		wg.Go(func() {
			defer func() { <-sem }()
			done[i] = c.process(ctx, msg)
		})
	}
	wg.Wait()
	c.deleteDone(ctx, msgs, done)
}

func (c *Consumer) process(ctx context.Context, msg types.Message) bool {
	log := c.log.With("message_id", aws.ToString(msg.MessageId),
		"body_bytes", len(aws.ToString(msg.Body)),
		"receive_count", msg.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)])
	start := time.Now()
	if err := c.handler(ctx, msg); err != nil {
		log.Warn("message failed, left on the queue for redelivery", "error", err)
		return false
	}
	log.Info("message processed", "duration", time.Since(start).String())
	return true
}

func (c *Consumer) deleteDone(ctx context.Context, msgs []types.Message, done []bool) {
	var entries []types.DeleteMessageBatchRequestEntry
	messageIDs := make(map[string]string, len(msgs))
	for i, msg := range msgs {
		if done[i] {
			id := strconv.Itoa(i)
			messageIDs[id] = aws.ToString(msg.MessageId)
			entries = append(entries, types.DeleteMessageBatchRequestEntry{Id: aws.String(id), ReceiptHandle: msg.ReceiptHandle})
		}
	}
	if len(entries) == 0 {
		return
	}

	dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), deleteTimeout)
	defer cancel()
	out, err := c.api.DeleteMessageBatch(dctx, &sqs.DeleteMessageBatchInput{
		QueueUrl: aws.String(c.opts.QueueURL),
		Entries:  entries,
	})
	if err != nil {
		c.log.Error("delete failed, processed messages will be redelivered", "error", err, "count", len(entries))
		return
	}
	for _, f := range out.Failed {
		c.log.Error("delete failed for message", "message_id", messageIDs[aws.ToString(f.Id)],
			"code", aws.ToString(f.Code), "reason", aws.ToString(f.Message))
	}
	c.log.Debug("deleted messages", "count", len(out.Successful))
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-ctx.Done():
	}
}
