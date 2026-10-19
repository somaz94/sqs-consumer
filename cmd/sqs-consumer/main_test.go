package main

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/somaz94/sqs-consumer/internal/consumer"
)

const testQueueURL = "https://sqs.us-east-1.amazonaws.com/123456789012/demo"

// oneBatchAPI returns one message, then cancels the run as SIGTERM would.
type oneBatchAPI struct {
	mu      sync.Mutex
	served  bool
	cancel  context.CancelFunc
	deleted int
}

func (f *oneBatchAPI) ReceiveMessage(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.served {
		f.served = true
		return &sqs.ReceiveMessageOutput{Messages: []types.Message{{
			MessageId: aws.String("m-1"), Body: aws.String("hello"), ReceiptHandle: aws.String("rh-1"),
		}}}, nil
	}
	f.cancel()
	return nil, ctx.Err()
}

func (f *oneBatchAPI) DeleteMessageBatch(_ context.Context, in *sqs.DeleteMessageBatchInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageBatchOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleted += len(in.Entries)
	return &sqs.DeleteMessageBatchOutput{}, nil
}

func baseEnv(extra map[string]string) func(string) string {
	vars := map[string]string{
		"SQS_QUEUE_URL": testQueueURL,
		"PROCESS_DELAY": "1ms",
		"HEALTH_ADDR":   "127.0.0.1:0",
		"LOG_FORMAT":    "text",
	}
	for k, v := range extra {
		vars[k] = v
	}
	return func(k string) string { return vars[k] }
}

func TestRunProcessesAndExitsCleanly(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	api := &oneBatchAPI{cancel: cancel}
	factory := func(context.Context, string) (consumer.API, string, error) { return api, "us-east-1", nil }
	var out bytes.Buffer

	if err := run(ctx, nil, baseEnv(nil), &out, factory); err != nil {
		t.Fatalf("run: %v", err)
	}
	if api.deleted != 1 {
		t.Errorf("deleted %d messages, want 1", api.deleted)
	}
	for _, want := range []string{"starting sqs-consumer", "message processed", "draining", "exiting"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("output missing %q:\n%s", want, out.String())
		}
	}
}

func TestRunErrors(t *testing.T) {
	busy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = busy.Close() }()

	okFactory := func(context.Context, string) (consumer.API, string, error) { return &oneBatchAPI{}, "us-east-1", nil }
	tests := []struct {
		name    string
		args    []string
		env     map[string]string
		factory clientFactory
		wantErr string
	}{
		{name: "unknown flag", args: []string{"-nope"}, factory: okFactory, wantErr: "flag provided but not defined"},
		{name: "invalid config", env: map[string]string{"SQS_MAX_MESSAGES": "50"}, factory: okFactory, wantErr: "SQS_MAX_MESSAGES"},
		{
			name: "client error",
			factory: func(context.Context, string) (consumer.API, string, error) {
				return nil, "", errors.New("no region")
			},
			wantErr: "no region",
		},
		{name: "health port in use", env: map[string]string{"HEALTH_ADDR": busy.Addr().String()}, factory: okFactory, wantErr: "health server"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := run(context.Background(), tt.args, baseEnv(tt.env), &bytes.Buffer{}, tt.factory)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("got %v, want an error containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestRunVersion(t *testing.T) {
	version, commit, date = "v1.2.3", "abc1234", "2026-01-01T00:00:00Z"
	t.Cleanup(func() { version, commit, date = "dev", "unknown", "unknown" })
	var out bytes.Buffer

	if err := run(context.Background(), []string{"-version"}, func(string) string { return "" }, &out, nil); err != nil {
		t.Fatal(err)
	}
	if want := "sqs-consumer v1.2.3 (commit abc1234, built 2026-01-01T00:00:00Z)\n"; out.String() != want {
		t.Errorf("got %q, want %q", out.String(), want)
	}
}

func TestRegionFromQueueURL(t *testing.T) {
	tests := map[string]string{
		testQueueURL: "us-east-1",
		"https://sqs.cn-north-1.amazonaws.com.cn/123456789012/demo": "cn-north-1",
		"http://localhost:4566/000000000000/demo":                   "",
		"https://sqs.amazonaws.com/123456789012/demo":               "",
		"://bad": "",
	}
	for in, want := range tests {
		if got := regionFromQueueURL(in); got != want {
			t.Errorf("regionFromQueueURL(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestNewSQSClient(t *testing.T) {
	tests := []struct {
		name       string
		region     string
		profile    string
		queueURL   string
		wantRegion string
		wantErr    string
	}{
		{name: "region from environment", region: "eu-west-1", queueURL: testQueueURL, wantRegion: "eu-west-1"},
		{name: "region from queue URL", queueURL: testQueueURL, wantRegion: "us-east-1"},
		{name: "no region anywhere", queueURL: "http://localhost:4566/000000000000/demo", wantErr: "AWS region not found"},
		{name: "broken shared config", profile: "missing", queueURL: testQueueURL, wantErr: "load AWS config"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isolateAWSEnv(t)
			t.Setenv("AWS_REGION", tt.region)
			t.Setenv("AWS_PROFILE", tt.profile)

			client, region, err := newSQSClient(context.Background(), tt.queueURL)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("got %v, want an error containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil || client == nil || region != tt.wantRegion {
				t.Fatalf("got client=%v region=%q err=%v, want region %q", client, region, err, tt.wantRegion)
			}
		})
	}
}

// isolateAWSEnv keeps the developer's ~/.aws and AWS_* variables out of the test.
func isolateAWSEnv(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	for _, kv := range os.Environ() {
		if k, _, _ := strings.Cut(kv, "="); strings.HasPrefix(k, "AWS_") {
			t.Setenv(k, "")
		}
	}
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(dir, "config"))
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(dir, "credentials"))
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
}

func TestShutdownIsBounded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	api := &oneBatchAPI{cancel: cancel}
	factory := func(context.Context, string) (consumer.API, string, error) { return api, "us-east-1", nil }
	time.AfterFunc(100*time.Millisecond, cancel)
	start := time.Now()

	if err := run(ctx, nil, baseEnv(map[string]string{"PROCESS_DELAY": "1h", "SHUTDOWN_TIMEOUT": "100ms"}), &bytes.Buffer{}, factory); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > 5*time.Second {
		t.Errorf("shutdown took %s", time.Since(start))
	}
	if api.deleted != 0 {
		t.Errorf("deleted %d messages, want 0 for unfinished work", api.deleted)
	}
}
