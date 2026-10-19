package config

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"
)

const queueURL = "https://sqs.us-east-1.amazonaws.com/123456789012/demo"

func env(kv map[string]string) func(string) string {
	return func(k string) string { return kv[k] }
}

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load(env(map[string]string{"SQS_QUEUE_URL": queueURL}))
	if err != nil {
		t.Fatal(err)
	}
	want := Config{
		QueueURL: queueURL, WaitTimeSeconds: 20, MaxMessages: 10, VisibilityTimeout: 0,
		ProcessDelay: 2 * time.Second, Concurrency: 1, LogLevel: slog.LevelInfo, LogFormat: "json",
		HealthAddr: ":8080", ShutdownTimeout: 30 * time.Second,
	}
	if cfg != want {
		t.Errorf("got %+v\nwant %+v", cfg, want)
	}
}

func TestLoadOverrides(t *testing.T) {
	cfg, err := Load(env(map[string]string{
		"SQS_QUEUE_URL":          "http://localhost:4566/000000000000/demo",
		"SQS_WAIT_TIME_SECONDS":  "0",
		"SQS_MAX_MESSAGES":       "5",
		"SQS_VISIBILITY_TIMEOUT": "120",
		"PROCESS_DELAY":          "0s",
		"CONCURRENCY":            "4",
		"LOG_LEVEL":              "DEBUG",
		"LOG_FORMAT":             "Text",
		"HEALTH_ADDR":            "127.0.0.1:9090",
		"SHUTDOWN_TIMEOUT":       "1m",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.WaitTimeSeconds != 0 || cfg.MaxMessages != 5 || cfg.VisibilityTimeout != 120 || cfg.ProcessDelay != 0 ||
		cfg.Concurrency != 4 || cfg.LogLevel != slog.LevelDebug || cfg.LogFormat != "text" ||
		cfg.HealthAddr != "127.0.0.1:9090" || cfg.ShutdownTimeout != time.Minute {
		t.Errorf("overrides not applied: %+v", cfg)
	}
}

func TestLoadInvalid(t *testing.T) {
	tests := []struct {
		key, value, wantErr string
	}{
		{"SQS_QUEUE_URL", "", "SQS_QUEUE_URL is required"},
		{"SQS_QUEUE_URL", "demo-queue", "must be a queue URL"},
		{"SQS_QUEUE_URL", "ftp://sqs.us-east-1.amazonaws.com/123456789012/demo", "must be a queue URL"},
		{"SQS_QUEUE_URL", "https://sqs.us-east-1.amazonaws.com/", "must be a queue URL"},
		{"SQS_WAIT_TIME_SECONDS", "21", "SQS_WAIT_TIME_SECONDS \"21\": must be an integer between 0 and 20"},
		{"SQS_WAIT_TIME_SECONDS", "abc", "SQS_WAIT_TIME_SECONDS"},
		{"SQS_MAX_MESSAGES", "0", "between 1 and 10"},
		{"SQS_MAX_MESSAGES", "11", "between 1 and 10"},
		{"SQS_VISIBILITY_TIMEOUT", "-1", "between 0 and 43200"},
		{"CONCURRENCY", "0", "CONCURRENCY \"0\""},
		{"PROCESS_DELAY", "2", "must be a Go duration"},
		{"PROCESS_DELAY", "-1s", "must not be negative"},
		{"SHUTDOWN_TIMEOUT", "0s", "must be greater than zero"},
		{"LOG_LEVEL", "verbose", "LOG_LEVEL \"verbose\""},
		{"LOG_FORMAT", "yaml", "must be json or text"},
		{"HEALTH_ADDR", "8080", "must be host:port"},
	}
	for _, tt := range tests {
		t.Run(tt.key+"="+tt.value, func(t *testing.T) {
			vars := map[string]string{"SQS_QUEUE_URL": queueURL, tt.key: tt.value}
			_, err := Load(env(vars))
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("got error %v, want it to contain %q", err, tt.wantErr)
			}
		})
	}
}

func TestLoadReportsEveryError(t *testing.T) {
	_, err := Load(env(map[string]string{"SQS_MAX_MESSAGES": "0", "LOG_FORMAT": "xml"}))
	if err == nil {
		t.Fatal("expected an error")
	}
	for _, want := range []string{"SQS_QUEUE_URL", "SQS_MAX_MESSAGES", "LOG_FORMAT"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %s", err, want)
		}
	}
}

func TestNewLogger(t *testing.T) {
	tests := []struct {
		format, want string
	}{
		{"json", `"msg":"hello"`},
		{"text", `msg=hello`},
	}
	for _, tt := range tests {
		t.Run(tt.format, func(t *testing.T) {
			var buf bytes.Buffer
			Config{LogFormat: tt.format, LogLevel: slog.LevelInfo}.NewLogger(&buf).Info("hello")
			if !strings.Contains(buf.String(), tt.want) {
				t.Errorf("got %q, want it to contain %q", buf.String(), tt.want)
			}
		})
	}
}
