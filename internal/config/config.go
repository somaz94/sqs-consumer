// Package config loads and validates the consumer settings from environment variables.
package config

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Config holds every setting the consumer reads from the environment.
type Config struct {
	QueueURL          string
	WaitTimeSeconds   int32
	MaxMessages       int32
	VisibilityTimeout int32
	ProcessDelay      time.Duration
	Concurrency       int
	LogLevel          slog.Level
	LogFormat         string
	HealthAddr        string
	ShutdownTimeout   time.Duration
}

// maxVisibilityTimeout is the SQS upper bound (12 hours).
const maxVisibilityTimeout = 43200

// Load reads the configuration through getenv and returns every invalid value at once.
func Load(getenv func(string) string) (Config, error) {
	var errs []error
	get := func(key, def string) string {
		if v := strings.TrimSpace(getenv(key)); v != "" {
			return v
		}
		return def
	}

	cfg := Config{
		QueueURL:   get("SQS_QUEUE_URL", ""),
		LogFormat:  strings.ToLower(get("LOG_FORMAT", "json")),
		HealthAddr: get("HEALTH_ADDR", ":8080"),
	}

	if err := validateQueueURL(cfg.QueueURL); err != nil {
		errs = append(errs, err)
	}

	cfg.WaitTimeSeconds = int32(intInRange(&errs, "SQS_WAIT_TIME_SECONDS", get("SQS_WAIT_TIME_SECONDS", "20"), 0, 20))
	cfg.MaxMessages = int32(intInRange(&errs, "SQS_MAX_MESSAGES", get("SQS_MAX_MESSAGES", "10"), 1, 10))
	cfg.VisibilityTimeout = int32(intInRange(&errs, "SQS_VISIBILITY_TIMEOUT",
		get("SQS_VISIBILITY_TIMEOUT", "0"), 0, maxVisibilityTimeout))
	// A received batch holds at most 10 messages, so more workers would sit idle.
	cfg.Concurrency = intInRange(&errs, "CONCURRENCY", get("CONCURRENCY", "1"), 1, 10)

	cfg.ProcessDelay = duration(&errs, "PROCESS_DELAY", get("PROCESS_DELAY", "2s"), false)
	cfg.ShutdownTimeout = duration(&errs, "SHUTDOWN_TIMEOUT", get("SHUTDOWN_TIMEOUT", "30s"), true)

	level := get("LOG_LEVEL", "info")
	if err := cfg.LogLevel.UnmarshalText([]byte(level)); err != nil {
		errs = append(errs, fmt.Errorf("LOG_LEVEL %q: must be one of debug, info, warn, error", level))
	}

	if cfg.LogFormat != "json" && cfg.LogFormat != "text" {
		errs = append(errs, fmt.Errorf("LOG_FORMAT %q: must be json or text", cfg.LogFormat))
	}

	if _, _, err := net.SplitHostPort(cfg.HealthAddr); err != nil {
		errs = append(errs, fmt.Errorf("HEALTH_ADDR %q: must be host:port, e.g. :8080", cfg.HealthAddr))
	}

	if err := errors.Join(errs...); err != nil {
		return Config{}, fmt.Errorf("invalid configuration:\n%w", err)
	}
	return cfg, nil
}

// NewLogger builds the slog logger the configuration asks for.
func (c Config) NewLogger(w io.Writer) *slog.Logger {
	opts := &slog.HandlerOptions{Level: c.LogLevel}
	if c.LogFormat == "text" {
		return slog.New(slog.NewTextHandler(w, opts))
	}
	return slog.New(slog.NewJSONHandler(w, opts))
}

func validateQueueURL(raw string) error {
	if raw == "" {
		return errors.New("SQS_QUEUE_URL is required")
	}
	u, err := url.Parse(raw)
	// http is allowed for local emulators such as LocalStack or ElasticMQ.
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" || strings.Trim(u.Path, "/") == "" {
		return fmt.Errorf("SQS_QUEUE_URL %q: must be a queue URL like https://sqs.<region>.amazonaws.com/<account-id>/<queue>", raw)
	}
	return nil
}

func intInRange(errs *[]error, key, raw string, lo, hi int) int {
	n, err := strconv.Atoi(raw)
	if err != nil || n < lo || n > hi {
		*errs = append(*errs, fmt.Errorf("%s %q: must be an integer between %d and %d", key, raw, lo, hi))
		return 0
	}
	return n
}

func duration(errs *[]error, key, raw string, positive bool) time.Duration {
	d, err := time.ParseDuration(raw)
	switch {
	case err != nil:
		*errs = append(*errs, fmt.Errorf("%s %q: must be a Go duration such as 500ms, 2s or 1m", key, raw))
	case positive && d <= 0:
		*errs = append(*errs, fmt.Errorf("%s %q: must be greater than zero", key, raw))
	case d < 0:
		*errs = append(*errs, fmt.Errorf("%s %q: must not be negative", key, raw))
	}
	return d
}
