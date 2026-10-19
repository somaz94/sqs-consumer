// Command sqs-consumer long-polls an SQS queue, simulates work per message and deletes what it processed.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/somaz94/sqs-consumer/internal/config"
	"github.com/somaz94/sqs-consumer/internal/consumer"
	"github.com/somaz94/sqs-consumer/internal/health"
)

// Set by -ldflags at build time.
var (
	version = "dev"
	commit  = "unknown"
	date    = "unknown"
)

const httpShutdownTimeout = 5 * time.Second

type clientFactory func(ctx context.Context, queueURL string) (consumer.API, string, error)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	// Restore default signal handling once draining starts, so a second Ctrl-C kills at once.
	context.AfterFunc(ctx, stop)

	if err := run(ctx, os.Args[1:], os.Getenv, os.Stdout, newSQSClient); err != nil {
		fmt.Fprintln(os.Stderr, "sqs-consumer:", err)
		os.Exit(1)
	}
}

func run(
	ctx context.Context, args []string, getenv func(string) string, stdout io.Writer, newClient clientFactory,
) error {
	fs := flag.NewFlagSet("sqs-consumer", flag.ContinueOnError)
	fs.SetOutput(stdout)
	showVersion := fs.Bool("version", false, "print version information and exit")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *showVersion {
		_, err := fmt.Fprintln(stdout, versionString())
		return err
	}

	cfg, err := config.Load(getenv)
	if err != nil {
		return err
	}
	log := cfg.NewLogger(stdout)
	log.Info("starting sqs-consumer", "version", version, "commit", commit, "date", date)

	client, region, err := newClient(ctx, cfg.QueueURL)
	if err != nil {
		return err
	}

	ln, err := net.Listen("tcp", cfg.HealthAddr)
	if err != nil {
		return fmt.Errorf("health server: %w", err)
	}
	state := &health.State{}
	srv := &http.Server{Handler: health.Handler(state), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("health server stopped", "error", err)
		}
	}()
	log.Info("health server listening", "addr", ln.Addr().String(), "region", region)

	draining := make(chan struct{})
	go func() {
		defer close(draining)
		<-ctx.Done()
		state.MarkShuttingDown()
		log.Info("shutdown signal received, draining", "shutdown_timeout", cfg.ShutdownTimeout.String())
	}()

	consumer.New(client, consumer.Options{
		QueueURL:          cfg.QueueURL,
		WaitTimeSeconds:   cfg.WaitTimeSeconds,
		MaxMessages:       cfg.MaxMessages,
		VisibilityTimeout: cfg.VisibilityTimeout,
		Concurrency:       cfg.Concurrency,
		ShutdownTimeout:   cfg.ShutdownTimeout,
	}, consumer.DemoHandler(cfg.ProcessDelay), state, log).Run(ctx)
	// Run returns only after ctx is done, so this cannot block.
	<-draining

	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), httpShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(sctx); err != nil {
		log.Warn("health server shutdown", "error", err)
	}
	log.Info("exiting")
	return nil
}

func versionString() string {
	return fmt.Sprintf("sqs-consumer %s (commit %s, built %s)", version, commit, date)
}

// newSQSClient uses the default credential chain only, which covers IRSA and EKS Pod Identity.
func newSQSClient(ctx context.Context, queueURL string) (consumer.API, string, error) {
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, "", fmt.Errorf("load AWS config: %w", err)
	}
	if awsCfg.Region == "" {
		awsCfg.Region = regionFromQueueURL(queueURL)
	}
	if awsCfg.Region == "" {
		return nil, "", errors.New(
			"AWS region not found: set AWS_REGION or use an https://sqs.<region>.amazonaws.com/... queue URL")
	}
	return sqs.NewFromConfig(awsCfg), awsCfg.Region, nil
}

// regionFromQueueURL reads the region out of an sqs.<region>.amazonaws.com host.
func regionFromQueueURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	parts := strings.Split(u.Hostname(), ".")
	if len(parts) >= 4 && parts[0] == "sqs" && parts[2] == "amazonaws" {
		return parts[1]
	}
	return ""
}
