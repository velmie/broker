// Command awsorders traces three serial typed SQS and SNS-to-SQS routes on a local emulator.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	nativeSNS "github.com/aws/aws-sdk-go-v2/service/sns"
	nativeSQS "github.com/aws/aws-sdk-go-v2/service/sqs"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

const (
	endpointOperation = "endpoint"
	directRoute       = "sqs"
	rawRoute          = "sns-raw"
	envelopeRoute     = "sns-envelope"
	sourceName        = "orders"
	operationBudget   = 5 * time.Second
	routeBudget       = 30 * time.Second
	recoveryBackoff   = 10 * time.Millisecond
	telemetryBudget   = 2 * time.Second
	exampleID         = "order-example-001"
	exampleAmount     = 42
)

func main() {
	endpoint := flag.String(endpointOperation, "", "required loopback HTTP emulator endpoint")
	flag.Parse()
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{ReplaceAttr: redactAttribute}))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	exporter, err := stdouttrace.New()
	if err == nil {
		provider := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exporter))
		err = run(ctx, *endpoint, provider, logger)
	}
	stop()
	if err != nil {
		logger.Error("AWS orders failed", slog.Any("error", err))
		os.Exit(1)
	}
}

type order struct {
	Amount int `json:"amount"`
}

func run(ctx context.Context, endpoint string, provider *sdktrace.TracerProvider, logger *slog.Logger) (result error) {
	defer func() { result = errors.Join(result, finishTelemetry(provider)) }()
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return &commandError{Operation: endpointOperation, Cause: err}
	}
	if parsed.Scheme != "http" || !net.ParseIP(parsed.Hostname()).IsLoopback() || parsed.User != nil ||
		parsed.RawQuery != "" || parsed.Fragment != "" {
		return &commandError{Operation: endpointOperation,
			Cause: errors.New("expected a loopback HTTP URL without credentials, query or fragment")}
	}
	queues, topics := newClients(endpoint)
	for _, route := range []string{directRoute, rawRoute, envelopeRoute} {
		if routeErr := runRoute(ctx, queues, topics, route, provider.Tracer("broker.awsorders.example"), logger); routeErr != nil {
			return routeErr
		}
	}
	return nil
}

func newClients(endpoint string) (queues *nativeSQS.Client, topics *nativeSNS.Client) {
	config := aws.Config{Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("synthetic", "synthetic", "")}
	queues = nativeSQS.NewFromConfig(config, func(o *nativeSQS.Options) { o.BaseEndpoint = aws.String(endpoint); o.RetryMaxAttempts = 1 })
	topics = nativeSNS.NewFromConfig(config, func(o *nativeSNS.Options) { o.BaseEndpoint = aws.String(endpoint); o.RetryMaxAttempts = 1 })
	return queues, topics
}

func finishTelemetry(provider *sdktrace.TracerProvider) error {
	flush, cancelFlush := context.WithTimeout(context.Background(), telemetryBudget)
	flushErr := provider.ForceFlush(flush)
	cancelFlush()
	shutdown, cancelShutdown := context.WithTimeout(context.Background(), telemetryBudget)
	shutdownErr := provider.Shutdown(shutdown)
	cancelShutdown()
	return errors.Join(wrapCommand("flush", flushErr), wrapCommand("shutdown", shutdownErr))
}
