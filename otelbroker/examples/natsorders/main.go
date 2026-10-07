// Command natsorders traces command publication, processing, receipt publication
// and native completion using an application-owned OpenTelemetry provider.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/velmie/broker"
	"github.com/velmie/broker/natsjs/v3"
	"github.com/velmie/broker/natsjs/v3/examples/orders"
	"github.com/velmie/broker/otelbroker"
)

const consumerName = "worker"

func main() {
	if err := runCommand(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runCommand() error {
	url := flag.String("url", nats.DefaultURL, "NATS server with JetStream enabled")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	exporter, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
	if err == nil {
		provider := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exporter))
		err = run(ctx, *url, provider)
	}
	return err
}

// run owns provider flush/shutdown after all consumer work has joined.
func run(ctx context.Context, url string, provider *sdktrace.TracerProvider) (result error) {
	defer func() { result = errors.Join(result, finishTelemetry(provider)) }()
	ctx, stop := context.WithCancel(ctx)
	defer stop()
	tracer := provider.Tracer("broker.orders.example")
	traceOptions := []otelbroker.Option{otelbroker.WithTracer(tracer), otelbroker.WithPropagator(propagation.TraceContext{})}

	nc, err := nats.Connect(url, nats.FlusherTimeout(time.Second))
	if err != nil {
		return err
	}
	defer nc.Close() // The connection owner closes it after coordinator.Run joins.
	js, err := nc.JetStream()
	if err != nil {
		return err
	}
	stream := "BROKER_EXAMPLE_" + nuid.Next()
	subject := stream + ".orders"
	replySubject := stream + ".created"
	// Provisioning is explicit application wiring, separate from Consumer.Run.
	if _, err = js.AddStream(&nats.StreamConfig{Name: stream, Subjects: []string{subject, replySubject},
		Storage: nats.MemoryStorage}); err != nil {
		return err
	}
	defer func() { result = errors.Join(result, js.DeleteStream(stream)) }()
	remote := &nats.ConsumerConfig{Durable: consumerName, FilterSubject: subject, AckPolicy: nats.AckExplicitPolicy}
	if _, err = js.AddConsumer(stream, remote); err != nil {
		return err
	}
	config := natsjs.ConsumerConfig{
		Stream: stream, Consumer: consumerName, Subject: subject, Shutdown: broker.ShutdownPolicy{GracePeriod: time.Second},
	}
	config.Observer = newObserver(tracer)
	record := config.Observer.Record
	config.Observer.Record = func(ctx context.Context, event natsjs.Observation) {
		record(ctx, event)
		if event.Operation == natsjs.OperationAck && event.Outcome == natsjs.OutcomeConfirmed {
			fmt.Println("ack confirmed")
			stop()
		}
	}
	replies, err := newReceiptPublisher(nc, replySubject, traceOptions)
	if err != nil {
		return err
	}
	handler := orders.NewReplyHandler(func(_ context.Context, command orders.CreateOrderCommand) error {
		fmt.Printf("created order %s: %d x %s\n", command.ID, command.Quantity, command.SKU)
		return nil
	}, replies).WithMiddleware(
		// The demo callback returns no sensitive application errors. Applications
		// must supply a handler that redacts their own causes and selected fields.
		otelbroker.TraceProcessing("nats", traceOptions...),
		broker.LogProcessing(slog.Default(), broker.ProcessingLogConfig{}),
		broker.RecoverPanics(), broker.SkipOwnMessages("order-worker"),
	)
	coordinator := broker.NewCoordinator()
	factory := func() (broker.Consumer, error) { return natsjs.NewConsumer(nc, config) }
	factory = broker.WithConsumerRecovery(factory, broker.RecoveryConfig{
		Source: stream + "/" + consumerName, MaxRestarts: 2, Backoff: 100 * time.Millisecond,
		Retryable: retryStartupTimeout, Logger: slog.Default(),
	})
	if addErr := coordinator.Add("create-order", factory, handler); addErr != nil {
		return addErr
	}
	pub, err := natsjs.NewPublisher(nc, natsjs.PublisherConfig{Subject: subject})
	if err != nil {
		return err
	}
	commandPublisher := broker.WrapPublisher(pub,
		otelbroker.TracePublishing("nats", subject, traceOptions...),
		broker.StampInstanceID("command-client"), broker.ValidateIDHeader("Command-Id"))
	publish := broker.NewTypedPublisher(broker.EncoderFunc(json.Marshal), commandPublisher,
		func(_ context.Context, dto orders.CreateOrderDTO) (broker.MessageMetadata, error) {
			id := dto.OrderID + ".create"
			return broker.MessageMetadata{ID: id, Headers: []broker.Header{{Name: "Command-Id", Value: []byte(id)}}}, nil
		})
	if publishErr := publish(ctx, orders.CreateOrderDTO{OrderID: "order-1", ProductCode: "book-1", Count: 2}); publishErr != nil {
		return publishErr
	}
	err = coordinator.Run(ctx)
	if onlyCancellation(err) {
		return nil
	}
	return err
}

func newReceiptPublisher(nc *nats.Conn, subject string, traceOptions []otelbroker.Option) (broker.Publisher, error) {
	pub, err := natsjs.NewPublisher(nc, natsjs.PublisherConfig{Subject: subject})
	if err != nil {
		return nil, err
	}
	return broker.WrapPublisher(pub,
		otelbroker.TracePublishing("nats", subject, traceOptions...),
		broker.StampInstanceID("order-worker"), func(next broker.Publisher) broker.Publisher {
			return broker.PublisherFunc(func(ctx context.Context, message broker.Message) error {
				if publishErr := next.Publish(ctx, message); publishErr != nil {
					return publishErr
				}
				fmt.Println("reply confirmed")
				return nil
			})
		}), nil
}

// A canceled owner context is expected here. Independent processing, transport
// or cleanup failures in an errors.Join tree still make the example fail.
func onlyCancellation(err error) bool {
	if err == nil || err == context.Canceled {
		return true
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		for _, cause := range joined.Unwrap() {
			if !onlyCancellation(cause) {
				return false
			}
		}
		return true
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		cause := wrapped.Unwrap()
		return cause != nil && onlyCancellation(cause)
	}
	return false
}

func finishTelemetry(provider *sdktrace.TracerProvider) error {
	const operationTimeout = 2 * time.Second
	flushContext, cancelFlush := context.WithTimeout(context.Background(), operationTimeout)
	flushErr := provider.ForceFlush(flushContext)
	cancelFlush()
	if flushErr != nil {
		flushErr = fmt.Errorf("flush telemetry: %w", flushErr)
	}
	shutdownContext, cancelShutdown := context.WithTimeout(context.Background(), operationTimeout)
	shutdownErr := provider.Shutdown(shutdownContext)
	cancelShutdown()
	if shutdownErr != nil {
		shutdownErr = fmt.Errorf("shutdown telemetry: %w", shutdownErr)
	}
	return errors.Join(flushErr, shutdownErr)
}
