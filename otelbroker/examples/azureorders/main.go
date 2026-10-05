// Command azureorders traces one typed order using existing Service Bus entities.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/velmie/broker"
	adapter "github.com/velmie/broker/azuresb"
	"github.com/velmie/broker/otelbroker"
)

const (
	argumentsField        = "arguments"
	routeField            = "queue/topic/subscription"
	receiveModeFlag       = "receive-mode"
	idFlag                = "id"
	sourceName            = "orders"
	connectionEnvironment = "AZURE_SERVICEBUS_CONNECTION_STRING"
	peekLockMode          = "peek-lock"
	receiveDeleteMode     = "receive-and-delete"
	operationBudget       = 10 * time.Second
	receiveBudget         = 2 * time.Second
	closeBudget           = 5 * time.Second
	routeBudget           = 45 * time.Second
	telemetryBudget       = 2 * time.Second
	recoveryBackoff       = 10 * time.Millisecond
	exampleAmount         = 42
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{ReplaceAttr: redactAttribute}))
	settings, err := parseOptions(os.Args[1:])
	if err == nil {
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		exporter, exportErr := stdouttrace.New()
		err = exportErr
		if err == nil {
			provider := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exporter))
			err = run(ctx, os.Getenv(connectionEnvironment), &settings, provider, logger)
		}
		stop()
	}
	if err != nil {
		logger.Error("Azure orders failed", slog.Any("error", err))
		os.Exit(1)
	}
}

type options struct{ queue, topic, subscription, receiveMode, id string }
type order struct {
	Amount int `json:"amount"`
}

func parseOptions(args []string) (options, error) {
	var settings options
	flags := flag.NewFlagSet("azureorders", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&settings.queue, "queue", "", "existing queue")
	flags.StringVar(&settings.topic, "topic", "", "existing topic")
	flags.StringVar(&settings.subscription, "subscription", "", "existing subscription")
	flags.StringVar(&settings.receiveMode, receiveModeFlag, peekLockMode, "peek-lock or receive-and-delete")
	flags.StringVar(&settings.id, idFlag, "order-example-001", "stable logical ID")
	if err := flags.Parse(args); err != nil {
		return settings, &adapter.FieldError{Field: argumentsField, Cause: err}
	}
	if flags.NArg() != 0 {
		return settings, &adapter.FieldError{Field: argumentsField, Cause: errors.New("unexpected positional arguments")}
	}
	validationErr := settings.validate()
	return settings, validationErr
}
func (o *options) validate() error {
	queue, topic, subscription := strings.TrimSpace(o.queue) != "", strings.TrimSpace(o.topic) != "", strings.TrimSpace(o.subscription) != ""
	validRoute := queue && !topic && !subscription || !queue && topic && subscription
	if !validRoute {
		return &adapter.FieldError{Field: routeField, Cause: errors.New("choose queue or topic with subscription")}
	}
	if o.receiveMode != peekLockMode && o.receiveMode != receiveDeleteMode {
		return &adapter.FieldError{Field: receiveModeFlag, Cause: errors.New("unsupported receive mode")}
	}
	if strings.TrimSpace(o.id) == "" {
		return &adapter.FieldError{Field: idFlag, Cause: errors.New("logical ID required")}
	}
	return nil
}
func (o *options) mode() azservicebus.ReceiveMode {
	if o.receiveMode == receiveDeleteMode {
		return azservicebus.ReceiveModeReceiveAndDelete
	}
	return azservicebus.ReceiveModePeekLock
}

func run(ctx context.Context, connection string, settings *options, provider *sdktrace.TracerProvider, logger *slog.Logger) (result error) {
	defer func() { result = errors.Join(result, finishTelemetry(provider)) }()
	if err := settings.validate(); err != nil {
		return err
	}
	if strings.TrimSpace(connection) == "" {
		return &adapter.FieldError{Field: connectionEnvironment, Cause: errors.New("connection string required")}
	}
	ctx, cancel := context.WithTimeout(ctx, routeBudget)
	defer cancel()
	client, err := azservicebus.NewClientFromConnectionString(connection, &azservicebus.ClientOptions{
		ApplicationID: "broker-traced-orders", RetryOptions: azservicebus.RetryOptions{MaxRetries: -1}})
	if err != nil {
		return operationError("create_client", err)
	}
	defer func() { result = errors.Join(result, closeNative("close_client", client.Close)) }()
	entity := settings.queue
	if entity == "" {
		entity = settings.topic
	}
	sender, err := client.NewSender(entity, nil)
	if err != nil {
		return operationError("create_sender", err)
	}
	defer func() { result = errors.Join(result, closeNative("close_sender", sender.Close)) }()
	tracer := provider.Tracer("broker.azureorders.example")
	if publishErr := publishOrder(ctx, sender, settings.id, tracer); publishErr != nil {
		return publishErr
	}
	logger.InfoContext(ctx, "Azure publication accepted", "source", sourceName)
	factory := func() (adapter.Receiver, error) { return newReceiver(client, settings) }
	return consume(ctx, factory, settings, logger, tracer, func(context.Context, broker.Delivery, order) error { return nil })
}

func newReceiver(client *azservicebus.Client, settings *options) (*azservicebus.Receiver, error) {
	nativeOptions := &azservicebus.ReceiverOptions{ReceiveMode: settings.mode()}
	if settings.queue != "" {
		return client.NewReceiverForQueue(settings.queue, nativeOptions)
	}
	return client.NewReceiverForSubscription(settings.topic, settings.subscription, nativeOptions)
}

func publishOrder(ctx context.Context, sender *azservicebus.Sender, id string, tracer trace.Tracer) error {
	pub, err := adapter.NewPublisher(sender, adapter.PublisherConfig{Source: sourceName, OperationTimeout: operationBudget,
		Observer: &adapter.Observer{Record: func(ctx context.Context, event adapter.Observation) { recordNative(ctx, tracer, &event) }}})
	if err != nil {
		return err
	}
	wrapped := broker.WrapPublisher(pub,
		otelbroker.TracePublishing("servicebus", sourceName, otelbroker.WithTracer(tracer),
			otelbroker.WithPropagator(propagation.TraceContext{})),
		broker.StampInstanceID("command-client"), broker.ValidateIDHeader("Command-Id"))
	publish := broker.NewTypedPublisher[order](broker.EncoderFunc(json.Marshal), wrapped,
		func(context.Context, order) (broker.MessageMetadata, error) {
			return broker.MessageMetadata{ID: id, Headers: []broker.Header{{Name: "Command-Id", Value: []byte(id)}}}, nil
		})
	return publish(ctx, order{Amount: exampleAmount})
}
func operationError(operation string, cause error) error {
	if cause == nil {
		return nil
	}
	return &adapter.OperationError{Source: sourceName, Operation: operation, Cause: cause}
}
func closeNative(operation string, closeResource func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), closeBudget)
	defer cancel()
	return operationError(operation, closeResource(ctx))
}
func finishTelemetry(provider *sdktrace.TracerProvider) error {
	ctx, cancel := context.WithTimeout(context.Background(), telemetryBudget)
	flushErr := provider.ForceFlush(ctx)
	cancel()
	shutdown, cancelShutdown := context.WithTimeout(context.Background(), telemetryBudget)
	shutdownErr := provider.Shutdown(shutdown)
	cancelShutdown()
	return errors.Join(operationError("flush", flushErr), operationError("shutdown", shutdownErr))
}
