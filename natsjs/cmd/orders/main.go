// Command orders creates a temporary JetStream source, publishes one command,
// processes it through Broker, publishes a receipt before ACK, and deletes the
// source after all work joins.
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

	"github.com/velmie/broker"
	"github.com/velmie/broker/natsjs/v3"
	"github.com/velmie/broker/natsjs/v3/examples/orders"
)

const consumerName = "worker"

func main() {
	url := flag.String("url", nats.DefaultURL, "NATS server with JetStream enabled")
	domain := flag.String("domain", "", "JetStream domain (mutually exclusive with -api-prefix)")
	prefix := flag.String("api-prefix", "", "JetStream API prefix, with optional trailing dot")
	flag.Parse()
	if err := run(*url, *domain, *prefix); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(url, domain, apiPrefix string) (result error) {
	ctx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	ctx, stop := context.WithCancel(ctx)
	defer stop()
	nc, err := nats.Connect(url, nats.FlusherTimeout(time.Second))
	if err != nil {
		return err
	}
	defer nc.Close() // The connection owner closes it after coordinator.Run joins.
	stream := "BROKER_EXAMPLE_" + nuid.Next()
	subject := stream + ".orders"
	replySubject := stream + ".created"
	config := natsjs.ConsumerConfig{
		Stream: stream, Consumer: consumerName, Subject: subject, Domain: domain, APIPrefix: apiPrefix,
		Shutdown: broker.ShutdownPolicy{GracePeriod: time.Second},
	}
	// Validate the same immutable selection before creating remote topology.
	if _, validationErr := natsjs.NewConsumer(nc, config); validationErr != nil {
		return validationErr
	}
	var options []nats.JSOpt
	if domain != "" {
		options = append(options, nats.Domain(domain))
	}
	if apiPrefix != "" {
		options = append(options, nats.APIPrefix(apiPrefix))
	}
	js, err := nc.JetStream(options...)
	if err != nil {
		return err
	}
	// Provisioning is explicit application wiring, separate from Consumer.Run.
	if _, err = js.AddStream(&nats.StreamConfig{Name: stream, Subjects: []string{subject, replySubject},
		Storage: nats.MemoryStorage}); err != nil {
		return &natsjs.OperationError{Source: stream, Operation: "create_stream", Cause: err}
	}
	defer func() {
		if cleanupErr := js.DeleteStream(stream); cleanupErr != nil {
			result = errors.Join(result, &natsjs.OperationError{Source: stream, Operation: "delete_stream", Cause: cleanupErr})
		}
	}()
	remote := &nats.ConsumerConfig{Durable: consumerName, FilterSubject: subject, AckPolicy: nats.AckExplicitPolicy}
	if _, err = js.AddConsumer(stream, remote); err != nil {
		return &natsjs.OperationError{Source: stream + "/" + consumerName, Operation: "create_consumer", Cause: err}
	}
	config.Observer.Record = func(_ context.Context, event natsjs.Observation) {
		if event.Operation == natsjs.OperationAck && event.Outcome == natsjs.OutcomeConfirmed {
			fmt.Println("ack confirmed")
			stop()
		}
	}
	replies, err := newReceiptPublisher(nc, replySubject)
	if err != nil {
		return err
	}
	handler := orders.NewReplyHandler(func(_ context.Context, command orders.CreateOrderCommand) error {
		fmt.Printf("created order %s: %d x %s\n", command.ID, command.Quantity, command.SKU)
		return nil
	}, replies).WithMiddleware(
		// The demo callback returns no sensitive application errors. Applications
		// must supply a handler that redacts their own causes and selected fields.
		broker.LogProcessing(slog.Default(), broker.ProcessingLogConfig{
			Attributes: func(_ context.Context, delivery broker.Delivery, _ broker.Disposition, err error) []slog.Attr {
				if err == nil {
					return nil
				}
				return []slog.Attr{slog.Int("payload_bytes", len(delivery.Message().Body))}
			},
		}),
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
	commandPublisher := broker.WrapPublisher(pub, broker.StampInstanceID("command-client"), broker.ValidateIDHeader("Command-Id"))
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

func newReceiptPublisher(nc *nats.Conn, subject string) (broker.Publisher, error) {
	pub, err := natsjs.NewPublisher(nc, natsjs.PublisherConfig{Subject: subject})
	if err != nil {
		return nil, err
	}
	return broker.WrapPublisher(pub, broker.StampInstanceID("order-worker"), func(next broker.Publisher) broker.Publisher {
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
