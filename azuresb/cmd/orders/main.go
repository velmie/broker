// Command orders publishes and processes one typed order on an existing Azure
// Service Bus queue or topic subscription. The caller provisions the entities.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"log/slog"
	"math"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"

	"github.com/velmie/broker"
	adapter "github.com/velmie/broker/azuresb"
)

const (
	deliveryCountField          = "DeliveryCount"
	connectionStringEnvironment = "AZURE_SERVICEBUS_CONNECTION_STRING"
	argumentsField              = "arguments"
	routeField                  = "queue/topic/subscription"
	receiveModeFlag             = "receive-mode"
	idFlag                      = "id"
	workDurationFlag            = "work-duration"
	renewEveryFlag              = "renew-every"
	orderField                  = "order"
	sourceName                  = "orders"
	defaultOrderID              = "order-example-001"
	peekLockMode                = "peek-lock"
	receiveAndDeleteMode        = "receive-and-delete"
	exampleAmount               = 42
	overallBudget               = 45 * time.Second
	operationBudget             = 10 * time.Second
	receiveBudget               = 2 * time.Second
	closeBudget                 = 5 * time.Second
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{ReplaceAttr: redactAttribute}))
	settings, err := parseOptions(os.Args[1:])
	if err == nil {
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		err = run(ctx, os.Getenv(connectionStringEnvironment), &settings, logger)
		stop()
	}
	if err != nil {
		logger.Error("Azure orders example failed", slog.Any("error", err))
		os.Exit(1)
	}
}

type options struct {
	queue, topic, subscription, receiveMode, id string
	workDuration, renewEvery                    time.Duration
}
type order struct {
	ID     string `json:"id"`
	Amount int    `json:"amount"`
}

func parseOptions(args []string) (options, error) {
	var settings options
	flags := flag.NewFlagSet("orders", flag.ContinueOnError)
	// Flag parsing errors may contain argument values. Keep their original causes
	// for the configured error redactor instead of printing them directly.
	flags.SetOutput(io.Discard)
	flags.StringVar(&settings.queue, "queue", "", "existing queue name")
	flags.StringVar(&settings.topic, "topic", "", "existing topic name")
	flags.StringVar(&settings.subscription, "subscription", "", "existing subscription name")
	flags.StringVar(&settings.receiveMode, receiveModeFlag, peekLockMode, "peek-lock or receive-and-delete")
	flags.StringVar(&settings.id, idFlag, defaultOrderID, "stable logical order ID")
	var durationError error
	durationFlag := func(name, description string, target *time.Duration) {
		flags.Func(name, description, func(value string) error {
			duration, err := time.ParseDuration(value)
			if err != nil {
				durationError = &adapter.FieldError{Field: name, Cause: err}
				return durationError
			}
			*target = duration
			return nil
		})
	}
	durationFlag(workDurationFlag, "duration of simulated business work (default 0s)", &settings.workDuration)
	durationFlag(renewEveryFlag, "PeekLock renewal cadence; zero disables renewal (default 0s)", &settings.renewEvery)
	if err := flags.Parse(args); err != nil {
		if durationError != nil {
			return settings, durationError
		}
		return settings, &adapter.FieldError{Field: argumentsField, Cause: err}
	}
	if flags.NArg() != 0 {
		return settings, &adapter.FieldError{Field: argumentsField, Cause: errors.New("unexpected positional arguments")}
	}
	validationErr := settings.validate()
	return settings, validationErr
}

func (o *options) validate() error {
	queue := strings.TrimSpace(o.queue) != ""
	topic := strings.TrimSpace(o.topic) != ""
	subscription := strings.TrimSpace(o.subscription) != ""
	validRoute := queue && !topic && !subscription || !queue && topic && subscription
	if !validRoute {
		return &adapter.FieldError{Field: routeField, Cause: errors.New("choose queue or topic with subscription")}
	}
	if o.receiveMode != peekLockMode && o.receiveMode != receiveAndDeleteMode {
		return &adapter.FieldError{Field: receiveModeFlag, Cause: errors.New("unsupported receive mode")}
	}
	if strings.TrimSpace(o.id) == "" {
		return &adapter.FieldError{Field: idFlag, Cause: errors.New("logical ID is required")}
	}
	if o.workDuration < 0 || o.workDuration > time.Duration(math.MaxInt64)-overallBudget {
		return &adapter.FieldError{Field: workDurationFlag,
			Cause: errors.New("work duration must be nonnegative and leave room for the overall budget")}
	}
	if o.renewEvery < 0 || o.renewEvery > 0 && o.receiveMode == receiveAndDeleteMode {
		return &adapter.FieldError{Field: renewEveryFlag, Cause: errors.New("renewal cadence must be nonnegative and requires PeekLock")}
	}
	return nil
}

func run(ctx context.Context, connectionString string, settings *options, logger *slog.Logger) (result error) {
	if err := settings.validate(); err != nil {
		return err
	}
	if strings.TrimSpace(connectionString) == "" {
		return &adapter.FieldError{Field: connectionStringEnvironment, Cause: errors.New("connection string is required")}
	}
	work, cancel := context.WithTimeout(ctx, overallBudget+settings.workDuration)
	defer cancel()
	// The command owns SDK retry policy and all native objects. Receiver ownership
	// transfers to each Run. Sender and client close after that Run has joined.
	client, err := azservicebus.NewClientFromConnectionString(connectionString, &azservicebus.ClientOptions{
		ApplicationID: "broker-orders", RetryOptions: azservicebus.RetryOptions{MaxRetries: -1},
	})
	if err != nil {
		return commandError("create_client", err)
	}
	defer func() { result = errors.Join(result, closeNative("close_client", client.Close)) }()
	entity := settings.queue
	if entity == "" {
		entity = settings.topic
	}
	sender, err := client.NewSender(entity, nil)
	if err != nil {
		return commandError("create_sender", err)
	}
	defer func() { result = errors.Join(result, closeNative("close_sender", sender.Close)) }()
	publisher, err := adapter.NewPublisher(sender, adapter.PublisherConfig{Source: sourceName, OperationTimeout: operationBudget})
	if err != nil {
		return err
	}
	publish := broker.NewTypedPublisher[order](broker.EncoderFunc(json.Marshal), publisher,
		func(_ context.Context, value order) (broker.MessageMetadata, error) {
			return broker.MessageMetadata{ID: value.ID, Headers: []broker.Header{{Name: "Correlation-Id", Value: []byte("order-example")}}}, nil
		})
	if publishErr := publish(work, order{ID: settings.id, Amount: exampleAmount}); publishErr != nil {
		return publishErr
	}
	logger.Info("Azure publication accepted", "source", sourceName)
	return consume(work, client, settings, logger)
}

func consume(ctx context.Context, client *azservicebus.Client, settings *options, logger *slog.Logger) error {
	mode := azservicebus.ReceiveModePeekLock
	if settings.receiveMode == receiveAndDeleteMode {
		mode = azservicebus.ReceiveModeReceiveAndDelete
	}
	receiverOptions := &azservicebus.ReceiverOptions{ReceiveMode: mode}
	factory := func() (adapter.Receiver, error) {
		if settings.queue != "" {
			return client.NewReceiverForQueue(settings.queue, receiverOptions)
		}
		return client.NewReceiverForSubscription(settings.topic, settings.subscription, receiverOptions)
	}
	runCtx, stopRun := context.WithCancel(ctx)
	defer stopRun()
	achieved := false
	observer := &adapter.Observer{Record: func(_ context.Context, event adapter.Observation) {
		if mode == azservicebus.ReceiveModeReceiveAndDelete && event.Operation == adapter.OperationProcess &&
			event.Outcome == adapter.OutcomeSucceeded {
			achieved = true
			logger.Info("ReceiveAndDelete processing finished; receipt preceded callback and has no acknowledgment guarantee", "source", sourceName)
			stopRun()
		}
		if event.Outcome != adapter.OutcomeAccepted {
			return
		}
		switch event.Operation {
		case adapter.OperationRenew:
			logger.Info("Azure lock renewal accepted", "source", sourceName)
		case adapter.OperationAbandon:
			logger.Info("Azure abandon accepted", "source", sourceName, slog.Any("cause", event.Cause))
		case adapter.OperationComplete:
			achieved = true
			logger.Info("Azure completion accepted", "source", sourceName)
			stopRun()
		}
	}}
	consumer, err := adapter.NewConsumer(factory, adapter.ConsumerConfig{Source: sourceName, ReceiveMode: mode,
		ReceiveTimeout: receiveBudget, OperationTimeout: operationBudget, CloseTimeout: closeBudget,
		Shutdown: broker.ShutdownPolicy{Mode: broker.ShutdownGraceful, GracePeriod: operationBudget}, Observer: observer})
	if err != nil {
		return err
	}
	err = consumer.Run(runCtx, orderHandler(settings, logger))
	if achieved && onlyCancellation(err) {
		return nil
	}
	if err == nil {
		return commandError("process", errors.New("order outcome was not observed"))
	}
	return err
}

func orderHandler(settings *options, logger *slog.Logger) broker.Handler {
	var requirements []broker.Requirement
	if settings.receiveMode == peekLockMode {
		requirements = []broker.Requirement{broker.AttemptsRequirement{}, broker.RedeliveryRequirement{}}
	}
	if settings.renewEvery > 0 {
		requirements = append(requirements, broker.KeepAliveRequirement{Interval: settings.renewEvery})
	}
	first := true
	return broker.NewTypedDeliveryHandler(broker.DecodeJSON[order],
		func(ctx context.Context, delivery broker.Delivery, value order) (broker.Disposition, error) {
			if value.ID != settings.id || value.Amount != exampleAmount {
				return nil, &adapter.FieldError{Field: orderField, Cause: errors.New("unexpected order")}
			}
			if settings.receiveMode == peekLockMode {
				attempt := delivery.(broker.AttemptReader).Attempt()
				if !attempt.Known {
					return nil, &adapter.FieldError{Field: deliveryCountField, Cause: errors.New("native delivery attempt is required")}
				}
				if first {
					first = false
					return broker.RetryAfter{Cause: errors.New("demonstration retry before business work")}, nil
				}
			}
			if settings.workDuration > 0 {
				timer := time.NewTimer(settings.workDuration)
				defer timer.Stop()
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-timer.C:
				}
				if err := ctx.Err(); err != nil {
					return nil, err
				}
			}
			logger.Info("Order effect completed", "source", sourceName, "amount", exampleAmount)
			return broker.Handled{}, nil
		}, broker.WithRequirements(requirements...)).WithMiddleware(broker.RecoverPanics())
}

func closeNative(operation string, closeResource func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), closeBudget)
	defer cancel()
	closeErr := closeResource(ctx)
	return commandError(operation, errors.Join(closeErr, ctx.Err()))
}
func commandError(operation string, cause error) error {
	if cause == nil {
		return nil
	}
	return &adapter.OperationError{Source: sourceName, Operation: operation, Cause: cause}
}

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
