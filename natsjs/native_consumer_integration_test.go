package natsjs_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/velmie/broker"
	"github.com/velmie/broker/natsjs/v3"
)

func TestPushConsumerProcessesTypedDelivery(t *testing.T) {
	for _, ephemeral := range []bool{false, true} {
		name := "durable"
		if ephemeral {
			name = "ephemeral"
		}
		t.Run(name, func(t *testing.T) {
			nc, js, config := provision(t, time.Second)
			config = provisionPush(t, js, config, "", ephemeral, time.Second)
			ctx, stop := context.WithTimeout(context.Background(), testTimeout)
			defer stop()
			config.Observer.Record = func(_ context.Context, event natsjs.Observation) {
				if event.Operation == natsjs.OperationAck && event.Outcome == natsjs.OutcomeConfirmed {
					stop()
				}
			}
			calls := 0
			handler := broker.NewTypedHandler(broker.DecodeJSON[struct{ Quantity int }],
				func(_ context.Context, order struct{ Quantity int }) error {
					calls++
					if order.Quantity != 2 {
						t.Errorf("quantity=%d", order.Quantity)
					}
					return nil
				}).WithMiddleware(broker.SkipOwnMessages("other-instance"))
			if _, err := js.Publish(config.Subject, []byte(`{"Quantity":2}`)); err != nil {
				t.Fatal(err)
			}
			consumer, err := natsjs.NewPushConsumer(nc, config)
			if err != nil {
				t.Fatal(err)
			}
			if err = consumer.Run(ctx, handler); !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if calls != 1 {
				t.Fatalf("calls=%d", calls)
			}
			assertPushProgress(t, js, config, ephemeral, 1)
			if err = nc.Flush(); err != nil {
				t.Fatalf("borrowed connection: %v", err)
			}
		})
	}
}

func TestPushConsumerQueueSharesProgressAndSurvivesPeerStop(t *testing.T) {
	nc, js, config := provision(t, time.Second)
	config = provisionPush(t, js, config, "workers", false, time.Second)
	ready := make(chan struct{}, 2)
	acked := make(chan struct{}, 3)
	delivered := make(chan string, 3)
	config.Observer.Record = func(_ context.Context, event natsjs.Observation) {
		if event.Operation == natsjs.OperationStartup && event.Outcome == natsjs.OutcomeSucceeded {
			ready <- struct{}{}
		}
		if event.Operation == natsjs.OperationAck && event.Outcome == natsjs.OutcomeConfirmed {
			acked <- struct{}{}
		}
	}
	handler := broker.NewHandler(func(_ context.Context, delivery broker.Delivery) (broker.Disposition, error) {
		if delivery.Source() != config.Stream+"/"+config.Consumer {
			t.Error("source identity changed")
		}
		delivered <- string(delivery.Message().Body)
		return broker.Handled{}, nil
	})
	firstCtx, stopFirst := context.WithCancel(context.Background())
	defer stopFirst()
	secondCtx, stopSecond := context.WithCancel(context.Background())
	defer stopSecond()
	first := runPush(t, nc, config, firstCtx, handler)
	second := runPush(t, nc, config, secondCtx, handler)
	receive(t, ready)
	receive(t, ready)
	for _, body := range []string{"first", "second"} {
		if _, err := js.Publish(config.Subject, []byte(body)); err != nil {
			t.Fatal(err)
		}
	}
	receive(t, acked)
	receive(t, acked)
	seen := map[string]bool{receive(t, delivered): true}
	seen[receive(t, delivered)] = true
	if !seen["first"] || !seen["second"] {
		t.Fatalf("queue deliveries: %v", seen)
	}
	stopFirst()
	if err := receive(t, first); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := js.Publish(config.Subject, []byte("after-stop")); err != nil {
		t.Fatal(err)
	}
	receive(t, acked)
	if got := receive(t, delivered); got != "after-stop" {
		t.Fatalf("remaining consumer: %q", got)
	}
	stopSecond()
	if err := receive(t, second); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	assertPushProgress(t, js, config, false, 3)
	info, err := js.ConsumerInfo(config.Stream, config.Consumer)
	if err != nil || info.Config.DeliverGroup != "workers" {
		t.Fatalf("queue ownership: %+v, %v", info, err)
	}
	if err = nc.Flush(); err != nil {
		t.Fatal(err)
	}
}

func TestPushConsumerGracefulStopJoinsCallbackAndRenewal(t *testing.T) {
	nc, js, config := provision(t, time.Second)
	config = provisionPush(t, js, config, "", true, 150*time.Millisecond)
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	entered := make(chan context.Context, 1)
	release := make(chan struct{})
	renewed := make(chan struct{}, 16)
	config.Observer.Record = func(_ context.Context, event natsjs.Observation) {
		if event.Operation == natsjs.OperationRenew && event.Outcome == natsjs.OutcomeConfirmed {
			renewed <- struct{}{}
		}
	}
	handler := broker.NewTypedHandler(broker.DecodeJSON[struct{}], func(work context.Context, _ struct{}) error {
		entered <- work
		<-release
		return nil
	}, broker.WithKeepAlive(25*time.Millisecond))
	result := runPush(t, nc, config, ctx, handler)
	if _, err := js.Publish(config.Subject, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	work := receive(t, entered)
	stop()
	for range 8 {
		receive(t, renewed)
	}
	if work.Err() != nil {
		t.Fatalf("graceful callback canceled: %v", work.Err())
	}
	select {
	case err := <-result:
		t.Fatalf("Run returned before callback joined: %v", err)
	default:
	}
	close(release)
	if err := receive(t, result); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	assertPushProgress(t, js, config, true, 1)
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
}

func TestPushConsumerFailureLeavesDeliveryUnconfirmed(t *testing.T) {
	nc, js, config := provision(t, time.Second)
	config = provisionPush(t, js, config, "", false, time.Second)
	cause := errors.New("business effect failed")
	var terminal int
	config.Observer.Record = func(_ context.Context, event natsjs.Observation) {
		if isTerminal(event.Operation) && event.Outcome == natsjs.OutcomeStarted {
			terminal++
		}
	}
	handler := broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
		return broker.Handled{}, cause
	})
	ctx, stop := context.WithTimeout(context.Background(), testTimeout)
	defer stop()
	result := runPush(t, nc, config, ctx, handler)
	if _, err := js.Publish(config.Subject, []byte("event")); err != nil {
		t.Fatal(err)
	}
	if err := receive(t, result); !errors.Is(err, cause) {
		t.Fatalf("processing cause lost: %v", err)
	}
	if terminal != 0 {
		t.Fatalf("terminal operations=%d", terminal)
	}
	assertPending(t, js, config, 1)
}

func TestPushConsumerForcedStopJoinsCallbackWithoutAck(t *testing.T) {
	nc, js, config := provision(t, time.Second)
	config = provisionPush(t, js, config, "", false, time.Second)
	config.Shutdown.Mode = broker.ShutdownCancel
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	handler := broker.NewHandler(func(work context.Context, _ broker.Delivery) (broker.Disposition, error) {
		close(entered)
		<-work.Done()
		close(canceled)
		<-release
		return broker.Handled{}, nil
	})
	result := runPush(t, nc, config, ctx, handler)
	if _, err := js.Publish(config.Subject, []byte("event")); err != nil {
		t.Fatal(err)
	}
	receive(t, entered)
	stop()
	receive(t, canceled)
	select {
	case err := <-result:
		t.Fatalf("callback not joined: %v", err)
	default:
	}
	close(release)
	if err := receive(t, result); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	assertPending(t, js, config, 1)
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
}

func TestPushConsumerRejectsPullProfileBeforeCallback(t *testing.T) {
	nc, _, config := provision(t, time.Second)
	consumer, err := natsjs.NewPushConsumer(nc, config)
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithTimeout(context.Background(), testTimeout)
	defer stop()
	err = consumer.Run(ctx, broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
		t.Error("push runtime accepted pull consumer")
		return broker.Handled{}, nil
	}))
	var field *broker.FieldError
	if !errors.As(err, &field) || field.Field != "Consumer" {
		t.Fatalf("profile rejection: %v", err)
	}
}

func TestPushConsumerRejectsExistingNonQueueBinding(t *testing.T) {
	nc, js, config := provision(t, time.Second)
	config = provisionPush(t, js, config, "", false, time.Second)
	owned, err := js.SubscribeSync(config.Subject, nats.Bind(config.Stream, config.Consumer))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := owned.Unsubscribe(); closeErr != nil {
			t.Error(closeErr)
		}
	})
	if err = nc.Flush(); err != nil {
		t.Fatal(err)
	}
	consumer, err := natsjs.NewPushConsumer(nc, config)
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer stop()
	err = consumer.Run(ctx, broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
		t.Error("second non-queue binding reached callback")
		return broker.Handled{}, nil
	}))
	var field *broker.FieldError
	if !errors.As(err, &field) || field.Field != "Consumer.PushBound" || !owned.IsValid() {
		t.Fatalf("existing binding was not preserved: %v valid=%v", err, owned.IsValid())
	}
}

//nolint:gocritic // The consumer owns its configuration copy.
func runPush(t *testing.T, nc *nats.Conn, config natsjs.ConsumerConfig, ctx context.Context, handler broker.Handler) <-chan error {
	t.Helper()
	consumer, err := natsjs.NewPushConsumer(nc, config)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- consumer.Run(ctx, handler) }()
	return result
}

//nolint:gocritic // The test creates an independent binding configuration.
func provisionPush(t *testing.T, js nats.JetStreamContext, config natsjs.ConsumerConfig,
	group string, ephemeral bool, ackWait time.Duration,
) natsjs.ConsumerConfig {
	t.Helper()
	name := "push"
	config.Consumer = name
	remote := &nats.ConsumerConfig{
		Name: name, FilterSubject: config.Subject, AckPolicy: nats.AckExplicitPolicy, AckWait: ackWait,
		DeliverSubject: nats.NewInbox(), DeliverGroup: group, InactiveThreshold: time.Minute,
	}
	if !ephemeral {
		remote.Durable = name
	}
	if _, err := js.AddConsumer(config.Stream, remote); err != nil {
		t.Fatal(err)
	}
	return config
}

//nolint:gocritic // Inspect the same immutable binding used by Run.
func assertPushProgress(t *testing.T, js nats.JetStreamContext, config natsjs.ConsumerConfig, ephemeral bool, sequence uint64) {
	t.Helper()
	info, err := js.ConsumerInfo(config.Stream, config.Consumer)
	if err != nil {
		t.Fatal(err)
	}
	if (info.Config.Durable == "") != ephemeral || info.Name != config.Consumer || info.Config.DeliverSubject == "" ||
		info.Config.AckPolicy != nats.AckExplicitPolicy || info.AckFloor.Stream != sequence || info.NumAckPending != 0 {
		t.Fatalf("push identity/progress: %+v", info)
	}
}
