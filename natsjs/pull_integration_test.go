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

func TestPullConsumerEphemeralFanoutAndIndependentStop(t *testing.T) {
	nc, js, config := provision(t, time.Second)
	first := namedEphemeral(t, js, config, "replica-one", time.Second)
	second := namedEphemeral(t, js, config, "replica-two", time.Second)
	firstCtx, stopFirst := context.WithCancel(context.Background())
	defer stopFirst()
	secondCtx, stopSecond := context.WithCancel(context.Background())
	defer stopSecond()
	firstAcks, secondAcks := make(chan struct{}, 4), make(chan struct{}, 4)
	firstCalls, secondCalls := make(chan string, 4), make(chan string, 4)
	first.Observer.Record = confirmedPullAcks(firstAcks)
	second.Observer.Record = confirmedPullAcks(secondAcks)
	handler := func(instance string, calls chan<- string) broker.Handler {
		return broker.NewHandler(func(_ context.Context, delivery broker.Delivery) (broker.Disposition, error) {
			calls <- delivery.Message().ID
			return broker.Handled{}, nil
		}).WithMiddleware(broker.SkipOwnMessages(instance))
	}
	firstResult := runPull(t, nc, first, firstCtx, handler("replica-one", firstCalls))
	secondResult := runPull(t, nc, second, secondCtx, handler("replica-two", secondCalls))
	pub, err := natsjs.NewPublisher(nc, natsjs.PublisherConfig{Subject: config.Subject})
	if err != nil {
		t.Fatal(err)
	}
	publisher := broker.WrapPublisher(pub, broker.StampInstanceID("replica-one"))
	if err = publisher.Publish(context.Background(), broker.Message{ID: "first"}); err != nil {
		t.Fatal(err)
	}
	receive(t, firstAcks)
	receive(t, secondAcks)
	if got := receive(t, secondCalls); got != "first" {
		t.Fatalf("foreign event=%q", got)
	}
	select {
	case got := <-firstCalls:
		t.Fatalf("own event reached callback: %q", got)
	default:
	}
	stopFirst()
	if err = receive(t, firstResult); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	assertEphemeralProgress(t, js, first, 1)
	if err = publisher.Publish(context.Background(), broker.Message{ID: "after-stop"}); err != nil {
		t.Fatal(err)
	}
	receive(t, secondAcks)
	if got := receive(t, secondCalls); got != "after-stop" {
		t.Fatalf("remaining replica event=%q", got)
	}
	stopSecond()
	if err = receive(t, secondResult); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	assertEphemeralProgress(t, js, first, 1)
	assertEphemeralProgress(t, js, second, 2)
	if err = nc.Flush(); err != nil {
		t.Fatalf("borrowed connection: %v", err)
	}
}

func TestPullConsumerPreservesDurableBacklog(t *testing.T) {
	nc, js, config := provision(t, time.Second)
	for _, body := range []string{"first", "second"} {
		if _, err := js.Publish(config.Subject, []byte(body)); err != nil {
			t.Fatal(err)
		}
	}
	for index, want := range []string{"first", "second"} {
		ctx, stop := context.WithCancel(context.Background())
		config.Observer.Record = func(_ context.Context, event natsjs.Observation) {
			if event.Operation == natsjs.OperationAck && event.Outcome == natsjs.OutcomeConfirmed {
				stop()
			}
		}
		var got string
		handler := broker.NewHandler(func(_ context.Context, delivery broker.Delivery) (broker.Disposition, error) {
			got = string(delivery.Message().Body)
			if delivery.Source() != config.Stream+"/"+config.Consumer {
				t.Error("source identity changed")
			}
			return broker.Handled{}, nil
		})
		result := runPull(t, nc, config, ctx, handler)
		err := receive(t, result)
		stop()
		if !errors.Is(err, context.Canceled) || got != want {
			t.Fatalf("backlog body=%q err=%v", got, err)
		}
		info, err := js.ConsumerInfo(config.Stream, config.Consumer)
		if err != nil {
			t.Fatal(err)
		}
		if info.Config.Durable != config.Consumer || info.AckFloor.Stream != uint64(index+1) || info.NumAckPending != 0 {
			t.Fatalf("durable progress: %+v", info)
		}
	}
}

func TestConsumerStillRejectsNamedEphemeral(t *testing.T) {
	nc, js, config := provision(t, time.Second)
	config = namedEphemeral(t, js, config, "replica", time.Second)
	consumer, err := natsjs.NewConsumer(nc, config)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	err = consumer.Run(ctx, broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
		t.Error("durable constructor accepted ephemeral")
		return broker.Handled{}, nil
	}))
	var field *broker.FieldError
	if !errors.As(err, &field) || field.Field != "Consumer" || field.Reason != "incompatible_consumer" {
		t.Fatalf("durable compatibility rejection: %v", err)
	}
	assertEphemeralProgress(t, js, config, 0)
}

func TestPullConsumerEphemeralGracefulStopJoinsRenewalAndTerminal(t *testing.T) {
	nc, js, config := provision(t, time.Second)
	config = namedEphemeral(t, js, config, "replica", 150*time.Millisecond)
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	entered := make(chan context.Context, 1)
	renewed := make(chan struct{}, 16)
	terminal := make(chan struct{})
	releaseCallback, releaseTerminal := make(chan struct{}), make(chan struct{})
	config.Observer.Record = func(_ context.Context, event natsjs.Observation) {
		if event.Operation == natsjs.OperationRenew && event.Outcome == natsjs.OutcomeConfirmed {
			renewed <- struct{}{}
		}
		if event.Operation == natsjs.OperationAck && event.Outcome == natsjs.OutcomeConfirmed {
			close(terminal)
			<-releaseTerminal
		}
	}
	handler := broker.NewTypedHandler(broker.DecodeJSON[struct{}], func(work context.Context, _ struct{}) error {
		entered <- work
		<-releaseCallback
		return nil
	}, broker.WithKeepAlive(25*time.Millisecond))
	result := runPull(t, nc, config, ctx, handler)
	if _, err := js.Publish(config.Subject, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	work := receive(t, entered)
	stop()
	for range 8 {
		receive(t, renewed)
	}
	if work.Err() != nil {
		t.Fatalf("graceful processing canceled: %v", work.Err())
	}
	select {
	case err := <-result:
		t.Fatalf("Run did not join callback: %v", err)
	default:
	}
	close(releaseCallback)
	receive(t, terminal)
	select {
	case err := <-result:
		t.Fatalf("Run did not join terminal observer: %v", err)
	default:
	}
	close(releaseTerminal)
	if err := receive(t, result); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	assertEphemeralProgress(t, js, config, 1)
	if err := nc.Flush(); err != nil {
		t.Fatalf("borrowed connection: %v", err)
	}
}

//nolint:gocritic // Each test receives an independent binding configuration.
func namedEphemeral(t *testing.T, js nats.JetStreamContext, config natsjs.ConsumerConfig,
	name string, ackWait time.Duration,
) natsjs.ConsumerConfig {
	t.Helper()
	config.Consumer = name
	if _, err := js.AddConsumer(config.Stream, &nats.ConsumerConfig{
		Name: name, FilterSubject: config.Subject, AckPolicy: nats.AckExplicitPolicy,
		DeliverPolicy: nats.DeliverNewPolicy, AckWait: ackWait, InactiveThreshold: time.Minute,
	}); err != nil {
		t.Fatal(err)
	}
	return config
}

func confirmedPullAcks(ch chan<- struct{}) func(context.Context, natsjs.Observation) {
	return func(_ context.Context, event natsjs.Observation) {
		if event.Operation == natsjs.OperationAck && event.Outcome == natsjs.OutcomeConfirmed {
			ch <- struct{}{}
		}
	}
}

//nolint:gocritic // The consumer receives its own configuration value.
func runPull(t *testing.T, nc *nats.Conn, config natsjs.ConsumerConfig,
	ctx context.Context, handler broker.Handler,
) <-chan error {
	t.Helper()
	consumer, err := natsjs.NewPullConsumer(nc, config)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- consumer.Run(ctx, handler) }()
	return result
}

//nolint:gocritic // Read back the same immutable binding configuration.
func assertEphemeralProgress(t *testing.T, js nats.JetStreamContext, config natsjs.ConsumerConfig, sequence uint64) {
	t.Helper()
	info, err := js.ConsumerInfo(config.Stream, config.Consumer)
	if err != nil {
		t.Fatal(err)
	}
	if info.Name != config.Consumer || info.Config.Durable != "" || info.Config.DeliverSubject != "" ||
		info.Config.AckPolicy != nats.AckExplicitPolicy || info.Config.InactiveThreshold != time.Minute ||
		info.AckFloor.Stream != sequence || info.NumAckPending != 0 {
		t.Fatalf("ephemeral identity/progress: %+v", info)
	}
}
