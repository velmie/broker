package natsjs_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/velmie/broker"
	"github.com/velmie/broker/natsjs/v3"
	"github.com/velmie/broker/natsjs/v3/examples/orders"
)

const testTimeout = 5 * time.Second

var serverURL, containerID string

func TestMain(m *testing.M) {
	serverImage := os.Getenv("NATS_TEST_IMAGE")
	if serverImage == "" {
		serverImage = "nats:2.15.0"
	}
	output, err := docker("run", "--rm", "-d", "-p", "127.0.0.1::4222", serverImage, "--jetstream")
	if err != nil {
		fmt.Fprintf(os.Stderr, "start NATS: %v\n%s", err, output)
		os.Exit(1)
	}
	containerID = strings.TrimSpace(string(output))
	output, err = docker("port", containerID, "4222/tcp")
	if err == nil {
		serverURL = "nats://" + strings.TrimSpace(string(output))
		err = waitForServer()
	}
	code := 1
	if err == nil {
		code = m.Run()
	} else {
		fmt.Fprintf(os.Stderr, "NATS port: %v\n%s", err, output)
	}
	if output, err = docker("rm", "-f", containerID); err != nil {
		fmt.Fprintf(os.Stderr, "NATS cleanup: %v\n%s", err, output)
		code = 1
	}
	os.Exit(code)
}

func TestTypedOrderEffectPrecedesConfirmedCompletion(t *testing.T) {
	nc, js, config := provision(t, time.Second)
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	var mu sync.Mutex
	var effects []string
	var observations []natsjs.Observation
	config.Observer.Record = func(_ context.Context, event natsjs.Observation) {
		mu.Lock()
		defer mu.Unlock()
		observations = append(observations, event)
		if event.Operation == natsjs.OperationAck && event.Outcome == natsjs.OutcomeConfirmed {
			if len(effects) != 1 || effects[0] != "order-1/book-1" {
				t.Error("ACK preceded the command effect")
			}
			stop()
		}
	}
	h := orders.NewHandler(func(_ context.Context, command orders.CreateOrderCommand) error {
		mu.Lock()
		defer mu.Unlock()
		effects = append(effects, command.ID+"/"+command.SKU)
		return nil
	})
	coordinator := broker.NewCoordinator()
	if err := coordinator.Add("create-order", func() (broker.Consumer, error) { return natsjs.NewConsumer(nc, config) }, h); err != nil {
		t.Fatal(err)
	}
	if _, err := js.Publish(config.Subject, []byte(`{"order_id":"order-1","product_code":"book-1","count":2}`)); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- coordinator.Run(ctx) }()
	if err := receive(t, result); !errors.Is(err, context.Canceled) {
		t.Fatalf("joined result: %v", err)
	}
	info, err := js.ConsumerInfo(config.Stream, config.Consumer)
	if err != nil || info.NumAckPending != 0 || info.AckFloor.Stream != 1 {
		t.Fatalf("completion readback: %+v, %v", info, err)
	}
	if nc.IsClosed() {
		t.Fatal("borrowed connection closed")
	}
	mu.Lock()
	defer mu.Unlock()
	var processed, confirmed bool
	for _, event := range observations {
		if event.Operation == natsjs.OperationProcess && event.Outcome == natsjs.OutcomeSucceeded {
			processed = true
		}
		if event.Operation == natsjs.OperationAck && event.Outcome == natsjs.OutcomeConfirmed {
			confirmed = true
		}
	}
	if !processed || !confirmed {
		t.Fatalf("missing distinct operation records: %+v", observations)
	}
}

func TestDynamicRetryAndNativeTermination(t *testing.T) {
	nc, js, config := provision(t, time.Second)
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	cause := errors.New("inventory unavailable")
	delays := []time.Duration{0, 37 * time.Millisecond, 83 * time.Millisecond}
	var called uint64
	var last time.Time
	config.Observer.Record = func(_ context.Context, event natsjs.Observation) {
		if event.Operation == natsjs.OperationNak && event.Outcome == natsjs.OutcomeConfirmed && !errors.Is(event.Cause, cause) {
			t.Error("retry cause lost")
		}
		if event.Operation == natsjs.OperationTerm && event.Outcome == natsjs.OutcomeConfirmed {
			stop()
		}
	}
	h := broker.NewTypedDeliveryHandler(broker.DecodeJSON[struct{}],
		func(_ context.Context, d broker.Delivery, _ struct{}) (broker.Disposition, error) {
			attempt := d.(broker.AttemptReader).Attempt()
			called++
			if !attempt.Known || attempt.Number != called {
				t.Errorf("attempt: %+v, call %d", attempt, called)
			}
			if called > 1 && time.Since(last) < delays[called-2] {
				t.Error("redelivery before requested delay")
			}
			last = time.Now()
			if called <= uint64(len(delays)) {
				return broker.RetryAfter{Delay: delays[called-1], Cause: cause}, nil
			}
			return natsjs.Terminate{}, nil
		}, broker.WithRequirements(broker.RedeliveryRequirement{}, broker.AttemptsRequirement{}, natsjs.TerminationRequirement{}))
	c, err := natsjs.NewConsumer(nc, config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = js.Publish(config.Subject, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- c.Run(ctx, h) }()
	if err = receive(t, result); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if called != uint64(len(delays))+1 {
		t.Fatalf("calls: %d", called)
	}
	info, err := js.ConsumerInfo(config.Stream, config.Consumer)
	if err != nil || info.NumAckPending != 0 {
		t.Fatalf("termination readback: %+v, %v", info, err)
	}
}

func provision(t *testing.T, ackWait time.Duration) (*nats.Conn, nats.JetStreamContext, natsjs.ConsumerConfig) {
	t.Helper()
	nc, err := nats.Connect(serverURL, nats.Timeout(testTimeout), nats.FlusherTimeout(time.Second), nats.NoReconnect())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := nc.JetStream(nats.MaxWait(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	name := strings.ReplaceAll(t.Name(), "/", "_")
	subject := name + ".orders"
	if _, err = js.AddStream(&nats.StreamConfig{Name: name, Subjects: []string{subject}, Storage: nats.MemoryStorage}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cleanupErr := js.DeleteStream(name); cleanupErr != nil {
			t.Error(cleanupErr)
		}
	})
	remote := &nats.ConsumerConfig{Durable: "worker", FilterSubject: subject, AckPolicy: nats.AckExplicitPolicy, AckWait: ackWait}
	if _, err = js.AddConsumer(name, remote); err != nil {
		t.Fatal(err)
	}
	return nc, js, natsjs.ConsumerConfig{
		Stream: name, Consumer: "worker", Subject: subject, FetchTimeout: 100 * time.Millisecond, OperationTimeout: time.Second,
	}
}

func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for owned work")
	}
	var zero T
	return zero
}

func docker(args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	//nolint:gosec // Test-owned Docker arguments are passed directly without a shell.
	output, err := exec.CommandContext(ctx, "docker", args...).Output()
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		err = fmt.Errorf("%w: %s", err, exit.Stderr)
	}
	return output, err
}

func waitForServer() error {
	timer := time.NewTimer(testTimeout)
	defer timer.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		nc, err := nats.Connect(serverURL, nats.Timeout(100*time.Millisecond), nats.NoReconnect())
		if err == nil {
			nc.Close()
			return nil
		}
		select {
		case <-timer.C:
			return err
		case <-ticker.C:
		}
	}
}
