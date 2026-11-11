// Command filters processes two selected subjects through one durable consumer.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"

	"github.com/velmie/broker"
	"github.com/velmie/broker/natsjs/v4"
)

const (
	eventCreated             = "created"
	eventPaid                = "paid"
	exampleTimeout           = 15 * time.Second
	cleanupTimeout           = 2 * time.Second
	expectedAcknowledgements = 2
)

func main() {
	url := flag.String("url", nats.DefaultURL, "NATS server with JetStream enabled")
	flag.Parse()
	if err := run(*url); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

//nolint:gocyclo // One small executable owns the full provisioning, processing and cleanup flow.
func run(url string) (result error) {
	ctx, cancel := context.WithTimeout(context.Background(), exampleTimeout)
	defer cancel()
	conn, err := nats.Connect(url, nats.FlusherTimeout(time.Second))
	if err != nil {
		return fmt.Errorf("connect to NATS: %w", err)
	}
	defer conn.Close()
	js, err := jetstream.New(conn)
	if err != nil {
		return fmt.Errorf("open JetStream: %w", err)
	}
	stream := "BROKER_FILTERS_" + nuid.Next()
	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     stream,
		Subjects: []string{stream + ".*"},
		Storage:  jetstream.MemoryStorage,
	})
	if err != nil {
		return fmt.Errorf("create stream: %w", err)
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), cleanupTimeout)
		defer stop()
		if cleanupErr := js.DeleteStream(cleanup, stream); cleanupErr != nil {
			result = errors.Join(result, fmt.Errorf("delete stream: %w", cleanupErr))
		}
	}()
	filters := []string{stream + ".created", stream + ".paid"}
	cfg := natsjs.DurableConsumerConfig{
		Stream:        stream,
		Durable:       "worker",
		Subjects:      filters,
		AckWait:       time.Second,
		MaxDeliveries: -1,
		MaxAckPending: 10,
	}
	if ensureErr := natsjs.EnsureDurableConsumer(ctx, js, cfg); ensureErr != nil {
		return ensureErr
	}
	for _, event := range []string{eventCreated, "excluded", eventPaid} {
		if _, err = js.Publish(ctx, stream+"."+event, []byte(event)); err != nil {
			return fmt.Errorf("publish %s: %w", event, err)
		}
	}
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	acknowledged := 0
	consumer, err := natsjs.NewConsumer(conn, natsjs.ConsumerConfig{
		Stream:   stream,
		Consumer: cfg.Durable,
		Subjects: cfg.Subjects,
		Observer: natsjs.Observer{
			Record: func(_ context.Context, event natsjs.Observation) {
				if event.Operation == natsjs.OperationAck && event.Outcome == natsjs.OutcomeConfirmed {
					acknowledged++
					if acknowledged == expectedAcknowledgements {
						stop()
					}
				}
			},
		},
	})
	if err != nil {
		return err
	}
	seen := map[string]bool{}
	handler := broker.NewHandler(func(_ context.Context, delivery broker.Delivery) (broker.Disposition, error) {
		event := string(delivery.Message().Body)
		if event != eventCreated && event != eventPaid {
			return nil, fmt.Errorf("unexpected event %q", event)
		}
		seen[event] = true
		fmt.Println("processed", event)
		return broker.Handled{}, nil
	})
	if err = consumer.RunService(runCtx, handler); err != nil {
		return fmt.Errorf("run consumer: %w", err)
	}
	if ctx.Err() != nil {
		return fmt.Errorf("wait for deliveries: %w", ctx.Err())
	}
	if acknowledged != expectedAcknowledgements || !seen[eventCreated] || !seen[eventPaid] {
		return errors.New("selected events were not acknowledged")
	}
	info, err := js.Consumer(ctx, stream, cfg.Durable)
	if err != nil {
		return fmt.Errorf("read consumer: %w", err)
	}
	if info.CachedInfo().NumAckPending != 0 || info.CachedInfo().AckFloor.Stream != 3 {
		return errors.New("consumer acknowledgement progress is incomplete")
	}
	fmt.Println("acknowledged 2")
	return nil
}
