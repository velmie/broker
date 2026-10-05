// Command orders demonstrates committed-marker replay across real NATS redelivery.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/velmie/idempo"
	"github.com/velmie/idempo/memory"

	"github.com/velmie/broker"
	"github.com/velmie/broker/idempotency"
	"github.com/velmie/broker/natsjs/v3"
)

const (
	sourceName      = "orders"
	workerName      = "worker"
	messageID       = "order-example-001"
	keyPrefix       = "orders:"
	exampleAmount   = 42
	operationBudget = 2 * time.Second
	callbackBudget  = 2 * time.Second
	routeBudget     = 20 * time.Second
	lockTTL         = 30 * time.Second
	resultTTL       = time.Minute
	ackWait         = 500 * time.Millisecond
)

func main() {
	endpoint := flag.String("url", "", "required loopback NATS endpoint with JetStream")
	flag.Parse()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{ReplaceAttr: redactAttribute}))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, *endpoint, logger)
	stop()
	if err != nil {
		logger.Error("idempotent orders failed", slog.Any("error", err))
		os.Exit(1)
	}
}

type order struct {
	Amount int `json:"amount"`
}

func run(ctx context.Context, endpoint string, logger *slog.Logger) (result error) {
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return failure("configure", "url", err)
	}
	if parsed.Scheme != "nats" || !net.ParseIP(parsed.Hostname()).IsLoopback() || parsed.User != nil ||
		parsed.RawQuery != "" || parsed.Fragment != "" {
		return failure("configure", "url", errors.New("expected explicit loopback NATS URL without credentials, query or fragment"))
	}
	ctx, cancel := context.WithTimeout(ctx, routeBudget)
	defer cancel()
	nc, err := nats.Connect(endpoint, nats.Timeout(operationBudget), nats.FlusherTimeout(operationBudget), nats.NoReconnect())
	if err != nil {
		return failure("connect", "", err)
	}
	defer nc.Close()
	js, err := nc.JetStream(nats.MaxWait(operationBudget))
	if err != nil {
		return failure("configure", "", err)
	}
	config, cleanup, err := provision(ctx, js)
	defer func() { result = errors.Join(result, cleanup()) }()
	if err != nil {
		return err
	}
	// The in-memory marker is local proof only and disappears with this process.
	store := memory.New()
	defer func() { result = errors.Join(result, failure("close_store", "", store.Close())) }()
	return demonstrateRedelivery(ctx, nc, js, &config, store, logger)
}

func demonstrateRedelivery(ctx context.Context, nc *nats.Conn, js nats.JetStreamContext,
	config *natsjs.ConsumerConfig, store *memory.Store, logger *slog.Logger) error {
	// The engine has no lease renewal. This lease exceeds the cooperating callback bound.
	engine := idempo.NewEngine(store, idempo.WithLockTTL(lockTTL), idempo.WithResultTTL(resultTTL))
	firstCtx, stopFirst := context.WithCancel(ctx)
	defer stopFirst()
	var effects, replays atomic.Int32
	middleware, err := idempotency.Middleware(engine, idempotency.WithRequireKey(true), idempotency.WithUseSourceInKey(false),
		idempotency.WithKeyPrefix(keyPrefix), idempotency.WithCommitTimeout(operationBudget),
		idempotency.WithOnReplay(func(context.Context, broker.Delivery, *idempo.Response) {
			replays.Add(1)
			logger.Info("stored completion replayed", "source", sourceName)
		}))
	if err != nil {
		return err
	}
	handler := broker.NewTypedHandler(broker.DecodeJSON[order], func(ctx context.Context, value order) error {
		work, cancelWork := context.WithTimeout(ctx, callbackBudget)
		defer cancelWork()
		if workErr := work.Err(); workErr != nil {
			return workErr
		}
		if value.Amount != exampleAmount {
			return failure("process", "amount", errors.New("unexpected amount"))
		}
		effects.Add(1)
		logger.Info("order effect completed", "source", sourceName)
		stopFirst() // The effect succeeded. Force shutdown before the adapter can ACK.
		return nil
	}).WithMiddleware(broker.LogProcessing(logger, broker.ProcessingLogConfig{}), broker.RecoverPanics(), middleware)
	if publishErr := publish(ctx, nc, config.Subject); publishErr != nil {
		return publishErr
	}
	if firstErr := firstRun(firstCtx, nc, config, handler); firstErr != nil {
		return firstErr
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if markerErr := verifyMarker(ctx, store); markerErr != nil {
		return markerErr
	}
	logger.Info("completion marker committed", "source", sourceName)
	logger.Info("first acknowledgment suppressed", "source", sourceName)
	info, err := js.ConsumerInfo(config.Stream, config.Consumer, nats.Context(ctx))
	if err != nil {
		return failure("readback", "", err)
	}
	if info.NumAckPending != 1 || info.AckFloor.Stream != 0 {
		return failure("readback", "", errors.New("first delivery was not pending"))
	}
	if secondErr := secondRun(ctx, nc, config, handler, logger); secondErr != nil {
		return secondErr
	}
	if effects.Load() != 1 || replays.Load() != 1 {
		return failure("process", "", errors.New("expected one effect and one replay"))
	}
	if markerErr := verifyMarker(ctx, store); markerErr != nil {
		return markerErr
	}
	if readbackErr := verifyEmpty(ctx, js, config); readbackErr != nil {
		return readbackErr
	}
	logger.Info("native consumer empty", "source", sourceName)
	return nil
}

func firstRun(ctx context.Context, nc *nats.Conn, config *natsjs.ConsumerConfig, handler broker.Handler) error {
	var suppressed atomic.Bool
	first := *config
	first.Shutdown = broker.ShutdownPolicy{Mode: broker.ShutdownCancel}
	first.Observer.Record = func(_ context.Context, event natsjs.Observation) {
		if event.Operation == natsjs.OperationAck && event.Outcome == natsjs.OutcomeSuppressed {
			suppressed.Store(true)
		}
	}
	consumer, err := natsjs.NewConsumer(nc, first)
	if err != nil {
		return err
	}
	err = consumer.Run(ctx, handler)
	if !onlyCancellation(err) {
		return err
	}
	if !suppressed.Load() {
		return failure("ack", "", errors.New("first acknowledgment was not suppressed"))
	}
	return nil
}
func secondRun(ctx context.Context, nc *nats.Conn, config *natsjs.ConsumerConfig, handler broker.Handler, logger *slog.Logger) error {
	work, stop := context.WithCancel(ctx)
	defer stop()
	var confirmed atomic.Bool
	second := *config
	second.Observer.Record = func(_ context.Context, event natsjs.Observation) {
		if event.Operation == natsjs.OperationAck && event.Outcome == natsjs.OutcomeConfirmed {
			confirmed.Store(true)
			logger.Info("native acknowledgment confirmed", "source", sourceName)
			stop()
		}
	}
	consumer, err := natsjs.NewConsumer(nc, second)
	if err != nil {
		return err
	}
	err = consumer.Run(work, handler)
	if ctx.Err() != nil {
		return errors.Join(err, ctx.Err())
	}
	if !onlyCancellation(err) {
		return err
	}
	if !confirmed.Load() {
		return failure("ack", "", errors.New("native acknowledgment not confirmed"))
	}
	return nil
}
func publish(ctx context.Context, nc *nats.Conn, subject string) error {
	pub, err := natsjs.NewPublisher(nc, natsjs.PublisherConfig{Subject: subject, OperationTimeout: operationBudget})
	if err != nil {
		return err
	}
	typed := broker.NewTypedPublisher[order](broker.EncoderFunc(json.Marshal), pub,
		func(context.Context, order) (broker.MessageMetadata, error) {
			return broker.MessageMetadata{ID: messageID}, nil
		})
	return typed(ctx, order{Amount: exampleAmount})
}
func verifyMarker(ctx context.Context, store *memory.Store) error {
	entry, err := store.Get(ctx, keyPrefix+messageID)
	if err != nil {
		return failure("read_marker", "", err)
	}
	if entry.Response == nil {
		return failure("read_marker", "", errors.New("committed marker absent"))
	}
	return nil
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
	if cause := errors.Unwrap(err); cause != nil {
		return onlyCancellation(cause)
	}
	return false
}

func provision(ctx context.Context, js nats.JetStreamContext) (natsjs.ConsumerConfig, func() error, error) {
	name := fmt.Sprintf("BROKER_IDEMPO_%d", time.Now().UnixNano())
	config := natsjs.ConsumerConfig{Stream: name, Consumer: workerName, Subject: name + ".orders",
		FetchTimeout: 100 * time.Millisecond, OperationTimeout: operationBudget, Shutdown: broker.ShutdownPolicy{GracePeriod: time.Second}}
	created := false
	cleanup := func() error {
		if !created {
			return nil
		}
		clean, cancel := context.WithTimeout(context.Background(), operationBudget)
		defer cancel()
		return failure("delete_stream", "", js.DeleteStream(name, nats.Context(clean)))
	}
	_, err := js.AddStream(&nats.StreamConfig{Name: name, Subjects: []string{config.Subject}, Storage: nats.MemoryStorage}, nats.Context(ctx))
	if err != nil {
		return config, cleanup, failure("create_stream", "", err)
	}
	created = true
	_, err = js.AddConsumer(name, &nats.ConsumerConfig{Durable: workerName, FilterSubject: config.Subject,
		AckPolicy: nats.AckExplicitPolicy, AckWait: ackWait}, nats.Context(ctx))
	return config, cleanup, failure("create_consumer", "", err)
}
func verifyEmpty(ctx context.Context, js nats.JetStreamContext, config *natsjs.ConsumerConfig) error {
	info, err := js.ConsumerInfo(config.Stream, config.Consumer, nats.Context(ctx))
	if err != nil {
		return failure("readback", "", err)
	}
	if info.NumPending != 0 || info.NumAckPending != 0 || info.AckFloor.Stream != 1 {
		return failure("readback", "", errors.New("native consumer has pending delivery or wrong ACK floor"))
	}
	return nil
}
