package natsjs_test

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/velmie/broker"
	"github.com/velmie/broker/natsjs/v4"
)

type gatedDialer struct {
	blocked atomic.Bool
}

func (d *gatedDialer) Dial(network, address string) (net.Conn, error) {
	if d.blocked.Load() {
		return nil, errors.New("test reconnect gate closed")
	}
	return (&net.Dialer{
		Timeout: time.Second,
	}).Dial(network, address)
}

func reconnectConnection(t *testing.T, opts ...nats.Option) *nats.Conn {
	t.Helper()
	options := []nats.Option{nats.FlusherTimeout(time.Second), nats.ReconnectWait(50 * time.Millisecond),
		nats.ReconnectJitter(0, 0), nats.MaxReconnects(-1)}
	options = append(options, opts...)
	nc, err := nats.Connect(serverURL, options...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	return nc
}

func waitUntil(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(testTimeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for condition")
}

//nolint:gocritic // The helper reads an immutable test configuration.
func waitPull(t *testing.T, js nats.JetStreamContext, config natsjs.ConsumerConfig) {
	t.Helper()
	waitUntil(t, func() bool {
		info, err := js.ConsumerInfo(config.Stream, config.Consumer)
		return err == nil && info.NumWaiting > 0
	})
}

func restartServer(t *testing.T) {
	t.Helper()
	out, err := docker("restart", containerID)
	if err != nil {
		t.Fatalf("restart: %v: %s", err, out)
	}
	out, err = docker("port", containerID, "4222/tcp")
	if err != nil || "nats://"+strings.TrimSpace(string(out)) != serverURL {
		t.Fatalf("endpoint changed: %s, %v", out, err)
	}
	if err = waitForServer(); err != nil {
		t.Fatal(err)
	}
}

//nolint:gocritic // Each run receives an independent test configuration.
func reconnectRun(
	t *testing.T,
	nc *nats.Conn,
	config natsjs.ConsumerConfig,
	push bool,
) (context.CancelFunc, <-chan error, <-chan string, <-chan natsjs.Observation) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	events := make(chan natsjs.Observation, 100)
	calls := make(chan string, 10)
	record := config.Observer.Record
	config.Observer.Record = func(ctx context.Context, event natsjs.Observation) {
		if record != nil {
			record(ctx, event)
		}
		events <- event
	}
	var c *natsjs.Consumer
	var err error
	if push {
		c, err = natsjs.NewPushConsumer(nc, config)
	} else {
		c, err = natsjs.NewPullConsumer(nc, config)
	}
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	done := make(chan struct{})
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(testTimeout):
			t.Error("Run did not join during cleanup")
		}
	})
	go func() {
		defer close(done)
		result <- c.Run(ctx, broker.NewHandler(func(_ context.Context, d broker.Delivery) (broker.Disposition, error) {
			calls <- string(d.Message().Body)
			return broker.Handled{}, nil
		}))
	}()
	return cancel, result, calls, events
}

func startHeldPull(t *testing.T, sub *nats.Subscription) <-chan *nats.Msg {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	result := make(chan *nats.Msg, 1)
	done := make(chan struct{})
	var fetchErr error
	go func() {
		defer close(done)
		messages, err := sub.Fetch(1, nats.Context(ctx))
		fetchErr = err
		if err == nil {
			result <- messages[0]
		}
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
			if fetchErr != nil && !errors.Is(fetchErr, context.Canceled) {
				t.Errorf("held pull: %v", fetchErr)
			}
		case <-time.After(testTimeout):
			t.Error("held pull did not join")
		}
	})
	return result
}

func eventMatching(t *testing.T, events <-chan natsjs.Observation, op, outcome string) natsjs.Observation {
	t.Helper()
	timer := time.NewTimer(testTimeout)
	defer timer.Stop()
	for {
		select {
		case event := <-events:
			if event.Operation == op && event.Outcome == outcome {
				return event
			}
		case <-timer.C:
			t.Fatalf("timed out waiting for %s/%s", op, outcome)
		}
	}
}

func TestPullConsumerSurvivesServerRestart(t *testing.T) {
	nc := reconnectConnection(t)
	js, config := provisionTopology(t, nc, nats.FileStorage, &nats.ConsumerConfig{
		AckWait: time.Minute,
	})
	if _, err := js.Publish(config.Subjects[0], []byte("old")); err != nil {
		t.Fatal(err)
	}
	sub, err := js.PullSubscribe(config.Subjects[0], config.Consumer, nats.Bind(config.Stream, config.Consumer))
	if err != nil {
		t.Fatal(err)
	}
	messages, err := sub.Fetch(1, nats.MaxWait(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err = sub.Unsubscribe(); err != nil {
		t.Fatal(err)
	}
	config.FetchTimeout = 3 * time.Second
	stop, result, calls, events := reconnectRun(t, nc, config, false)
	waitPull(t, js, config)
	restartServer(t)
	eventMatching(t, events, natsjs.OperationFetch, "interrupted")
	waitUntil(t, nc.IsConnected)
	if err = messages[0].Nak(); err != nil {
		t.Fatal(err)
	}
	if _, err = js.Publish(config.Subjects[0], []byte("new")); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{receive(t, calls): true, receive(t, calls): true}
	if !seen["old"] || !seen["new"] {
		t.Fatalf("deliveries: %v", seen)
	}
	eventMatching(t, events, natsjs.OperationAck, natsjs.OutcomeConfirmed)
	eventMatching(t, events, natsjs.OperationAck, natsjs.OutcomeConfirmed)
	info, err := js.ConsumerInfo(config.Stream, config.Consumer)
	if err != nil || info.NumAckPending != 0 || info.AckFloor.Stream != 2 {
		t.Fatalf("progress: %+v %v", info, err)
	}
	stop()
	if err = receive(t, result); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestLegacyFetchServerShutdownStatus(t *testing.T) {
	nc := reconnectConnection(t)
	js, err := nc.JetStream(nats.APIPrefix("test.reconnect"))
	if err != nil {
		t.Fatal(err)
	}
	responder, err := nc.Subscribe("test.reconnect.CONSUMER.MSG.NEXT.>", func(m *nats.Msg) {
		reply := &nats.Msg{
			Subject: m.Reply,
			Header:  nats.Header{"Status": []string{"409"}, "Description": []string{"Server Shutdown"}},
		}
		if publishErr := nc.PublishMsg(reply); publishErr != nil {
			t.Error(publishErr)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cleanupErr := responder.Unsubscribe(); cleanupErr != nil {
			t.Error(cleanupErr)
		}
	})
	if err = nc.Flush(); err != nil {
		t.Fatal(err)
	}
	sub, err := js.PullSubscribe("test.subject", "worker", nats.Bind("stream", "worker"), nats.SkipConsumerLookup())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cleanupErr := sub.Unsubscribe(); cleanupErr != nil {
			t.Error(cleanupErr)
		}
	})
	_, err = sub.Fetch(1, nats.MaxWait(time.Second))
	if err == nil || err.Error() != "nats: Server Shutdown" {
		t.Fatalf("native fetch: %v", err)
	}
}

func TestPullConsumerRetriesMaxWaiting(t *testing.T) {
	nc := reconnectConnection(t)
	js, config := provisionTopology(t, nc, nats.MemoryStorage, &nats.ConsumerConfig{
		AckWait:    time.Second,
		MaxWaiting: 1,
	})
	held, err := js.PullSubscribe(config.Subjects[0], config.Consumer, nats.Bind(config.Stream, config.Consumer))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cleanupErr := held.Unsubscribe(); cleanupErr != nil {
			t.Error(cleanupErr)
		}
	})
	fetched := startHeldPull(t, held)
	waitPull(t, js, config)
	config.FetchTimeout = 2 * time.Second
	times := make(chan time.Time, 10)
	config.Observer.Record = func(_ context.Context, event natsjs.Observation) {
		if event.Operation == natsjs.OperationFetch && event.Outcome == "interrupted" {
			times <- time.Now()
		}
	}
	stop, result, calls, events := reconnectRun(t, nc, config, false)
	first := eventMatching(t, events, natsjs.OperationFetch, "interrupted")
	second := eventMatching(t, events, natsjs.OperationFetch, "interrupted")
	firstTime, secondTime := receive(t, times), receive(t, times)
	var firstOperation, secondOperation *natsjs.OperationError
	if !errors.As(first.Err, &firstOperation) || firstOperation.Cause.Error() != "nats: Exceeded MaxWaiting" ||
		!errors.As(second.Err, &secondOperation) || secondOperation.Cause.Error() != "nats: Exceeded MaxWaiting" ||
		secondTime.Sub(firstTime) < 250*time.Millisecond {
		t.Fatalf("interruptions: %v, %v after %v", first.Err, second.Err, secondTime.Sub(firstTime))
	}
	if _, err = js.Publish(config.Subjects[0], []byte("held")); err != nil {
		t.Fatal(err)
	}
	msg := receive(t, fetched)
	if err = msg.AckSync(); err != nil {
		t.Fatal(err)
	}
	if _, err = js.Publish(config.Subjects[0], []byte("managed")); err != nil {
		t.Fatal(err)
	}
	if got := receive(t, calls); got != "managed" {
		t.Fatal(got)
	}
	eventMatching(t, events, natsjs.OperationAck, natsjs.OutcomeConfirmed)
	stop()
	if err = receive(t, result); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestPullConsumerStopsOnConsumerDeleted(t *testing.T) {
	nc := reconnectConnection(t)
	js, config := provisionTopology(t, nc, nats.MemoryStorage, &nats.ConsumerConfig{
		AckWait: time.Second,
	})
	config.FetchTimeout = 3 * time.Second
	_, result, _, events := reconnectRun(t, nc, config, false)
	waitPull(t, js, config)
	if err := js.DeleteConsumer(config.Stream, config.Consumer); err != nil {
		t.Fatal(err)
	}
	if err := receive(t, result); !errors.Is(err, nats.ErrConsumerDeleted) {
		t.Fatal(err)
	}
	event := eventMatching(t, events, natsjs.OperationFetch, natsjs.OutcomeFailed)
	if !errors.Is(event.Err, nats.ErrConsumerDeleted) {
		t.Fatal(event.Err)
	}
}

func TestPullConsumerStopsAfterTopologyDeletionDuringReconnect(t *testing.T) {
	for _, kind := range []string{"consumer", "stream"} {
		t.Run(kind, func(t *testing.T) {
			dialer := &gatedDialer{}
			nc := reconnectConnection(t, nats.SetCustomDialer(dialer))
			admin := reconnectConnection(t)
			js, config := provisionTopology(t, admin, nats.FileStorage, &nats.ConsumerConfig{
				AckWait: time.Second,
			})
			config.FetchTimeout = 3 * time.Second

			_, result, _, events := reconnectRun(t, nc, config, false)
			waitPull(t, js, config)
			dialer.blocked.Store(true)
			t.Cleanup(func() { dialer.blocked.Store(false) })
			restartServer(t)
			waitUntil(t, func() bool { return !nc.IsConnected() && admin.IsConnected() })
			var err error
			if kind == "consumer" {
				err = js.DeleteConsumer(config.Stream, config.Consumer)
			} else {
				err = js.DeleteStream(config.Stream)
			}
			if err != nil {
				t.Fatal(err)
			}

			dialer.blocked.Store(false)
			err = receive(t, result)
			want := nats.ErrConsumerNotFound
			if kind == "stream" {
				want = nats.ErrStreamNotFound
			}
			if !errors.Is(err, want) {
				t.Fatalf("terminal causes: %v", err)
			}
			event := eventMatching(t, events, natsjs.OperationFetch, natsjs.OutcomeFailed)
			if !errors.Is(event.Err, want) {
				t.Fatal(event.Err)
			}
			_, err = js.ConsumerInfo(config.Stream, config.Consumer)
			if !errors.Is(err, want) {
				t.Fatalf("topology recreated: %v", err)
			}
		})
	}
}

func TestPullConsumerStopsAfterConsumerDeletionBetweenFetchesDuringReconnect(t *testing.T) {
	dialer := &gatedDialer{}
	nc := reconnectConnection(t, nats.SetCustomDialer(dialer))
	admin := reconnectConnection(t)
	js, config := provisionTopology(t, admin, nats.FileStorage, &nats.ConsumerConfig{
		AckWait: time.Second,
	})
	config.FetchTimeout = time.Second
	idle := make(chan struct{}, 1)
	release := make(chan struct{})
	var paused atomic.Bool
	config.Observer.Record = func(ctx context.Context, event natsjs.Observation) {
		if event.Operation == natsjs.OperationFetch && event.Outcome == natsjs.OutcomeIdle &&
			paused.CompareAndSwap(false, true) {
			idle <- struct{}{}
			select {
			case <-release:
			case <-ctx.Done():
			}
		}
	}
	_, result, _, events := reconnectRun(t, nc, config, false)
	receive(t, idle)
	lookups := make(chan struct{}, 1)
	monitor, err := admin.Subscribe("$JS.API.CONSUMER.INFO."+config.Stream+"."+config.Consumer, func(_ *nats.Msg) {
		select {
		case lookups <- struct{}{}:
		default:
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cleanupErr := monitor.Unsubscribe(); cleanupErr != nil {
			t.Error(cleanupErr)
		}
	})
	if err = admin.Flush(); err != nil {
		t.Fatal(err)
	}
	reconnects := nc.Stats().Reconnects
	dialer.blocked.Store(true)
	t.Cleanup(func() { dialer.blocked.Store(false) })
	restartServer(t)
	waitUntil(t, func() bool { return !nc.IsConnected() && admin.IsConnected() })
	if err := js.DeleteConsumer(config.Stream, config.Consumer); err != nil {
		t.Fatal(err)
	}
	dialer.blocked.Store(false)
	waitUntil(t, func() bool { return nc.IsConnected() && nc.Stats().Reconnects > reconnects })
	close(release)
	if err := receive(t, result); !errors.Is(err, nats.ErrConsumerNotFound) {
		t.Fatalf("terminal cause: %v", err)
	}
	select {
	case <-lookups:
	case <-time.After(testTimeout):
		t.Fatal("reconnect did not trigger consumer lookup")
	}
	assertMissingConsumerWithoutInterruptedPull(t, events)
}

func assertMissingConsumerWithoutInterruptedPull(t *testing.T, events <-chan natsjs.Observation) {
	t.Helper()
	for {
		event := receive(t, events)
		if event.Operation != natsjs.OperationFetch {
			continue
		}
		if event.Outcome == natsjs.OutcomeInterrupted {
			t.Fatalf("pull was interrupted before consumer lookup: %v", event.Err)
		}
		if event.Outcome == natsjs.OutcomeFailed {
			if !errors.Is(event.Err, nats.ErrConsumerNotFound) {
				t.Fatal(event.Err)
			}
			return
		}
	}
}

func TestPullConsumerStopsOnClosedConnection(t *testing.T) {
	nc := reconnectConnection(t)
	admin := reconnectConnection(t)
	js, config := provisionTopology(t, admin, nats.MemoryStorage, &nats.ConsumerConfig{
		AckWait: time.Second,
	})
	config.FetchTimeout = 3 * time.Second
	var interrupted atomic.Bool
	config.Observer.Record = func(_ context.Context, event natsjs.Observation) {
		if event.Operation == natsjs.OperationFetch && event.Outcome == natsjs.OutcomeInterrupted {
			interrupted.Store(true)
		}
	}
	_, result, _, events := reconnectRun(t, nc, config, false)
	waitPull(t, js, config)
	nc.Close()
	if err := receive(t, result); !errors.Is(err, nats.ErrConnectionClosed) {
		t.Fatal(err)
	}
	eventMatching(t, events, natsjs.OperationFetch, natsjs.OutcomeFailed)
	if interrupted.Load() {
		t.Fatal("closed connection was selected for recovery")
	}
}

func TestPullConsumerCancellationDuringRecovery(t *testing.T) {
	t.Run("connected delay", func(t *testing.T) {
		nc := reconnectConnection(t)
		js, config := provisionTopology(t, nc, nats.MemoryStorage, &nats.ConsumerConfig{
			AckWait:    time.Second,
			MaxWaiting: 1,
		})
		held, err := js.PullSubscribe(config.Subjects[0], config.Consumer, nats.Bind(config.Stream, config.Consumer))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if cleanupErr := held.Unsubscribe(); cleanupErr != nil {
				t.Error(cleanupErr)
			}
		})
		startHeldPull(t, held)
		waitPull(t, js, config)
		stop, result, _, events := reconnectRun(t, nc, config, false)
		eventMatching(t, events, natsjs.OperationFetch, "interrupted")
		stop()
		err = receive(t, result)
		if !errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "MaxWaiting") {
			t.Fatal(err)
		}
	})
	t.Run("disconnected polling", func(t *testing.T) {
		dialer := &gatedDialer{}
		nc := reconnectConnection(t, nats.SetCustomDialer(dialer))
		admin := reconnectConnection(t)
		js, config := provisionTopology(t, admin, nats.FileStorage, &nats.ConsumerConfig{
			AckWait: time.Second,
		})
		config.FetchTimeout = 3 * time.Second
		stop, result, _, events := reconnectRun(t, nc, config, false)
		waitPull(t, js, config)
		dialer.blocked.Store(true)
		t.Cleanup(func() { dialer.blocked.Store(false) })
		restartServer(t)
		eventMatching(t, events, natsjs.OperationFetch, "interrupted")
		stop()
		err := receive(t, result)
		if !errors.Is(err, context.Canceled) || errors.Is(err, nats.ErrFetchDisconnected) {
			t.Fatal(err)
		}
	})
}

func TestPushConsumerSurvivesServerRestart(t *testing.T) {
	nc := reconnectConnection(t)
	js, config := provisionTopology(t, nc, nats.FileStorage, &nats.ConsumerConfig{
		AckWait: time.Second,
	})
	config = provisionPush(t, js, config, "", false, time.Second)
	config.FetchTimeout = 3 * time.Second
	stop, result, calls, events := reconnectRun(t, nc, config, true)
	eventMatching(t, events, natsjs.OperationStartup, natsjs.OutcomeSucceeded)
	restartServer(t)
	waitUntil(t, nc.IsConnected)
	if _, err := js.Publish(config.Subjects[0], []byte("push")); err != nil {
		t.Fatal(err)
	}
	if got := receive(t, calls); got != "push" {
		t.Fatal(got)
	}
	eventMatching(t, events, natsjs.OperationAck, natsjs.OutcomeConfirmed)
	stop()
	if err := receive(t, result); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestPullConsumerRetriesReconnectProbe(t *testing.T) {
	t.Run("consumer deleted", func(t *testing.T) { testPullConsumerRetriesReconnectProbe(t, true) })
	t.Run("consumer remains", func(t *testing.T) { testPullConsumerRetriesReconnectProbe(t, false) })
}

func testPullConsumerRetriesReconnectProbe(t *testing.T, deleted bool) {
	t.Helper()
	dialer := &gatedDialer{}
	nc := reconnectConnection(t, nats.SetCustomDialer(dialer))
	admin := reconnectConnection(t)
	js, config := provisionTopology(t, admin, nats.FileStorage, &nats.ConsumerConfig{
		AckWait: time.Second,
	})
	const prefix = "retryprobe"
	dropNext, lookups := startConsumerProxy(t, admin, prefix)
	config.APIPrefix = prefix
	config.FetchTimeout = time.Second
	config.OperationTimeout = 300 * time.Millisecond
	stop, result, _, events := reconnectRun(t, nc, config, false)
	waitPull(t, js, config)
	dialer.blocked.Store(true)
	t.Cleanup(func() { dialer.blocked.Store(false) })
	restartServer(t)
	waitUntil(t, func() bool { return !nc.IsConnected() && admin.IsConnected() })
	if deleted {
		if err := js.DeleteConsumer(config.Stream, config.Consumer); err != nil {
			t.Fatal(err)
		}
	}
	dropNext.Store(true)
	dialer.blocked.Store(false)
	waitUntil(t, func() bool { return lookups.Load() >= 2 })
	if deleted {
		err := receive(t, result)
		if !errors.Is(err, nats.ErrConsumerNotFound) {
			t.Fatalf("lookup after temporary failure: %v", err)
		}
		var event natsjs.Observation
		for {
			event = eventMatching(t, events, natsjs.OperationFetch, natsjs.OutcomeInterrupted)
			if errors.Is(event.Err, context.DeadlineExceeded) || errors.Is(event.Err, nats.ErrTimeout) {
				break
			}
		}
		event = eventMatching(t, events, natsjs.OperationFetch, natsjs.OutcomeFailed)
		if !errors.Is(event.Err, nats.ErrConsumerNotFound) {
			t.Fatalf("terminal causes: %v", event.Err)
		}
		return
	}
	for range 3 {
		eventMatching(t, events, natsjs.OperationFetch, natsjs.OutcomeIdle)
	}
	if got := lookups.Load(); got != 2 {
		t.Fatalf("consumer lookup continued after recovery: %d requests", got)
	}
	stop()
	if err := receive(t, result); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func startConsumerProxy(t *testing.T, admin *nats.Conn, prefix string) (*atomic.Bool, *atomic.Int32) {
	t.Helper()
	dropNext := &atomic.Bool{}
	lookups := &atomic.Int32{}
	proxy, err := admin.Subscribe(prefix+".CONSUMER.>", func(msg *nats.Msg) {
		if strings.Contains(msg.Subject, ".CONSUMER.INFO.") && dropNext.Load() {
			lookups.Add(1)
			if dropNext.CompareAndSwap(true, false) {
				return
			}
		}
		if strings.Contains(msg.Subject, ".CONSUMER.INFO.") && lookups.Load() > 0 {
			lookups.Add(1)
		}
		target := "$JS.API" + strings.TrimPrefix(msg.Subject, prefix)
		if publishErr := admin.PublishRequest(target, msg.Reply, msg.Data); publishErr != nil {
			t.Error(publishErr)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cleanupErr := proxy.Unsubscribe(); cleanupErr != nil {
			t.Error(cleanupErr)
		}
	})
	if err = admin.Flush(); err != nil {
		t.Fatal(err)
	}
	return dropNext, lookups
}
