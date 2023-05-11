package natsjs_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/pkg/errors"
	"github.com/velmie/broker"

	"github.com/velmie/broker/natsjs/v2/conn"
	"github.com/velmie/broker/natsjs/v2/publisher"
	"github.com/velmie/broker/natsjs/v2/subscriber"
)

var srv *server

type server struct {
	pool      *dockertest.Pool
	container *dockertest.Resource
	conn      *nats.Conn
	js        nats.JetStreamContext
}

func startServer() (*server, error) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		return nil, err
	}

	err = pool.Client.Ping()
	if err != nil {
		return nil, err
	}

	cont, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "nats",
		Tag:        "latest",
		Cmd: []string{
			"--jetstream",
		},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"4222": {{HostIP: "localhost", HostPort: "4222"}},
		},
		ExposedPorts: []string{"4222"},
	}, func(cfg *docker.HostConfig) {
		cfg.RestartPolicy = docker.NeverRestart()
		cfg.AutoRemove = true
	})
	if err != nil {
		return nil, err
	}

	// set expiration for resource, so even if something went really wrong resources are released after 30 seconds
	if err = cont.Expire(30); err != nil {
		return nil, err
	}

	var (
		conn *nats.Conn
		js   nats.JetStreamContext
	)
	err = pool.Retry(func() error {
		// connect to server via native package, which will be used for verification of topology, etc.
		conn, err = nats.Connect(nats.DefaultURL)
		if err != nil {
			return err
		}

		js, err = conn.JetStream()
		if err != nil {
			return err
		}

		return nil
	})

	return &server{
		pool:      pool,
		container: cont,
		conn:      conn,
		js:        js,
	}, nil
}

func (s *server) purge() error {
	return s.pool.Purge(s.container)
}

func (s *server) streamInfo(stream string) (*nats.StreamInfo, error) {
	return s.js.StreamInfo(stream)
}

func (s *server) consumerInfo(stream string, consumer string) (*nats.ConsumerInfo, error) {
	return s.js.ConsumerInfo(stream, consumer)
}

func (s *server) pause() error {
	return s.pool.Client.PauseContainer(s.container.Container.ID)
}

func (s *server) unpause() error {
	return s.pool.Client.UnpauseContainer(s.container.Container.ID)
}

func TestMain(m *testing.M) {
	s, err := startServer()
	if err != nil {
		log.Fatalf("failed to start NAST server in docker: %v", err)
	}
	srv = s

	code := m.Run()

	if err = srv.purge(); err != nil {
		log.Fatalf("failed to purge docker resources: %v", err)
	}

	os.Exit(code)
}

func TestPublisher_WaitFailedConnectRetry(t *testing.T) {
	const connTimeout = 5 * time.Second

	startCh := make(chan struct{}, 1)

	if err := srv.pause(); err != nil {
		t.Fatalf("failed to pause server container: %v", err)
	}

	// we start publisher in new goroutine trying to connect
	go func() {
		// start unpause async
		go func() {
			if err := srv.unpause(); err != nil {
				t.Fatalf("failed to unpause server container: %v", err)
			}
		}()

		p, connErr := publisher.Connect(
			publisher.ConnectionOptions(
				conn.URL(nats.DefaultURL),
				conn.NATSOptions(
					nats.ReconnectWait(time.Second/2), // try to reconnect every half of second
					nats.RetryOnFailedConnect(true),
				),
			),
		)
		if connErr != nil {
			t.Fatalf("connection await was expected but no success: %v", connErr)
		}
		defer p.Close()

		// notify we connected after wait
		startCh <- struct{}{}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), connTimeout)
	defer cancel()

	// we wait few seconds for successful connection or raise timeout
	select {
	case <-ctx.Done():
		t.Fatalf("failed to connect within timeout: %s", connTimeout)
	case <-startCh:
	}
}

func TestPublisher_NoWaitFailedConnectRetry(t *testing.T) {
	if err := srv.pause(); err != nil {
		t.Fatalf("failed to pause server container: %v", err)
	}

	// unpause in the end no matter if test failed or not
	defer func() {
		if err := srv.unpause(); err != nil {
			t.Fatalf("failed to unpause server container: %v", err)
		}
	}()

	p, err := publisher.Connect(
		publisher.ConnectionOptions(
			conn.NATSOptions(
				nats.ReconnectWait(time.Second), // try to reconnect every second
				nats.RetryOnFailedConnect(true),
			),
			conn.NoWaitFailedConnectRetry(),
		),
		publisher.PubOptions(
			// minimize wait time for timeout
			nats.AckWait(time.Millisecond),
			nats.RetryWait(time.Millisecond),
			nats.RetryAttempts(1),
		),
	)
	if err != nil {
		t.Fatalf("no error expected on connect, but it occurred: %v", err)
	}
	defer p.Close()

	// publish won't succeed since container is paused
	err = p.Publish("random.subject", &broker.Message{
		ID:   "random-id",
		Body: []byte("random message"),
	})
	if !errors.Is(err, nats.ErrTimeout) && !errors.Is(err, nats.ErrHeadersNotSupported) {
		t.Fatalf("expected reconnection state but succeed to reach the server: %v", err)
	}
}

func TestPublisher_InitStream(t *testing.T) {
	const initStream = "INIT-STREAM"

	cfg := &nats.StreamConfig{
		Name:     initStream,
		Subjects: []string{"INIT-SUBJECT.>"},
		NoAck:    true,
	}

	p, err := publisher.Connect(
		publisher.InitJetStream(cfg),
	)
	if err != nil {
		t.Fatalf("failed to connect publisher to NATS server: %v", err)
	}
	defer p.Close()

	_, err = srv.streamInfo(initStream)
	if err != nil {
		t.Fatalf("failed to get stream info: %v", err)
	}
}

func TestSubscriber_DefaultPullSubscriber(t *testing.T) {
	const (
		stream  = "PULL-SUB"
		subject = "pull.new"
	)

	type msg struct {
		id     string
		data   string
		header map[string]string
	}

	pub, err := publisher.Connect(
		publisher.InitJetStream(&nats.StreamConfig{
			Name:      stream,
			Subjects:  []string{"pull.>"},
			Retention: nats.InterestPolicy,
		}),
	)
	if err != nil {
		t.Fatalf("failed to connect publisher to NATS server: %v", err)
	}
	defer pub.Close()

	sub, err := subscriber.Connect(
		stream,
		subscriber.SubscriptionFactory(subscriber.DefaultSubscriptionFactory()),
	)
	if err != nil {
		t.Fatalf("failed to connect subscriber to NATS server: %v", err)
	}
	defer sub.Close()

	resCh := make(chan msg, 1)
	errCh := make(chan error, 1)

	handler := func(evt broker.Event) error {
		bm := evt.Message()
		m := msg{
			id:     bm.ID,
			data:   string(bm.Body),
			header: bm.Header,
		}
		resCh <- m

		return evt.Ack()
	}

	errHandler := func(err error, sub broker.Subscription) {
		errCh <- err
	}

	ns, err := sub.Subscribe(subject, handler, broker.WithErrorHandler(errHandler))
	if err != nil {
		t.Fatalf("failed to create subscription: %v", err)
	}
	defer func() {
		if err = ns.Unsubscribe(); err != nil {
			t.Fatalf("failed to unsubscribe for subject %s: %v", subject, err)
		}
	}()

	expected := msg{
		id:   "random-id",
		data: "random-data",
		header: map[string]string{
			"random-header": "random-header-value",
			"natsjs-msg-id": "random-id",
		},
	}

	err = pub.Publish(subject, &broker.Message{
		ID:     expected.id,
		Header: expected.header,
		Body:   []byte(expected.data),
	})
	if err != nil {
		t.Fatalf("failed to publish message to subject %s: %v", subject, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	select {
	case <-ctx.Done():
		t.Fatal("processing of message is running for too long")
	case err = <-errCh:
		t.Fatalf("error occurred on processing message: %v", err)
	case actual := <-resCh:
		if !reflect.DeepEqual(expected, actual) {
			t.Fatalf("expected to get %v but got %v", expected, actual)
		}
	}
}

func TestSubscriber_MultiplePullSubscribers(t *testing.T) {
	const (
		count   = 50
		stream  = "MULTI-PULL-SUB"
		subject = "multipull.new"
	)

	pub, err := publisher.Connect(
		publisher.InitJetStream(&nats.StreamConfig{
			Name:      stream,
			Subjects:  []string{"multipull.>"},
			Retention: nats.InterestPolicy,
		}),
	)
	if err != nil {
		t.Fatalf("failed to connect publisher to NATS server: %v", err)
	}
	defer pub.Close()

	// we are going to increase atomic until each time message is retrieved and handled
	var called atomic.Int32
	doneCh := make(chan struct{}, 1)
	errCh := make(chan error, 1)

	handler := func(evt broker.Event) error {
		called.Add(1)
		if called.Load() == count { // as soon as the last message arrived, notify that is done
			doneCh <- struct{}{}
		}
		return nil
	}

	errHandler := func(err error, sub broker.Subscription) {
		errCh <- err
	}

	sub, err := subscriber.Connect(
		stream,
		subscriber.SubscriptionFactory(subscriber.DefaultSubscriptionFactory()),
	)
	if err != nil {
		t.Fatalf("failed to connect subscriber to NATS server: %v", err)
	}
	defer sub.Close()

	// create 3 subscriptions with the same durable
	ns1, err := sub.Subscribe(subject, handler, broker.WithErrorHandler(errHandler))
	if err != nil {
		t.Fatalf("failed to subscribe to subject %s: %v", subject, err)
	}
	defer ns1.Unsubscribe()

	ns2, err := sub.Subscribe(subject, handler, broker.WithErrorHandler(errHandler))
	if err != nil {
		t.Fatalf("failed to subscribe to subject %s: %v", subject, err)
	}
	defer ns2.Unsubscribe()

	ns3, err := sub.Subscribe(subject, handler, broker.WithErrorHandler(errHandler))
	if err != nil {
		t.Fatalf("failed to subscribe to subject %s: %v", subject, err)
	}
	defer ns3.Unsubscribe()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	for i := 0; i < count; i++ {
		err = pub.Publish(subject, &broker.Message{
			ID: fmt.Sprintf("test-id-%d", i+1),
		})
		if err != nil {
			t.Fatalf("failed to publish message to subject %s: %v", subject, err)
		}
	}

	select {
	case <-ctx.Done():
		t.Fatal("processing of message is running for too long")
	case err = <-errCh:
		t.Fatalf("error occurred on processing message: %v", err)
	case <-doneCh:
		if actual := called.Load(); actual != count {
			t.Fatalf("expected to call message processing %v times but processed %v times", count, actual)
		}
	}
}

func TestSubscriber_MultipleAsyncSubscribers(t *testing.T) {
	const (
		count    = 50
		stream   = "MULTI-ASYNC-SUB"
		subject  = "multiasync.new"
		expected = count * 2 // 2 async subscriptions
	)

	pub, err := publisher.Connect(
		publisher.InitJetStream(&nats.StreamConfig{
			Name:      stream,
			Subjects:  []string{"multiasync.>"},
			Retention: nats.InterestPolicy,
		}),
	)
	if err != nil {
		t.Fatalf("failed to connect publisher to NATS server: %v", err)
	}
	defer pub.Close()

	// we are going to increase atomic until each time message is retrieved and handled
	var called atomic.Int32
	doneCh := make(chan struct{}, 1)
	errCh := make(chan error, 1)

	handler := func(evt broker.Event) error {
		called.Add(1)
		if called.Load() == expected { // each async subscription will process message
			doneCh <- struct{}{}
		}
		return nil
	}

	errHandler := func(err error, sub broker.Subscription) {
		errCh <- err
	}

	subFactory := func(stream string, subj string) subscriber.Subscriptor {
		return subscriber.AsyncSubscription().Subject(subj)
	}

	sub, err := subscriber.Connect(
		stream,
		subscriber.SubscriptionFactory(subFactory),
	)
	if err != nil {
		t.Fatalf("failed to connect subscriber to NATS server: %v", err)
	}
	defer sub.Close()

	// create 2 async subscriptions
	ns1, err := sub.Subscribe(subject, handler, broker.WithErrorHandler(errHandler))
	if err != nil {
		t.Fatalf("failed to subscribe to subject %s: %v", subject, err)
	}
	defer ns1.Unsubscribe()

	ns2, err := sub.Subscribe(subject, handler, broker.WithErrorHandler(errHandler))
	if err != nil {
		t.Fatalf("failed to subscribe to subject %s: %v", subject, err)
	}
	defer ns2.Unsubscribe()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	for i := 0; i < count; i++ {
		err = pub.Publish(subject, &broker.Message{
			ID: fmt.Sprintf("test-id-%d", i+1),
		})
		if err != nil {
			t.Fatalf("failed to publish message to subject %s: %v", subject, err)
		}
	}

	select {
	case <-ctx.Done():
		t.Fatal("processing of message is running for too long")
	case err = <-errCh:
		t.Fatalf("error occurred on processing message: %v", err)
	case <-doneCh:
		if actual := called.Load(); actual != expected {
			t.Fatalf("expected to call message processing %v times but processed %v times", expected, actual)
		}
	}
}

func TestSubscriber_SyncSubscription(t *testing.T) {
	const (
		stream  = "SYNC-SUB"
		subject = "sync.new"
	)

	pub, err := publisher.Connect(
		publisher.InitJetStream(&nats.StreamConfig{
			Name:      stream,
			Subjects:  []string{"sync.>"},
			Retention: nats.InterestPolicy,
		}),
	)
	if err != nil {
		t.Fatalf("failed to connect publisher to NATS server: %v", err)
	}
	defer pub.Close()

	replyCh := make(chan struct{}, 1)
	errCh := make(chan error, 1)

	handler := func(evt broker.Event) error {
		replyCh <- struct{}{}
		return nil
	}

	errHandler := func(err error, sub broker.Subscription) {
		if errors.Is(err, nats.ErrTimeout) {
			return
		}
		errCh <- err
	}

	subFactory := func(stream string, subj string) subscriber.Subscriptor {
		return subscriber.SyncSubscription().Subject(subj)
	}

	sub, err := subscriber.Connect(
		stream,
		subscriber.SubscriptionFactory(subFactory),
	)
	if err != nil {
		t.Fatalf("failed to connect subscriber to NATS server: %v", err)
	}
	defer sub.Close()

	ns, err := sub.Subscribe(subject, handler, broker.WithErrorHandler(errHandler))
	if err != nil {
		t.Fatalf("failed to subscribe to subject %s: %v", subject, err)
	}
	defer ns.Unsubscribe()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err = pub.Publish(subject, &broker.Message{
		ID: "test-id",
	})
	if err != nil {
		t.Fatalf("failed to publish message to subject %s: %v", subject, err)
	}

	itr, err := subscriber.NewSyncIterator(ns)
	if err != nil {
		t.Fatalf("expected sync iterator to be build correctly: %v", err)
	}

	err = itr.Next(time.Second)
	if err != nil {
		t.Fatalf("expected no error on sync processing but it occurred: %v", err)
	}

	select {
	case <-ctx.Done():
		t.Fatal("processing of messages is running for too long")
	case err = <-errCh:
		t.Fatalf("error occurred on processing message: %v", err)
	case <-replyCh:
	}
}

func TestSubscriber_LostConnection(t *testing.T) {
	const (
		stream       = "LOST-CONN"
		subject      = "lost.new"
		durable      = "lost-durable"
		pauseMsgData = "pause"
	)

	pub, err := publisher.Connect(
		publisher.InitJetStream(&nats.StreamConfig{
			Name:      stream,
			Subjects:  []string{"lost.>"},
			Retention: nats.InterestPolicy,
		}),
	)
	if err != nil {
		t.Fatalf("failed to connect publisher to NATS server: %v", err)
	}
	defer pub.Close()

	subFactory := func(stream string, subj string) subscriber.Subscriptor {
		return subscriber.PullSubscription().
			Subject(subj).
			Durable(durable).
			DoubleAck(true).
			SubOptions(
				nats.DeliverLast(),
				nats.AckExplicit(),
				nats.ReplayInstant(),
			)
	}

	replyCh := make(chan struct{}, 1)
	pauseCh := make(chan struct{}, 1)
	errCh := make(chan error, 1)

	sub, err := subscriber.Connect(
		stream,
		subscriber.SubscriptionFactory(subFactory),
	)
	if err != nil {
		t.Fatalf("failed to connect subscriber to NATS server: %v", err)
	}
	defer sub.Close()

	handler := func(evt broker.Event) error {
		// if msg pause is sent stop container
		if string(evt.Message().Body) == pauseMsgData {
			pauseCh <- struct{}{}
			return nil
		}
		replyCh <- struct{}{}
		return nil
	}

	errHandler := func(err error, sub broker.Subscription) {
		errCh <- err
	}

	ns, err := sub.Subscribe(subject, handler, broker.WithErrorHandler(errHandler))
	if err != nil {
		t.Fatalf("failed to create subscription: %v", err)
	}
	defer ns.Unsubscribe()

	// send pause signal
	err = pub.Publish(subject, &broker.Message{ID: "test-id", Body: []byte(pauseMsgData)})
	if err != nil {
		t.Fatalf("failed to publish message to subject %s: %v", subject, err)
	}

	<-pauseCh // wait for pause signal
	if err = srv.pause(); err != nil {
		t.Fatalf("failed to pause server container: %v", err)
	}

	// unpause after 5 seconds
	<-time.After(5 * time.Second)
	if err = srv.unpause(); err != nil {
		t.Fatalf("failed to unpause server container: %v", err)
	}

	// publish normal message
	err = pub.Publish(subject, &broker.Message{ID: "test-id"})
	if err != nil {
		t.Fatalf("failed to publish message to subject %s: %v", subject, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// wait for message to be retrieved and processed
	select {
	case <-ctx.Done():
		t.Fatal("processing of message is running for too long")
	case err = <-errCh:
		t.Fatalf("error occurred on processing message: %v", err)
	case <-replyCh:
	}
}

func TestSubscriber_InitConsumer(t *testing.T) {
	const (
		stream   = "INIT-CONSUMER"
		consumer = "initcons"
		subject1 = "initcons.created"
		subject2 = "initcons.updated"
	)

	// just for stream init
	pub, err := publisher.Connect(
		publisher.InitJetStream(&nats.StreamConfig{
			Name:      stream,
			Subjects:  []string{"initcons.>"},
			Retention: nats.InterestPolicy,
		}),
	)
	if err != nil {
		t.Fatalf("failed to connect publisher to NATS server: %v", err)
	}
	defer pub.Close()

	// create the same consumer for both subscriptions
	consFactory := func(stream string, subj string) *nats.ConsumerConfig {
		return &nats.ConsumerConfig{
			Name:          consumer,
			Durable:       consumer,
			DeliverPolicy: nats.DeliverLastPolicy,
			AckPolicy:     nats.AckExplicitPolicy,
		}
	}

	subFactory := func(stream string, subj string) subscriber.Subscriptor {
		return subscriber.PullSubscription().
			Subject(subj).
			Durable(consumer).
			SubOptions(
				nats.DeliverLast(),
				nats.AckExplicit(),
				nats.ReplayInstant(),
			)
	}

	sub, err := subscriber.Connect(
		stream,
		subscriber.ConsumerFactory(consFactory),
		subscriber.SubscriptionFactory(subFactory),
	)
	if err != nil {
		t.Fatalf("failed to connect subscriber to NATS server: %v", err)
	}
	defer sub.Close()

	handler := func(evt broker.Event) error {
		return nil
	}

	ns1, err := sub.Subscribe(subject1, handler)
	if err != nil {
		t.Fatalf("failed to create subscription: %v", err)
	}
	defer ns1.Unsubscribe()

	ns2, err := sub.Subscribe(subject2, handler)
	if err != nil {
		t.Fatalf("failed to create subscription: %v", err)
	}
	defer ns2.Unsubscribe()

	_, err = srv.consumerInfo(stream, consumer)
	if err != nil {
		t.Fatalf("failed to get consumer info: %v", err)
	}
}