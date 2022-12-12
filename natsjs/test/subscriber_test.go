package natsjs_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"

	natsserver "github.com/nats-io/nats-server/v2/test"

	"github.com/velmie/broker"
	"github.com/velmie/broker/natsjs"
)

func TestSubscriberParams(t *testing.T) {
	var (
		streamName  = "PROJECT"
		serviceName = "test-receiver"
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, err := natsjs.NewSubscriber(
		streamName,
		"",
		natsjs.DefaultConnectionFactory(),
		natsjs.DefaultJetStreamFactory(nats.Context(ctx)),
		natsjs.DefaultSubscriptionFactory(),
		natsjs.DefaultConsumerFactory(nil),
		natsjs.MaxSubAttempts(10),
	)

	if errors.Cause(err) != natsjs.ErrServiceName {
		t.Fatalf("Did not get the proper error, got %v", err)
	}

	_, err = natsjs.NewSubscriber(
		"",
		serviceName,
		natsjs.DefaultConnectionFactory(),
		natsjs.DefaultJetStreamFactory(nats.Context(ctx)),
		natsjs.DefaultSubscriptionFactory(),
		natsjs.DefaultConsumerFactory(nil),
		natsjs.MaxSubAttempts(10),
	)

	if errors.Cause(err) != natsjs.ErrStreamName {
		t.Fatalf("Did not get the proper error, got %v", err)
	}
}

func TestSubscriberInvalidSubject(t *testing.T) {
	var (
		streamName  = "PROJECT"
		serviceName = "test-receiver"
		subject     = "invalid_subject"
	)

	srv := runBasicServer()
	defer shutdownServer(t, srv)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	js, err := natsjs.NewSubscriber(
		streamName,
		serviceName,
		natsjs.DefaultConnectionFactory(),
		natsjs.DefaultJetStreamFactory(nats.Context(ctx)),
		natsjs.DefaultSubscriptionFactory(),
		natsjs.DefaultConsumerFactory(nil),
		natsjs.SubscriberConnURL(srv.ClientURL()),
	)
	if err != nil {
		t.Fatalf("Something went wrong, got %v", err)
	}
	defer js.Close()

	_, err = js.Subscribe(subject, func(event broker.Event) error { return nil })
	if errors.Cause(err) != natsjs.ErrSubjectInvalid {
		t.Fatalf("Did not get the proper error, got %v", err)
	}
}

func TestSubscriberUnsubscribed(t *testing.T) {
	var (
		streamName  = "PROJECT"
		subjPrefix  = "sender"
		serviceName = "test-receiver"
		subject     = "sender.test.message"
	)

	srv := runBasicServer()
	defer shutdownServer(t, srv)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	connected := make(chan struct{})
	jsp, err := natsjs.NewPublisher(
		streamName,
		subjPrefix,
		natsjs.DefaultConnectionFactory(),
		natsjs.DefaultJetStreamFactory(nats.Context(ctx)),
		natsjs.PublisherConnURL(srv.ClientURL()),
		natsjs.PublisherConnectCb(func(nc *nats.Conn) {
			connected <- struct{}{}
		}),
	)
	if err != nil {
		t.Fatalf("Something went wrong, got %v", err)
	}
	defer jsp.Close()

	js, err := natsjs.NewSubscriber(
		streamName,
		serviceName,
		natsjs.DefaultConnectionFactory(),
		natsjs.DefaultJetStreamFactory(nats.Context(ctx)),
		natsjs.DefaultSubscriptionFactory(),
		natsjs.DefaultConsumerFactory(nil),
		natsjs.SubscriberConnURL(srv.ClientURL()),
	)
	if err != nil {
		t.Fatalf("Something went wrong, got %v", err)
	}
	defer js.Close()

	select {
	case <-connected:
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not connect")
	}

	sub, err := js.Subscribe(subject, func(event broker.Event) error { return nil })
	if err != nil {
		t.Fatalf("Got error %v", err)
	}

	for i := 0; i < 3; i++ {
		err = sub.Unsubscribe()
		if err != nil && strings.Contains(err.Error(), "invalid subscription") {
			t.Logf("Got error %v", err)
			time.Sleep(1 * time.Second)
			continue
		}
		break
	}

	if err != nil {
		t.Fatalf("Got error %v", err)
	}
	<-sub.Done()
}

func TestSubscriberAlreadySubscribed(t *testing.T) {
	var (
		streamName  = "PROJECT"
		serviceName = "test-receiver"
		subject     = "sender.subject.action"
	)

	srv := runBasicServer()
	defer shutdownServer(t, srv)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	js, err := natsjs.NewSubscriber(
		streamName,
		serviceName,
		natsjs.DefaultConnectionFactory(),
		natsjs.DefaultJetStreamFactory(nats.Context(ctx)),
		natsjs.DefaultSubscriptionFactory(),
		natsjs.DefaultConsumerFactory(nil),
		natsjs.SubscriberConnURL(srv.ClientURL()),
	)
	if err != nil {
		t.Fatalf("Something went wrong, got %v", err)
	}
	defer js.Close()

	sub, err := js.Subscribe(subject, func(event broker.Event) error { return nil })
	if err != nil {
		t.Fatalf("Got error %v", err)
	}

	_, err = js.Subscribe(subject, func(event broker.Event) error { return nil })
	if errors.Cause(err) != broker.AlreadySubscribed {
		t.Fatalf("Did not get the proper error, got %v", err)
	}
	_ = sub.Unsubscribe()
	<-sub.Done()
}

func TestSubscriberGetMessage(t *testing.T) {
	var (
		streamName  = "PROJECT"
		subjPrefix  = "service"
		serviceName = "test-receiver"
		subject     = "service.test.message"
	)

	srv := runBasicServer()
	defer shutdownServer(t, srv)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	connected := make(chan bool)
	jsp, err := natsjs.NewPublisher(
		streamName,
		subjPrefix,
		natsjs.DefaultConnectionFactory(),
		natsjs.DefaultJetStreamFactory(nats.Context(ctx)),
		natsjs.PublisherConnURL(srv.ClientURL()),
		natsjs.PublisherConnectCb(func(nc *nats.Conn) {
			connected <- true
		}),
	)
	if err != nil {
		t.Fatalf("Something went wrong, got %v", err)
	}
	defer jsp.Close()

	subscribed := make(chan bool)
	js, err := natsjs.NewSubscriber(
		streamName,
		serviceName,
		natsjs.DefaultConnectionFactory(),
		natsjs.DefaultJetStreamFactory(nats.Context(ctx)),
		natsjs.DefaultSubscriptionFactory(),
		natsjs.DefaultConsumerFactory(nil),
		natsjs.SubscriberConnURL(srv.ClientURL()),
		natsjs.DelayBetweenAttempts(1*time.Second),
		natsjs.SubscribedCb(func(subj string) {
			subscribed <- true
		}),
	)
	if err != nil {
		t.Fatalf("Something went wrong, got %v", err)
	}
	defer js.Close()

	if err = waitTime(connected, 30*time.Second); err != nil {
		t.Fatalf("Did not connect, got %v", err)
	}

	done := make(chan bool)
	sub, err := js.Subscribe(
		subject,
		func(event broker.Event) error {
			_ = event.Ack()
			done <- true
			return nil
		},
		broker.WithLogger(&stubLogger{}),
		broker.DisableAutoAck(),
	)

	if err != nil {
		t.Fatalf("Got error %v", err)
	}

	if err = waitTime(subscribed, 5*time.Second); err != nil {
		t.Fatalf("Did not subscribe, got %v", err)
	}

	err = jsp.Publish(subject, &broker.Message{
		ID:     fmt.Sprintf("test-id-%d", time.Now().Unix()),
		Header: map[string]string{"something": "rational"},
		Body:   []byte(`{"greeting":"Test"}`),
	})

	if err != nil {
		t.Fatalf("Got error %v", err)
	}

	if err = waitTime(done, 5*time.Second); err != nil {
		t.Fatalf("Did not receive the message in time, got %v", err)
	}

	_ = sub.Unsubscribe()
	<-sub.Done()
}

func TestSubscriberGetInvalidMessage(t *testing.T) {
	var (
		streamName  = "PROJECT"
		subjPrefix  = "service"
		serviceName = "test-receiver"
		subject     = "service.test.message"
	)

	srv := runBasicServer()
	defer shutdownServer(t, srv)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	connected := make(chan bool)
	jsp, err := natsjs.NewPublisher(
		streamName,
		subjPrefix,
		natsjs.DefaultConnectionFactory(),
		natsjs.DefaultJetStreamFactory(nats.Context(ctx)),
		natsjs.PublisherConnURL(srv.ClientURL()),
		natsjs.PublisherConnectCb(func(nc *nats.Conn) {
			connected <- true
		}),
	)
	if err != nil {
		t.Fatalf("Something went wrong, got %v", err)
	}
	defer jsp.Close()

	subscribed := make(chan bool)
	js, err := natsjs.NewSubscriber(
		streamName,
		serviceName,
		natsjs.DefaultConnectionFactory(),
		natsjs.DefaultJetStreamFactory(nats.Context(ctx)),
		natsjs.DefaultSubscriptionFactory(),
		natsjs.DefaultConsumerFactory(&nats.ConsumerConfig{
			AckWait:       2 * time.Second,
			DeliverPolicy: nats.DeliverLastPolicy,
			AckPolicy:     nats.AckExplicitPolicy,
			ReplayPolicy:  nats.ReplayInstantPolicy,
		}),
		natsjs.SubscriberConnURL(srv.ClientURL()),
		natsjs.DelayBetweenAttempts(1*time.Second),
		natsjs.SubscribedCb(func(subj string) {
			subscribed <- true
		}),
	)
	if err != nil {
		t.Fatalf("Something went wrong, got %v", err)
	}
	defer js.Close()

	if err = waitTime(connected, 30*time.Second); err != nil {
		t.Fatalf("Did not connect, got %v", err)
	}

	var delivered uint32 = 0
	done := make(chan bool, 1)
	sub, err := js.Subscribe(
		subject,
		func(event broker.Event) error {
			if atomic.LoadUint32(&delivered) == 0 {
				atomic.AddUint32(&delivered, 1)
				return errors.New("Invalid message")
			}
			_ = event.Ack()
			done <- true
			return nil
		},
		broker.WithLogger(&stubLogger{}),
		broker.WithErrorHandler(func(err error, sub broker.Subscription) {}),
		broker.DisableAutoAck(),
	)

	if err != nil {
		t.Fatalf("Got error %v", err)
	}

	if err = waitTime(subscribed, 5*time.Second); err != nil {
		t.Fatalf("Did not subscribe, got %v", err)
	}

	err = jsp.Publish(subject, &broker.Message{
		ID:     fmt.Sprintf("test-id-%d", time.Now().Unix()),
		Header: map[string]string{"something": "rational"},
		Body:   []byte(`{"greeting":"Test"}`),
	})

	if err != nil {
		t.Fatalf("Got error %v", err)
	}

	if err = waitTime(done, 5*time.Second); err != nil {
		t.Fatalf("Did not receive the message in time, got %v", err)
	}

	_ = sub.Unsubscribe()
	<-sub.Done()
}

func TestSubscriberGetMessageInCluster(t *testing.T) {
	var (
		streamName  = "PROJECT"
		subjPrefix  = "sender"
		serviceName = "test-receiver"
		subject     = "sender.test.message"
	)

	srv := runBasicServer()
	defer shutdownServer(t, srv)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	connected := make(chan bool)
	jsp, err := natsjs.NewPublisher(
		streamName,
		subjPrefix,
		natsjs.DefaultConnectionFactory(),
		natsjs.DefaultJetStreamFactory(nats.Context(ctx)),
		natsjs.PublisherConnURL(srv.ClientURL()),
		natsjs.PublisherConnectCb(func(nc *nats.Conn) {
			connected <- true
		}),
	)
	if err != nil {
		t.Fatalf("Something went wrong, got %v", err)
	}
	defer jsp.Close()

	if err = waitTime(connected, 30*time.Second); err != nil {
		t.Fatalf("Did not connect, got %v", err)
	}

	size := 3
	clusterjs := make([]*natsjs.Subscriber, size)
	subs := make([]broker.Subscription, size)
	errCh := make(chan error, size)

	var delivered uint32
	var wg sync.WaitGroup
	for i := 0; i < size; i++ {
		wg.Add(1)
		js, inErr := natsjs.NewSubscriber(
			streamName,
			serviceName,
			natsjs.DefaultConnectionFactory(),
			natsjs.DefaultJetStreamFactory(nats.Context(ctx)),
			natsjs.DefaultSubscriptionFactory(),
			natsjs.DefaultConsumerFactory(nil),
			natsjs.SubscriberConnURL(srv.ClientURL()),
			natsjs.DelayBetweenAttempts(1*time.Second),
			natsjs.SubscribedCb(func(subj string) {
				wg.Done()
			}),
		)

		if inErr != nil {
			errCh <- inErr
			continue
		}

		clusterjs[i] = js

		sub, inErr := js.Subscribe(
			subject,
			func(event broker.Event) error {
				atomic.AddUint32(&delivered, 1)
				return nil
			},
			broker.WithLogger(&stubLogger{}),
		)
		if inErr != nil {
			errCh <- inErr
			continue
		}
		subs[i] = sub
	}

	wgCh := make(chan struct{})
	go func() {
		wg.Wait()
		wgCh <- struct{}{}
	}()

	if err = waitTime(wgCh, 5*time.Second); err != nil {
		t.Fatalf("Did not subscribe, got %v", err)
	}

	err = jsp.Publish(subject, &broker.Message{
		ID:     fmt.Sprintf("test-id-%d", time.Now().Unix()),
		Header: map[string]string{"something": "rational"},
		Body:   []byte(`{"greeting":"Test"}`),
	})

	if err != nil {
		t.Fatalf("Got error %v", err)
	}

	tick := time.Tick(1 * time.Second)
	for {
		actual := atomic.LoadUint32(&delivered)
		select {
		case err = <-errCh:
			t.Fatalf("Got error %v", err)
		case <-tick:
			if actual != 1 {
				continue
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("Expected %v, got: %v", 1, actual)
		}
		break
	}

	for _, sub := range subs {
		_ = sub.Unsubscribe()
		<-sub.Done()
	}

	for _, js := range clusterjs {
		js.Close()
	}
}

func TestSubscriberGetMessageByMany(t *testing.T) {
	var (
		streamName   = "PROJECT"
		subjPrefix   = "sender"
		serviceNames = []string{"test-receiver1", "test-receiver2"}
		subject      = "sender.test.message"
	)

	srv := runBasicServer()
	defer shutdownServer(t, srv)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	connected := make(chan bool)
	jsp, err := natsjs.NewPublisher(
		streamName,
		subjPrefix,
		natsjs.DefaultConnectionFactory(),
		natsjs.DefaultJetStreamFactory(nats.Context(ctx)),
		natsjs.PublisherConnURL(srv.ClientURL()),
		natsjs.PublisherConnectCb(func(nc *nats.Conn) {
			connected <- true
		}),
	)
	if err != nil {
		t.Fatalf("Something went wrong, got %v", err)
	}
	defer jsp.Close()

	if err = waitTime(connected, 30*time.Second); err != nil {
		t.Fatalf("Did not connect, got %v", err)
	}

	size := uint32(len(serviceNames))
	errCh := make(chan error, size)
	services := make([]*natsjs.Subscriber, size)
	subs := make([]broker.Subscription, size)

	var wgSub sync.WaitGroup
	var wgMsg sync.WaitGroup
	for i, serviceName := range serviceNames {
		wgSub.Add(1)
		js, inErr := natsjs.NewSubscriber(
			streamName,
			serviceName,
			natsjs.DefaultConnectionFactory(),
			natsjs.DefaultJetStreamFactory(nats.Context(ctx)),
			natsjs.DefaultSubscriptionFactory(),
			natsjs.DefaultConsumerFactory(nil),
			natsjs.SubscriberConnURL(srv.ClientURL()),
			natsjs.DelayBetweenAttempts(1*time.Second),
			natsjs.SubscribedCb(func(subj string) {
				wgSub.Done()
			}),
		)

		if inErr != nil {
			errCh <- inErr
			continue
		}

		wgMsg.Add(1)
		sub, inErr := js.Subscribe(
			subject,
			func(event broker.Event) error {
				_ = event.Ack()
				wgMsg.Done()
				return nil
			},
			broker.WithLogger(&stubLogger{}),
			broker.DisableAutoAck(),
		)
		if inErr != nil {
			errCh <- inErr
			continue
		}
		services[i] = js
		subs[i] = sub
	}

	wgSubCh := make(chan bool)
	go func() {
		wgSub.Wait()
		wgSubCh <- true
	}()

	if err = waitTime(wgSubCh, 5*time.Second); err != nil {
		t.Fatalf("Did not subscribe, got %v", err)
	}

	err = jsp.Publish(subject, &broker.Message{
		ID:     fmt.Sprintf("test-id-%d", time.Now().Unix()),
		Header: map[string]string{"something": "rational"},
		Body:   []byte(`{"greeting":"Test"}`),
	})

	if err != nil {
		t.Fatalf("Got error %v", err)
	}

	wgMsgCh := make(chan bool)
	go func() {
		wgMsg.Wait()
		wgMsgCh <- true
	}()

	select {
	case <-wgMsgCh:
	case err = <-errCh:
		t.Fatalf("Got error %v", err)
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive the messages in time")
	}

	for _, sub := range subs {
		_ = sub.Unsubscribe()
		<-sub.Done()
	}

	for _, js := range services {
		js.Close()
	}
}

func TestSubscriberGetMessageAfterReconnect(t *testing.T) {
	var (
		streamName  = "PROJECT"
		subjPrefix  = "service"
		serviceName = "test-receiver"
		subject     = "service.test.message"
	)

	srv := runBasicServer()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	connected := make(chan struct{})
	reconnected := make(chan struct{})
	jsp, err := natsjs.NewPublisher(
		streamName,
		subjPrefix,
		natsjs.DefaultConnectionFactory(),
		natsjs.DefaultJetStreamFactory(nats.Context(ctx)),
		natsjs.PublisherConnURL(srv.ClientURL()),
		natsjs.PublisherConnectCb(func(nc *nats.Conn) {
			connected <- struct{}{}
		}),
		natsjs.PublisherReconnectCb(func(nc *nats.Conn) {
			reconnected <- struct{}{}
		}),
	)
	if err != nil {
		t.Fatalf("Something went wrong, got %v", err)
	}

	if err = waitTime(connected, 30*time.Second); err != nil {
		t.Fatalf("Did not connect, got %v", err)
	}

	subscribed := make(chan bool, 1)
	js, err := natsjs.NewSubscriber(
		streamName,
		serviceName,
		natsjs.DefaultConnectionFactory(
			nats.ReconnectWait(100*time.Millisecond),
			nats.ReconnectJitter(0, 0),
		),
		natsjs.DefaultJetStreamFactory(nats.Context(ctx)),
		natsjs.DefaultSubscriptionFactory(),
		natsjs.DefaultConsumerFactory(nil),
		natsjs.SubscriberConnURL(srv.ClientURL()),
		natsjs.DelayBetweenAttempts(1*time.Second),
		natsjs.SubscribedCb(func(subj string) {
			subscribed <- true
		}),
	)
	if err != nil {
		t.Fatalf("Something went wrong, got %v", err)
	}

	done := make(chan bool)
	sub, err := js.Subscribe(
		subject,
		func(event broker.Event) error {
			_ = event.Ack()
			done <- true
			return nil
		},
		broker.WithLogger(&stubLogger{}),
		broker.DisableAutoAck(),
	)

	if err != nil {
		t.Fatalf("Got error %v", err)
	}

	if err = waitTime(subscribed, 5*time.Second); err != nil {
		t.Fatalf("Did not subscribe, got %v", err)
	}

	err = jsp.Publish(subject, &broker.Message{
		ID:     fmt.Sprintf("test-id-%d", time.Now().Unix()),
		Header: map[string]string{"something": "rational"},
		Body:   []byte(`{"greeting":"Test"}`),
	})

	if err != nil {
		t.Fatalf("Got error %v", err)
	}

	if err = waitTime(done, 5*time.Second); err != nil {
		t.Fatalf("Did not receive the message in time, got %v", err)
	}

	srv = restartBasicServer(t, srv)
	defer shutdownServer(t, srv)
	defer jsp.Close()
	defer js.Close()

	if err = waitTime(subscribed, 5*time.Second); err != nil {
		t.Fatalf("Did not subscribe, got %v", err)
	}

	err = jsp.Publish(subject, &broker.Message{
		ID:     fmt.Sprintf("test-id-%d", time.Now().Unix()),
		Header: map[string]string{"something": "rational"},
		Body:   []byte(`{"greeting":"Test"}`),
	})

	if err = waitTime(done, 5*time.Second); err != nil {
		t.Fatalf("Did not receive the message in time, got %v", err)
	}

	_ = sub.Unsubscribe()
	<-sub.Done()
}

func TestSubscriberGetMessageAfterServerStart(t *testing.T) {
	var (
		streamName  = "PROJECT"
		subjPrefix  = "service"
		serviceName = "test-receiver"
		subject     = "service.test.message"
		port        = 40369
	)

	connURL := fmt.Sprintf("nats://%s:%d", natsserver.DefaultTestOptions.Host, port)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	jsp, err := natsjs.NewPublisher(
		streamName,
		subjPrefix,
		natsjs.DefaultConnectionFactory(
			nats.ReconnectWait(100*time.Millisecond),
			nats.ReconnectJitter(0, 0),
		),
		natsjs.DefaultJetStreamFactory(nats.Context(ctx)),
		natsjs.PublisherConnURL(connURL),
	)
	if err != nil {
		t.Fatalf("Something went wrong, got %v", err)
	}
	defer jsp.Close()

	subscribed := make(chan bool)
	js, err := natsjs.NewSubscriber(
		streamName,
		serviceName,
		natsjs.DefaultConnectionFactory(
			nats.ReconnectWait(100*time.Millisecond),
			nats.ReconnectJitter(0, 0),
		),
		natsjs.DefaultJetStreamFactory(nats.Context(ctx)),
		natsjs.DefaultSubscriptionFactory(),
		natsjs.DefaultConsumerFactory(nil),
		natsjs.SubscriberConnURL(connURL),
		natsjs.DelayBetweenAttempts(1*time.Second),
		natsjs.SubscribedCb(func(_s string) {
			subscribed <- true
		}),
	)
	if err != nil {
		t.Fatalf("Something went wrong, got %v", err)
	}

	done := make(chan bool)
	sub, err := js.Subscribe(
		subject,
		func(event broker.Event) error {
			_ = event.Ack()
			done <- true
			return nil
		},
		broker.WithLogger(&stubLogger{}),
		broker.DisableAutoAck(),
	)

	if err != nil {
		t.Fatalf("Cannot subscribe, got %v", err)
	}

	srv := runServerOnPort(port)
	defer shutdownServer(t, srv)
	defer js.Close()

	if err = waitTime(subscribed, 30*time.Second); err != nil {
		t.Fatalf("Did not subscribe, got %v", err)
	}

	err = jsp.Publish(subject, &broker.Message{
		ID:     fmt.Sprintf("test-id-%d", time.Now().Unix()),
		Header: map[string]string{"something": "rational"},
		Body:   []byte(`{"greeting":"Test"}`),
	})

	if err != nil {
		t.Fatalf("Got error %v", err)
	}

	if err = waitTime(done, 5*time.Second); err != nil {
		t.Fatalf("Did not receive the message in time, got %v", err)
	}

	_ = sub.Unsubscribe()
	<-sub.Done()
}
