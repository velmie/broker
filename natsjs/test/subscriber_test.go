package natsjs_test

import (
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"

	"github.com/velmie/broker"
	"github.com/velmie/broker/natsjs"
)

func TestSubscriberParams(t *testing.T) {
	var (
		streamName  = "PROJECT"
		serviceName = "test-receiver"
	)

	_, err := natsjs.NewSubscriber(
		streamName,
		"",
		natsjs.DefaultConnectionFactory(),
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

	js, err := natsjs.NewSubscriber(
		streamName,
		serviceName,
		natsjs.DefaultConnectionFactory(),
		natsjs.DefaultSubscriptionFactory(),
		natsjs.DefaultConsumerFactory(nil),
		natsjs.SubcsriberConnURL(srv.ClientURL()),
	)

	if err != nil {
		t.Fatalf("Something went wrong, got %v", err)
	}

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

	var jsp *natsjs.Publisher
	var err error
	go func(jsp *natsjs.Publisher, err error) {
		jsp, err = natsjs.NewPublisher(
			streamName,
			subjPrefix,
			natsjs.DefaultConnectionFactory(),
			natsjs.PublisherConnURL(srv.ClientURL()),
		)
	}(jsp, err)

	if err != nil {
		t.Fatalf("Something went wrong, got %v", err)
	}

	js, err := natsjs.NewSubscriber(
		streamName,
		serviceName,
		natsjs.DefaultConnectionFactory(),
		natsjs.DefaultSubscriptionFactory(),
		natsjs.DefaultConsumerFactory(nil),
		natsjs.SubcsriberConnURL(srv.ClientURL()),
	)

	if err != nil {
		t.Fatalf("Something went wrong, got %v", err)
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

	js, err := natsjs.NewSubscriber(
		streamName,
		serviceName,
		natsjs.DefaultConnectionFactory(),
		natsjs.DefaultSubscriptionFactory(),
		natsjs.DefaultConsumerFactory(nil),
		natsjs.SubcsriberConnURL(srv.ClientURL()),
	)

	if err != nil {
		t.Fatalf("Something went wrong, got %v", err)
	}

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

func TestSubscriberGetMessageGeneralCase(t *testing.T) {
	var (
		streamName  = "PROJECT"
		subjPrefix  = "service"
		serviceName = "test-receiver"
		subject     = "service.test.message"
	)

	srv := runBasicServer()
	defer shutdownServer(t, srv)

	var jsp *natsjs.Publisher
	var err error
	go func() {
		jsp, err = natsjs.NewPublisher(
			streamName,
			subjPrefix,
			natsjs.DefaultConnectionFactory(),
			natsjs.PublisherConnURL(srv.ClientURL()),
		)
	}()

	if err != nil {
		t.Fatalf("Something went wrong, got %v", err)
	}

	time.Sleep(1 * time.Second)

	js, err := natsjs.NewSubscriber(
		streamName,
		serviceName,
		natsjs.DefaultConnectionFactory(),
		natsjs.DefaultSubscriptionFactory(),
		natsjs.DefaultConsumerFactory(nil),
		natsjs.SubcsriberConnURL(srv.ClientURL()),
	)

	if err != nil {
		t.Fatalf("Something went wrong, got %v", err)
	}

	done := make(chan interface{})
	sub, err := js.Subscribe(
		subject,
		func(event broker.Event) error {
			done <- struct{}{}
			return nil
		},
		broker.WithDefaultErrorHandler(js, &stubLogger{}),
		broker.WithLogger(&stubLogger{}),
	)

	if err != nil {
		t.Fatalf("Got error %v", err)
	}

	err = jsp.Publish(subject, &broker.Message{
		ID:     fmt.Sprintf("test-id-%d", time.Now().Unix()),
		Header: map[string]string{"something": "rational"},
		Body:   []byte(`{"greeting":"Test"}`),
	})

	if err != nil {
		t.Fatalf("Got error %v", err)
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive the message in time")
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

	var jsp *natsjs.Publisher
	var err error
	go func() {
		jsp, err = natsjs.NewPublisher(
			streamName,
			subjPrefix,
			natsjs.DefaultConnectionFactory(),
			natsjs.PublisherConnURL(srv.ClientURL()),
		)
	}()

	if err != nil {
		t.Fatalf("Something went wrong, got %v", err)
	}

	time.Sleep(1 * time.Second)

	size := 3
	clusterjs := make([]*natsjs.Subscriber, size)
	subs := make([]broker.Subscription, size)
	errCh := make(chan error, size)

	var delivered uint32
	for i := 0; i < size; i++ {
		js, inErr := natsjs.NewSubscriber(
			streamName,
			serviceName,
			natsjs.DefaultConnectionFactory(),
			natsjs.DefaultSubscriptionFactory(),
			natsjs.DefaultConsumerFactory(nil),
			natsjs.SubcsriberConnURL(srv.ClientURL()),
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
			broker.WithDefaultErrorHandler(js, &stubLogger{}),
			broker.WithLogger(&stubLogger{}),
		)
		if inErr != nil {
			errCh <- inErr
			continue
		}
		subs[i] = sub
	}

	err = jsp.Publish(subject, &broker.Message{
		ID:     fmt.Sprintf("test-id-%d", time.Now().Unix()),
		Header: map[string]string{"something": "rational"},
		Body:   []byte(`{"greeting":"Test"}`),
	})

	if err != nil {
		t.Fatalf("Got error %v", err)
	}

	select {
	case err = <-errCh:
		t.Fatalf("Got error %v", err)
	case <-time.After(3 * time.Second):
		actual := atomic.LoadUint32(&delivered)
		if actual != 1 {
			t.Fatalf("Expected %v, got: %v", 1, actual)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive the messages in time")
	}

	for _, sub := range subs {
		_ = sub.Unsubscribe()
		<-sub.Done()
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

	var jsp *natsjs.Publisher
	var err error
	go func() {
		jsp, err = natsjs.NewPublisher(
			streamName,
			subjPrefix,
			natsjs.DefaultConnectionFactory(),
			natsjs.PublisherConnURL(srv.ClientURL()),
		)
	}()

	if err != nil {
		t.Fatalf("Something went wrong, got %v", err)
	}

	time.Sleep(1 * time.Second)

	size := uint32(len(serviceNames))
	errCh := make(chan error, size)
	services := make([]*natsjs.Subscriber, size)
	subs := make([]broker.Subscription, size)

	var delivered uint32
	for i, serviceName := range serviceNames {
		js, inErr := natsjs.NewSubscriber(
			streamName,
			serviceName,
			natsjs.DefaultConnectionFactory(),
			natsjs.DefaultSubscriptionFactory(),
			natsjs.DefaultConsumerFactory(nil),
			natsjs.SubcsriberConnURL(srv.ClientURL()),
		)

		if inErr != nil {
			errCh <- inErr
			continue
		}

		sub, inErr := js.Subscribe(
			subject,
			func(event broker.Event) error {
				atomic.AddUint32(&delivered, 1)
				return nil
			},
			broker.WithDefaultErrorHandler(js, &stubLogger{}),
			broker.WithLogger(&stubLogger{}),
		)
		if inErr != nil {
			errCh <- inErr
			continue
		}
		services[i] = js
		subs[i] = sub
	}

	err = jsp.Publish(subject, &broker.Message{
		ID:     fmt.Sprintf("test-id-%d", time.Now().Unix()),
		Header: map[string]string{"something": "rational"},
		Body:   []byte(`{"greeting":"Test"}`),
	})

	if err != nil {
		t.Fatalf("Got error %v", err)
	}

	select {
	case err = <-errCh:
		t.Fatalf("Got error %v", err)
	case <-time.After(3 * time.Second):
		actual := atomic.LoadUint32(&delivered)
		if actual < size {
			t.Fatalf("Expected %d, got: %d", size, actual)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive the messages in time")
	}

	for _, sub := range subs {
		_ = sub.Unsubscribe()
		<-sub.Done()
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

	var jsp *natsjs.Publisher
	var err error
	go func() {
		jsp, err = natsjs.NewPublisher(
			streamName,
			subjPrefix,
			natsjs.DefaultConnectionFactory(),
			natsjs.PublisherConnURL(srv.ClientURL()),
		)
	}()

	if err != nil {
		t.Fatalf("Something went wrong, got %v", err)
	}

	time.Sleep(1 * time.Second)

	js, err := natsjs.NewSubscriber(
		streamName,
		serviceName,
		natsjs.DefaultConnectionFactory(
			nats.ReconnectWait(100*time.Millisecond),
			nats.ReconnectJitter(0, 0),
		),
		natsjs.DefaultSubscriptionFactory(),
		natsjs.DefaultConsumerFactory(nil),
		natsjs.SubcsriberConnURL(srv.ClientURL()),
	)

	if err != nil {
		t.Fatalf("Something went wrong, got %v", err)
	}

	done := make(chan interface{})
	sub, err := js.Subscribe(
		subject,
		func(event broker.Event) error {
			done <- struct{}{}
			return nil
		},
		broker.WithDefaultErrorHandler(js, &stubLogger{}),
		broker.WithLogger(&stubLogger{}),
	)

	if err != nil {
		t.Fatalf("Got error %v", err)
	}

	err = jsp.Publish(subject, &broker.Message{
		ID:     fmt.Sprintf("test-id-%d", time.Now().Unix()),
		Header: map[string]string{"something": "rational"},
		Body:   []byte(`{"greeting":"Test"}`),
	})

	if err != nil {
		t.Fatalf("Got error %v", err)
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive the message in time")
	}

	srv = restartBasicServer(t, srv)
	defer shutdownServer(t, srv)

	time.Sleep(1 * time.Second)

	err = jsp.Publish(subject, &broker.Message{
		ID:     fmt.Sprintf("test-id-%d", time.Now().Unix()),
		Header: map[string]string{"something": "rational"},
		Body:   []byte(`{"greeting":"Test"}`),
	})

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive the message in time")
	}

	_ = sub.Unsubscribe()
	<-sub.Done()
}
