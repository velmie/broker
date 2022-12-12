package natsjs_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"

	natsserver "github.com/nats-io/nats-server/v2/test"

	"github.com/velmie/broker"
	"github.com/velmie/broker/natsjs"
)

func TestPublisherParams(t *testing.T) {

	_, err := natsjs.NewPublisher(
		"PROJECT",
		"",
		natsjs.DefaultConnectionFactory(),
		natsjs.DefaultJetStreamFactory(),
	)

	if errors.Cause(err) != natsjs.ErrSubjectPrefix {
		t.Fatalf("Did not get the proper error, got %v", err)
	}

	_, err = natsjs.NewPublisher("",
		"SERVICE1",
		natsjs.DefaultConnectionFactory(),
		natsjs.DefaultJetStreamFactory(),
	)

	if errors.Cause(err) != natsjs.ErrStreamName {
		t.Fatalf("Did not get the proper error, got %v", err)
	}
}

func TestPublisherSendMessagePos(t *testing.T) {
	var (
		stream     = "project"
		subjPrefix = "service"
		subject    = "test.message"
	)

	srv := runBasicServer()
	defer shutdownServer(t, srv)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	connected := make(chan struct{})
	jsp, err := natsjs.NewPublisher(
		stream,
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

	if err = waitTime(connected, 30*time.Second); err != nil {
		t.Fatalf("Did not connect, got %v", err)
	}

	err = jsp.Publish(subject, &broker.Message{
		ID:     fmt.Sprintf("test-id-%d", time.Now().Unix()),
		Header: map[string]string{"something": "rational"},
		Body:   []byte(`{"greeting":"Test"}`),
	})

	if err != nil {
		t.Fatalf("Got error %v", err)
	}
}

func TestPublisherReconnect(t *testing.T) {
	var (
		stream     = "project"
		subjPrefix = "service"
		subject    = "test.message"
	)

	srv := runBasicServer()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reconnected := make(chan struct{})
	jsp, err := natsjs.NewPublisher(
		stream,
		subjPrefix,
		natsjs.DefaultConnectionFactory(),
		natsjs.DefaultJetStreamFactory(nats.Context(ctx), nats.MaxWait(5*time.Second)),
		natsjs.PublisherConnURL(srv.ClientURL()),
		natsjs.PublisherReconnectCb(func(nc *nats.Conn) {
			reconnected <- struct{}{}
		}),
	)
	if err != nil {
		t.Fatalf("Something went wrong, got %v", err)
	}

	msg := broker.Message{
		ID:     fmt.Sprintf("test-id-%d", time.Now().Unix()),
		Header: map[string]string{"something": "rational"},
		Body:   []byte(`{"greeting":"Test"}`),
	}

	err = jsp.Publish(subject, &msg)
	if err != nil {
		t.Fatalf("Got error %v", err)
	}

	srv = restartBasicServer(t, srv)
	defer shutdownServer(t, srv)
	defer jsp.Close()

	if err = waitTime(reconnected, 30*time.Second); err != nil {
		t.Fatalf("Did not reconnect, got %v", err)
	}

	err = jsp.Publish(subject, &msg)
	if err != nil {
		t.Fatalf("Got error %v", err)
	}
}

func TestPublisherConnectAfterServerStart(t *testing.T) {
	var (
		stream     = "project"
		subjPrefix = "service"
		subject    = "test.message"
		port       = 40639
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	defOpts := natsserver.DefaultTestOptions
	connURL := fmt.Sprintf("nats://%s:%d", defOpts.Host, port)

	connected := make(chan struct{})
	jsp, err := natsjs.NewPublisher(
		stream,
		subjPrefix,
		natsjs.DefaultConnectionFactory(
			nats.ReconnectWait(1*time.Second),
			nats.ReconnectJitter(0, 0),
		),
		natsjs.DefaultJetStreamFactory(nats.Context(ctx)),
		natsjs.PublisherConnURL(connURL),
		natsjs.PublisherReconnectCb(func(nc *nats.Conn) {
			connected <- struct{}{}
		}),
	)
	if err != nil {
		t.Fatalf("Something went wrong, got %v", err)
	}

	msg := broker.Message{
		ID:     fmt.Sprintf("test-id-%d", time.Now().Unix()),
		Header: map[string]string{"something": "rational"},
		Body:   []byte(`{"greeting":"Test"}`),
	}

	err = jsp.Publish(subject, &msg)
	if err == nil || !strings.Contains(err.Error(), "cannot send message") {
		t.Fatalf("Did not get the proper error, got %v", err)
	}

	srv := runServerOnPort(port)
	defer shutdownServer(t, srv)
	defer jsp.Close()

	if err = waitTime(connected, 30*time.Second); err != nil {
		t.Fatalf("Did not connect, got %v", err)
	}

	err = jsp.Publish(subject, &msg)
	if err != nil {
		t.Fatalf("Got error %v", err)
	}
}
