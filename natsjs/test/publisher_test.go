package natsjs_test

import (
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
	_, err := natsjs.NewPublisher("PROJECT", "", natsjs.DefaultConnectionFactory())

	if errors.Cause(err) != natsjs.ErrSubjectPrefix {
		t.Fatalf("Did not get the proper error, got %v", err)
	}

	_, err = natsjs.NewPublisher("", "SERVICE1", natsjs.DefaultConnectionFactory())

	if errors.Cause(err) != natsjs.ErrStreamName {
		t.Fatalf("Did not get the proper error, got %v", err)
	}
}

func TestPublisherSendMessageNeg(t *testing.T) {
	var (
		stream     = "project"
		subjPrefix = "service"
		subject    = "test.message"
	)

	jsp, err := natsjs.NewPublisher(stream, subjPrefix, natsjs.DefaultConnectionFactory())
	if err != nil {
		t.Fatalf("Something went wrong, got %v", err)
	}

	err = jsp.Publish(subject, &broker.Message{
		ID:     fmt.Sprintf("test-id-%d", time.Now().Unix()),
		Header: map[string]string{"something": "rational"},
		Body:   []byte(`{"greeting":"Test"}`),
	})

	if err == nil || !strings.Contains(err.Error(), "cannot send message") {
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

	jsp, err := natsjs.NewPublisher(
		stream,
		subjPrefix,
		natsjs.DefaultConnectionFactory(),
		natsjs.PublisherConnURL(srv.ClientURL()),
	)
	if err != nil {
		t.Fatalf("Something went wrong, got %v", err)
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

	jsp, err := natsjs.NewPublisher(
		stream,
		subjPrefix,
		natsjs.DefaultConnectionFactory(),
		natsjs.PublisherConnURL(srv.ClientURL()),
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

	defOpts := natsserver.DefaultTestOptions
	connURL := fmt.Sprintf("nats://%s:%d", defOpts.Host, port)

	jsp, err := natsjs.NewPublisher(
		stream,
		subjPrefix,
		natsjs.DefaultConnectionFactory(
			nats.ReconnectWait(1*time.Second),
			nats.ReconnectJitter(0, 0),
		),
		natsjs.PublisherConnURL(connURL),
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

	attempts := 1
	for {
		time.Sleep(1 * time.Second)

		err = jsp.Publish(subject, &msg)
		if err != nil {
			if attempts > 3 {
				t.Fatalf("Got error %v", err)
			}
			t.Errorf("Got error %v, attempt %d", err, attempts)
			attempts += 1
			continue
		}
		break
	}
}
