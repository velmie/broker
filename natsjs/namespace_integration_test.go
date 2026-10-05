package natsjs_test

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/velmie/broker"
	"github.com/velmie/broker/natsjs/v3"
)

func TestConsumerSelectedJetStreamNamespace(t *testing.T) {
	nc, wire := namespaceConnection(t)
	js, err := nc.JetStream(nats.Domain("broker"), nats.MaxWait(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	for _, profile := range []struct {
		name, domain, prefix string
	}{
		{name: "domain", domain: "broker"},
		{name: "prefix", prefix: "$JS.broker.API"},
		{name: "prefix-dot", prefix: "$JS.broker.API."},
	} {
		t.Run(profile.name, func(t *testing.T) {
			config := namespaceSource(t, js)
			config.Domain, config.APIPrefix = profile.domain, profile.prefix
			before, err := js.ConsumerInfo(config.Stream, config.Consumer)
			if err != nil {
				t.Fatal(err)
			}
			publisher, err := natsjs.NewPublisher(nc, natsjs.PublisherConfig{Subject: config.Subject})
			if err != nil {
				t.Fatal(err)
			}
			if err = publisher.Publish(context.Background(), broker.Message{ID: "namespace-event", Body: []byte(`{"value":42}`)}); err != nil {
				t.Fatal(err)
			}
			ctx, stop := context.WithTimeout(context.Background(), testTimeout)
			defer stop()
			var steps []string
			config.Observer.Record = func(_ context.Context, event natsjs.Observation) {
				if event.Operation == natsjs.OperationAck && event.Outcome == natsjs.OutcomeConfirmed {
					steps = append(steps, "ack")
					stop()
				}
			}
			type command struct{ Value int }
			handler := broker.NewTypedHandler(broker.DecodeJSON[command], func(_ context.Context, value command) error {
				if value.Value != 42 {
					return errors.New("unexpected decoded command")
				}
				steps = append(steps, "effect")
				return nil
			})
			consumer, err := natsjs.NewConsumer(nc, config)
			if err != nil {
				t.Fatal(err)
			}
			wire.reset()
			if err = consumer.Run(ctx, handler); !namespaceCancellationOnly(err) {
				t.Fatalf("joined namespace consumer: %v", err)
			}
			if strings.Join(steps, ",") != "effect,ack" {
				t.Fatalf("effect/ACK order: %v", steps)
			}
			protocol := wire.snapshot()
			for _, operation := range []string{"CONSUMER.INFO.", "CONSUMER.MSG.NEXT."} {
				if !strings.Contains(protocol, "PUB $JS.broker.API."+operation+config.Stream+"."+config.Consumer+" ") {
					t.Errorf("selected namespace missing from SDK request for %s", operation)
				}
			}
			after, err := js.ConsumerInfo(config.Stream, config.Consumer)
			if err != nil || after.NumAckPending != 0 || after.AckFloor.Stream != 1 || !reflect.DeepEqual(before.Config, after.Config) {
				t.Fatalf("namespace readback changed policy or lost completion: %+v, %v", after, err)
			}
			if nc.IsClosed() {
				t.Fatal("borrowed connection closed")
			}
		})
	}
}

func TestConsumerWrongNamespacePreservesNativeCause(t *testing.T) {
	nc, _ := namespaceConnection(t)
	js, err := nc.JetStream(nats.Domain("broker"), nats.MaxWait(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	config := namespaceSource(t, js)
	config.Domain = "missing"
	consumer, err := natsjs.NewConsumer(nc, config)
	if err != nil {
		t.Fatal(err)
	}
	called := false
	err = consumer.Run(context.Background(), broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
		called = true
		return broker.Handled{}, nil
	}))
	var operation *natsjs.OperationError
	if called || !errors.Is(err, nats.ErrJetStreamNotEnabled) || !errors.As(err, &operation) ||
		operation.Operation != "startup" || operation.Source != config.Stream+"/"+config.Consumer {
		t.Fatalf("wrong namespace cause or boundary: called=%v error=%v", called, err)
	}
}

func namespaceConnection(t *testing.T) (*nats.Conn, *namespaceDialer) {
	t.Helper()
	config := filepath.Join(t.TempDir(), "nats.conf")
	if err := os.WriteFile(config, []byte(`port: 4222
jetstream { domain: broker, store_dir: /tmp/jetstream }
`), 0o600); err != nil {
		t.Fatal(err)
	}
	image := os.Getenv("NATS_TEST_IMAGE")
	if image == "" {
		image = "nats:2.15.0"
	}
	output, err := docker("run", "--rm", "-d", "-p", "127.0.0.1::4222", "--volume", config+":/etc/nats/profile.conf:ro",
		image, "--config", "/etc/nats/profile.conf")
	if err != nil {
		t.Fatalf("start namespace server: %v: %s", err, output)
	}
	container := strings.TrimSpace(string(output))
	t.Cleanup(func() {
		if cleanupOutput, cleanupErr := docker("rm", "-f", container); cleanupErr != nil {
			t.Errorf("remove namespace server: %v: %s", cleanupErr, cleanupOutput)
		}
	})
	output, err = docker("port", container, "4222/tcp")
	if err != nil {
		t.Fatalf("namespace server endpoint: %v: %s", err, output)
	}
	url := "nats://" + strings.TrimSpace(string(output))
	deadline := time.Now().Add(testTimeout)
	wire := &namespaceDialer{}
	for {
		nc, connectErr := nats.Connect(url, nats.Timeout(time.Second), nats.FlusherTimeout(time.Second), nats.NoReconnect(),
			nats.SetCustomDialer(wire))
		if connectErr == nil {
			t.Cleanup(nc.Close)
			return nc, wire
		}
		if time.Now().After(deadline) {
			t.Fatalf("namespace server readiness: %v", connectErr)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func namespaceSource(t *testing.T, js nats.JetStreamContext) natsjs.ConsumerConfig {
	t.Helper()
	stream := strings.ReplaceAll(t.Name(), "/", "_")
	subject := stream + ".orders"
	if _, err := js.AddStream(&nats.StreamConfig{Name: stream, Subjects: []string{subject}, Storage: nats.MemoryStorage}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := js.DeleteStream(stream); err != nil {
			t.Errorf("delete namespace stream: %v", err)
		}
	})
	if _, err := js.AddConsumer(stream, &nats.ConsumerConfig{Durable: "worker", FilterSubject: subject,
		AckPolicy: nats.AckExplicitPolicy, Description: "namespace binding", MaxAckPending: 3}); err != nil {
		t.Fatal(err)
	}
	return natsjs.ConsumerConfig{Stream: stream, Consumer: "worker", Subject: subject, FetchTimeout: 100 * time.Millisecond}
}

func namespaceCancellationOnly(err error) bool {
	if err == context.Canceled {
		return true
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		causes := joined.Unwrap()
		for _, cause := range causes {
			if !namespaceCancellationOnly(cause) {
				return false
			}
		}
		return len(causes) > 0
	}
	if cause := errors.Unwrap(err); cause != nil {
		return namespaceCancellationOnly(cause)
	}
	return false
}

// namespaceDialer records the actual outbound SDK protocol without rewriting it.
// Server-side domain mappings otherwise hide the original API request subject.
type namespaceDialer struct {
	mu       sync.Mutex
	outbound []byte
}

func (d *namespaceDialer) Dial(network, address string) (net.Conn, error) {
	conn, err := net.DialTimeout(network, address, time.Second)
	if err != nil {
		return nil, err
	}
	return &namespaceConn{Conn: conn, dialer: d}, nil
}
func (d *namespaceDialer) reset() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.outbound = nil
}
func (d *namespaceDialer) snapshot() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return string(d.outbound)
}

type namespaceConn struct {
	net.Conn
	dialer *namespaceDialer
}

func (c *namespaceConn) Write(p []byte) (int, error) {
	c.dialer.mu.Lock()
	c.dialer.outbound = append(c.dialer.outbound, p...)
	c.dialer.mu.Unlock()
	return c.Conn.Write(p)
}
