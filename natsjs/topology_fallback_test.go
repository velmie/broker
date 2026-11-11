package natsjs_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/velmie/broker/natsjs/v3"
)

type failingTopology struct {
	jetstream.JetStream
	createErr     error
	lookupErr     error
	streamCalls   int
	consumerCalls int
	stream        jetstream.Stream
}

func (f *failingTopology) Stream(context.Context, string) (jetstream.Stream, error) {
	if f.stream != nil {
		return f.stream, nil
	}
	f.streamCalls++
	if f.streamCalls == 1 {
		return nil, jetstream.ErrStreamNotFound
	}
	return nil, f.lookupErr
}

type staticStream struct {
	jetstream.Stream
	info jetstream.StreamInfo
}

func (s *staticStream) CachedInfo() *jetstream.StreamInfo { return &s.info }

func (f *failingTopology) CreateStream(context.Context, jetstream.StreamConfig) (jetstream.Stream, error) {
	return nil, f.createErr
}

func (f *failingTopology) Consumer(context.Context, string, string) (jetstream.Consumer, error) {
	f.consumerCalls++
	if f.consumerCalls == 1 {
		return nil, jetstream.ErrConsumerNotFound
	}
	return nil, f.lookupErr
}

func (f *failingTopology) CreateConsumer(context.Context, string, jetstream.ConsumerConfig) (jetstream.Consumer, error) {
	return nil, f.createErr
}

func TestTopologyCreateFailuresKeepBothCauses(t *testing.T) {
	createErr := errors.New("create failed")
	lookupErr := errors.New("fallback lookup failed")
	js := &failingTopology{
		createErr: createErr,
		lookupErr: lookupErr,
	}
	cfg := natsjs.LimitsStreamConfig{
		Name:            "ORDERS",
		Subjects:        []string{"orders.>"},
		DuplicateWindow: time.Minute,
	}
	err := natsjs.EnsureLimitsStream(context.Background(), js, cfg)
	var op *natsjs.OperationError
	if !errors.Is(err, createErr) || !errors.Is(err, lookupErr) || !errors.As(err, &op) || op.Operation != "stream_create" {
		t.Fatalf("expected stream create and both causes: %v", err)
	}
	js.stream = &staticStream{
		info: jetstream.StreamInfo{
			Config: jetstream.StreamConfig{
				Name:     "ORDERS",
				Subjects: []string{"orders.>"},
			},
		},
	}
	consumer := natsjs.DurableConsumerConfig{
		Stream:        "ORDERS",
		Durable:       "worker",
		Subject:       "orders.created",
		AckWait:       time.Second,
		MaxDeliveries: 1,
		MaxAckPending: 1,
	}
	err = natsjs.EnsureDurableConsumer(context.Background(), js, consumer)
	if !errors.Is(err, createErr) || !errors.Is(err, lookupErr) || !errors.As(err, &op) || op.Operation != "consumer_create" {
		t.Fatalf("expected consumer create and both causes: %v", err)
	}
}
