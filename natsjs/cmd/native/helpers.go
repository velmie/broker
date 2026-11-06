package main

import (
	"context"
	"errors"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"

	"github.com/velmie/broker/natsjs/v3"
)

const operationTimeout = 2 * time.Second

func newStream(ctx context.Context, js nats.JetStreamContext) (stream string, cleanup func() error, err error) {
	stream = "BROKER_NATIVE_" + nuid.Next()
	cleanup = func() error { return deleteStream(js, stream) }
	callCtx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()
	config := &nats.StreamConfig{Name: stream, Subjects: []string{stream + ".>"}, Storage: nats.MemoryStorage}
	_, err = js.AddStream(config, nats.Context(callCtx))
	if err != nil {
		err = errors.Join(operationError(stream, "create_stream", err), cleanup())
	}
	return stream, cleanup, err
}

func deleteStream(js nats.JetStreamContext, stream string) error {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), operationTimeout)
	defer cancel()
	err := js.DeleteStream(stream, nats.Context(cleanupCtx))
	if errors.Is(err, nats.ErrStreamNotFound) {
		return nil
	}
	return operationError(stream, "delete_stream", err)
}

func operationError(source, operation string, err error) error {
	if err == nil {
		return nil
	}
	return &natsjs.OperationError{Source: source, Operation: operation, Cause: err}
}
