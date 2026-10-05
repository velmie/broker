package main

import (
	"context"
	"testing"

	"github.com/nats-io/nats.go"

	"github.com/velmie/broker"
	"github.com/velmie/broker/natsjs/v3"
)

func publishOne(t *testing.T, nc *nats.Conn, config *natsjs.ConsumerConfig) {
	t.Helper()
	publisher, err := natsjs.NewPublisher(nc, natsjs.PublisherConfig{Subject: config.Subject})
	if err != nil {
		t.Fatal(err)
	}
	if err = publisher.Publish(context.Background(), broker.Message{ID: "order-1", Body: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
}
func pauseServer(t *testing.T) {
	t.Helper()
	if output, err := docker("pause", containerID); err != nil {
		t.Fatalf("pause: %v: %s", err, output)
	}
	t.Cleanup(func() {
		if output, err := docker("unpause", containerID); err != nil {
			t.Errorf("unpause: %v: %s", err, output)
		}
	})
}
func isTerminal(operation string) bool {
	return operation == natsjs.OperationAck || operation == natsjs.OperationNak ||
		operation == natsjs.OperationTerm || operation == natsjs.OperationDisposition
}
