package sqs_test

import (
	"context"
	"testing"
	"time"

	"github.com/velmie/broker"
	adapter "github.com/velmie/broker/sqs"
)

func TestConfigurationValidation(t *testing.T) {
	client, url := newWire(t, func(context.Context, string, request) (int, any) {
		t.Error("constructor performed network work")
		return 200, request{}
	})
	for _, change := range []func(*adapter.ConsumerConfig){
		func(c *adapter.ConsumerConfig) { c.BatchSize = -1 }, func(c *adapter.ConsumerConfig) { c.BatchSize = 11 },
		func(c *adapter.ConsumerConfig) { c.WaitTimeSeconds = -1 }, func(c *adapter.ConsumerConfig) { c.WaitTimeSeconds = 21 },
		func(c *adapter.ConsumerConfig) { c.ReceiveTimeout = 20 * time.Second },
		func(c *adapter.ConsumerConfig) { c.ReceiveTimeout = -time.Second },
		func(c *adapter.ConsumerConfig) { c.VisibilityTimeout = -time.Second },
		func(c *adapter.ConsumerConfig) { c.VisibilityTimeout = time.Millisecond },
		func(c *adapter.ConsumerConfig) { c.VisibilityTimeout = 13 * time.Hour },
		func(c *adapter.ConsumerConfig) { c.OperationTimeout = -time.Second },
		func(c *adapter.ConsumerConfig) { c.Shutdown.Mode = 99 }, func(c *adapter.ConsumerConfig) { c.Shutdown.GracePeriod = -time.Second },
		func(c *adapter.ConsumerConfig) {
			c.Shutdown = broker.ShutdownPolicy{Mode: broker.ShutdownCancel, GracePeriod: time.Second}
		},
		func(c *adapter.ConsumerConfig) { c.QueueURL = "https://name:secret@example.com/queue" },
		func(c *adapter.ConsumerConfig) { c.Source = "secret\nforged" },
	} {
		config := adapter.ConsumerConfig{QueueURL: url, Source: "orders"}
		change(&config)
		if _, err := adapter.NewConsumer(client, config); err == nil {
			t.Errorf("accepted invalid config %+v", config)
		}
	}
	for _, config := range []adapter.PublisherConfig{
		{QueueURL: url, Source: "orders", FIFO: true}, {QueueURL: url, Source: "orders", MessageGroupID: "has space"},
		{QueueURL: url, Source: "orders", OperationTimeout: -time.Second}, {QueueURL: url, Source: ""},
	} {
		if _, err := adapter.NewPublisher(client, config); err == nil {
			t.Error("invalid publisher accepted")
		}
	}
	for _, wait := range []int{0, 1, 20} {
		if _, err := adapter.NewConsumer(client, adapter.ConsumerConfig{QueueURL: url, Source: "orders", WaitTimeSeconds: wait}); err != nil {
			t.Fatal(err)
		}
	}
}
