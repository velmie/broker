package natsjs_test

import (
	"context"
	"fmt"
	"time"

	"github.com/velmie/broker/natsjs/v4"
)

func ExampleEnsureLimitsStream() {
	// Invalid profiles fail before the JetStream client is used.
	err := natsjs.EnsureLimitsStream(context.Background(), nil, natsjs.LimitsStreamConfig{})
	fmt.Println(err != nil)
	// Output: true
}

func ExampleEnsureWorkQueueStream() {
	err := natsjs.EnsureWorkQueueStream(context.Background(), nil, natsjs.WorkQueueStreamConfig{})
	fmt.Println(err != nil)
	// Output: true
}

func ExampleRequireRetainedStream() {
	err := natsjs.RequireRetainedStream(context.Background(), nil, natsjs.RetainedStreamConfig{
		Stream: "EVENTS",
	})
	fmt.Println(err != nil)
	// Output: true
}

func ExampleRetainedStreamConfig() {
	config := natsjs.RetainedStreamConfig{
		Stream:          "EVENTS",
		MaxMessageBytes: 1024 * 1024,
	}
	fmt.Println(config.Validate() == nil)
	// Output: true
}

func ExampleEnsureDurableConsumer() {
	err := natsjs.EnsureDurableConsumer(context.Background(), nil, natsjs.DurableConsumerConfig{})
	fmt.Println(err != nil)
	// Output: true
}

func ExampleCheckStreams() {
	// With no stream names, there is nothing to check.
	fmt.Println(natsjs.CheckStreams(context.Background(), nil) == nil)
	// Output: true
}

func ExampleDurableConsumerConfig() {
	config := natsjs.DurableConsumerConfig{
		Stream:        "EVENTS",
		Durable:       "worker",
		Subjects:      []string{"events.created", "events.paid"},
		AckWait:       time.Second,
		MaxDeliveries: -1,
		MaxAckPending: 10,
	}
	fmt.Println(config.Validate() == nil)
	// Output: true
}

func ExampleConsumerConfig() {
	config := natsjs.ConsumerConfig{
		Stream:   "EVENTS",
		Consumer: "worker",
		Subjects: []string{"events.created", "events.paid"},
	}
	fmt.Println(len(config.Subjects))
	// Output: 2
}
