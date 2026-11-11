package natsjs_test

import (
	"context"
	"fmt"

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
	err := natsjs.RequireRetainedStream(context.Background(), nil, natsjs.RetainedStreamConfig{})
	fmt.Println(err != nil)
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
