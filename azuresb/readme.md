# azuresb

The package provides a simple, idiomatic API for both publishing and subscribing to messages via Azure Service Bus, 
abstracting away the complexity of the underlying Azure SDK.


## Usage

### Publisher example

Publish messages to an Azure Service Bus queue or topic using a sender factory and publisher.

```go
package main

import (
	"log"

	"github.com/velmie/broker"
	"github.com/velmie/broker/azuresb"
)

func main() {
	// Initialize the sender factory with your Azure Service Bus connection string
	senderFactory := azuresb.NewDefaultPublisherFactory(
		azuresb.WithConnectionString("your-connection-string"),
	)

	// Create a new publisher
	publisher := azuresb.NewPublisher(senderFactory)

	// Create a broker message
	msg := broker.NewMessage()
	msg.Body = []byte("Hello, Azure Service Bus!")
	msg.ID = "msg-001"
	msg.Header.Set("Content-Type", "text/plain")

	// Publish the message to a queue or topic
	if err := publisher.Publish("my-queue", msg); err != nil {
		log.Fatalf("publish message error: %v", err)
	}
	log.Println("message published successfully!")
}

```

### Subscriber Example

Subscribe to messages from an Azure Service Bus queue (or topic subscription) with a custom handler.

```go
package main

import (
	"log"

	"github.com/velmie/broker"
	"github.com/velmie/broker/azuresb"
)

func main() {
	// Create a receiver factory with your Azure Service Bus connection string.
	receiverFactory := azuresb.NewDefaultReceiverFactory(
		azuresb.WithConnectionString("your-connection-string"),
		// if you're using topics with subscriptions
		// azuresb.WithSubscriptionName("your-subscription-name"),
		// azuresb.WithReceiverType(azuresb.ReceiverTypeSubscription),
	)

	// Create a new subscriber
	subscriber := azuresb.NewSubscriber(receiverFactory)

	// Define the message handler.
	handler := func(e broker.Event) error {
		msg := e.Message()
		log.Printf("Received message with ID %s: %s", msg.ID, string(msg.Body))
		// You can optionally call e.Ack() here if AutoAck is not enabled.
		return nil
	}

	// Subscribe to a queue or topic
	sub, err := subscriber.Subscribe("my-queue", handler, func(o *broker.SubscribeOptions) {
		o.AutoAck = true // Automatically acknowledge messages after successful handling
	})
	if err != nil {
		log.Fatalf("Failed to subscribe: %v", err)
	}

	// Wait until the subscription is terminated (e.g., via an external signal).
	<-sub.Done()
	log.Println("Subscription closed.")
}
```