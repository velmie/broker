package main

import (
	"fmt"
	"time"

	"github.com/velmie/broker"
	"github.com/velmie/broker/natsjs"
)

func main() {
	var (
		streamName    = "PROJECT"
		subjectPrefix = "SERVICE1"
		subject       = "orders.create"
	)

	jsPublisher, err := natsjs.NewPublisher(
		streamName,
		subjectPrefix,
		natsjs.DefaultConnectionFactory(),
		natsjs.DefaultJetStreamFactory(),
	)
	if err != nil {
		panic(err)
	}

	msgID := fmt.Sprintf("my-unique-id-%d", time.Now().Unix())
	for {
		err = jsPublisher.Publish(subject, &broker.Message{
			ID:     msgID,
			Header: map[string]string{"something": "rational"},
			Body:   []byte(`{"greeting":"Hello there!"}`),
		})
		if err != nil {
			fmt.Println("failed to publish new message: ", err)
			time.Sleep(5 * time.Second)
			fmt.Println("Wait for reconnect ")
			continue
		}
		break
	}
	fmt.Printf("message has been successfully sent ID: %s\n", msgID)
}
