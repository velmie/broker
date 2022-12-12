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
	)

	jsPublisher, err := natsjs.NewPublisher(streamName, subjectPrefix, natsjs.DefaultConnectionFactory())
	if err != nil {
		panic(err)
	}

	for {
		err = jsPublisher.Publish("orders.create", &broker.Message{
			ID:     fmt.Sprintf("my-unique-id-%d", time.Now().Unix()),
			Header: map[string]string{"something": "rational"},
			Body:   []byte(`{"greeting":"Hello there!"}`),
		})
		if err != nil {
			fmt.Println("failed to publish new message: ", err)
			time.Sleep(10 * time.Second)
			fmt.Println("Wait for reconnect ")
			continue
		}
		break
	}
	fmt.Println("message has been successfully sent")

	time.Sleep(2 * time.Minute)
}
