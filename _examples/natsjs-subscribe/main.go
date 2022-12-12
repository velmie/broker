package main

import (
	"fmt"
	"time"

	"github.com/velmie/broker"
	"github.com/velmie/broker/natsjs"
)

func main() {
	var (
		serviceName    = "MAIN1"
		streamName     = "PROJECT"
		subjectPrefix1 = "service1"
	)

	jsSubscriber, err := natsjs.NewSubscriber(
		streamName,
		serviceName,
		natsjs.DefaultConnectionFactory(),
		natsjs.DefaultSubscriptionFactory(),
		natsjs.DefaultConsumerFactory(nil),
	)

	sub1, err := jsSubscriber.Subscribe(
		subjectPrefix1+".orders.create",
		eventHandler,
		broker.WithDefaultErrorHandler(jsSubscriber, &stubLogger{}),
		broker.WithLogger(&stubLogger{}),
	)

	if err != nil {
		panic(err)
	}

	go func() {
		time.Sleep(time.Minute * 1)
		fmt.Println("Unsubscribing now 1", sub1.Unsubscribe())
	}()

	<-sub1.Done()
}

func eventHandler(event broker.Event) error {
	fmt.Printf("[%s] received new message\n", event.Topic())
	msg := event.Message()
	fmt.Println("Headers: ")
	for k, v := range msg.Header {
		fmt.Printf("\t%s = %s\n", k, v)
	}
	fmt.Printf("BODY:\n%s\n", string(msg.Body))

	return nil
}

type stubLogger struct {
}

func (s *stubLogger) Debug(v ...interface{}) {
	fmt.Println("DEBUG:")
	fmt.Println(v...)
}

func (s *stubLogger) Error(v ...interface{}) {
	fmt.Println("ERROR:")
	fmt.Println(v...)
}

func (s *stubLogger) Info(v ...interface{}) {
	fmt.Println("INFO:")
	fmt.Println(v...)
}
