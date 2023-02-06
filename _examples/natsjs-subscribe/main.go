package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/velmie/broker"
	"github.com/velmie/broker/natsjs"
)

func main() {
	var (
		serviceName = "MAIN1"
		streamName  = "PROJECT"
		subject     = "service1.orders.create"
	)

	js, err := natsjs.NewSubscriber(
		streamName,
		serviceName,
		natsjs.DefaultConnectionFactory(),
		natsjs.DefaultJetStreamFactory(),
		natsjs.DefaultSubscriptionFactory(),
		natsjs.DefaultConsumerFactory(nil),
	)

	if err != nil {
		panic(err)
	}

	sub, err := js.Subscribe(
		subject,
		eventHandler,
		broker.WithDefaultErrorHandler(js, &stubLogger{}),
		broker.WithLogger(&stubLogger{}),
	)

	if err != nil {
		panic(err)
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT)
	<-quit

	fmt.Println("Unsubscribing now", sub.Unsubscribe())

	<-sub.Done()
}

func eventHandler(event broker.Event) error {
	fmt.Printf("[%s] received new message\n", event.Topic())
	msg := event.Message()
	fmt.Println("Headers: ")
	for k, v := range msg.Header {
		fmt.Printf("\t%s = %s\n", k, v)
	}
	fmt.Printf("BODY:\n%s\n", string(msg.Body))

	time.Sleep(5 * time.Second)
	fmt.Println("Msg processing finished: ")
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
