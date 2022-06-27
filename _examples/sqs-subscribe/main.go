package main

import (
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"

	"github.com/velmie/broker"
	sqsBroker "github.com/velmie/broker/sqs"
)

func main() {
	httpClient := http.Client{
		Timeout: 10 * time.Second,
	}

	awsSession, err := session.NewSession(&aws.Config{HTTPClient: &httpClient})
	if err != nil {
		fmt.Println("cannot create AWS session: ", err)
		os.Exit(1)
	}

	sqsService := sqs.New(awsSession)

	sqsSubscriber := sqsBroker.NewSubscriber(
		sqsService,
		// implementation specific options could be applied to the subscriber
		sqsBroker.LongPollingDuration(10),   // default is 20 (which is maximum)
		sqsBroker.RequestMultipleMessage(5), // receive 5 messages per request
	)

	subscription, err := sqsSubscriber.Subscribe(
		"test-sqs-1.fifo",
		eventHandler,
		broker.WithDefaultErrorHandler(sqsSubscriber, &stubLogger{}),
	)

	go func() {
		time.Sleep(time.Minute)
		fmt.Println("Unsubscribing now", subscription.Unsubscribe())
	}()

	<-subscription.Done()
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
