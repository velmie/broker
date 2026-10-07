// Command orders demonstrates explicit raw and notification SNS-to-SQS routes.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log/slog"
	"net"
	"net/url"
	"os"
	"os/signal"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	nativeSNS "github.com/aws/aws-sdk-go-v2/service/sns"
	nativeSQS "github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/velmie/broker"
	"github.com/velmie/broker/sns"
	"github.com/velmie/broker/sqs"
)

const (
	operationBudget = 30 * time.Second
	exampleEventID  = "order-event-1"
)

func main() {
	endpoint := flag.String("endpoint", "http://127.0.0.1:5000", "loopback Moto endpoint")
	flag.Parse()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{ReplaceAttr: redactAttribute}))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	if err := run(ctx, *endpoint, logger); err != nil {
		logger.Error("SNS orders example failed", slog.Any("error", err))
		stop()
		os.Exit(1)
	}
	stop()
}

type order struct {
	Amount int `json:"amount"`
}

type forwardedDelivery struct {
	original broker.Delivery
	message  broker.Message
}

func (d *forwardedDelivery) Message() broker.Message { return d.message }
func (d *forwardedDelivery) Source() string          { return d.original.Source() }
func (d *forwardedDelivery) Attempt() broker.Attempt {
	return d.original.(broker.AttemptReader).Attempt()
}
func (d *forwardedDelivery) Metadata() sqs.Metadata {
	return d.original.(sqs.MetadataReader).Metadata()
}

func run(ctx context.Context, endpoint string, logger *slog.Logger) error {
	parsed, err := url.Parse(endpoint)
	if err != nil ||
		parsed.Scheme != "http" ||
		!net.ParseIP(parsed.Hostname()).IsLoopback() ||
		parsed.User != nil ||
		parsed.RawQuery != "" ||
		parsed.Fragment != "" {
		return errors.New("endpoint must be a loopback HTTP URL")
	}
	config := aws.Config{Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("synthetic", "synthetic", "")}
	topics := nativeSNS.NewFromConfig(config, func(o *nativeSNS.Options) { o.BaseEndpoint = aws.String(endpoint) })
	queues := nativeSQS.NewFromConfig(config, func(o *nativeSQS.Options) { o.BaseEndpoint = aws.String(endpoint) })
	for _, raw := range []bool{true, false} {
		if routeErr := runRoute(ctx, topics, queues, raw, logger); routeErr != nil {
			return routeErr
		}
	}
	return nil
}

func runRoute(ctx context.Context, topics *nativeSNS.Client, queues *nativeSQS.Client, raw bool, logger *slog.Logger) (result error) {
	ctx, cancel := context.WithTimeout(ctx, operationBudget)
	defer cancel()
	topic, queue, cleanup, err := createRoute(ctx, topics, queues, raw)
	defer func() { result = errors.Join(result, cleanup()) }()
	if err != nil {
		return err
	}
	publisher, err := sns.NewPublisher(topics, sns.PublisherConfig{TopicARN: topic, Source: "orders/publish", RawSQSDelivery: raw})
	if err != nil {
		return err
	}
	publish := broker.NewTypedPublisher[order](broker.EncoderFunc(json.Marshal),
		publisher,
		func(context.Context,
			order) (broker.MessageMetadata,
			error) {
			return broker.MessageMetadata{ID: exampleEventID, Headers: []broker.Header{{Name: "Correlation-Id", Value: []byte("order-1")}}}, nil
		})
	if publishErr := publish(ctx, order{Amount: 42}); publishErr != nil {
		return publishErr
	}
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	accepted := false
	consumer,
		err := sqs.NewConsumer(queues,
		sqs.ConsumerConfig{QueueURL: queue,
			Source:          "orders/consume",
			WaitTimeSeconds: 1,
			Observer: &sqs.Observer{Record: func(_ context.Context,
				event sqs.Observation) {
				if event.Operation == sqs.OperationDelete && event.Outcome == sqs.OutcomeAccepted {
					accepted = true
					logger.Info("forwarded order deletion accepted", "raw", raw)
					stop()
				}
			}}})
	if err != nil {
		return err
	}
	typed := broker.NewTypedDeliveryHandler(broker.DecodeJSON[order],
		func(_ context.Context,
			delivery broker.Delivery,
			value order) (broker.Disposition,
			error) {
			if value.Amount != 42 || delivery.Message().ID != exampleEventID {
				return nil, errors.New("forwarded order mismatch")
			}
			logger.Info("forwarded order processed", "raw", raw, "amount", value.Amount)
			return broker.Handled{}, nil
		}, broker.WithRequirements(broker.AttemptsRequirement{}))
	handler := typed
	if !raw {
		handler = broker.NewHandler(func(callbackCtx context.Context, delivery broker.Delivery) (broker.Disposition, error) {
			message, decodeErr := sns.DecodeNotification(delivery.Message().Body, topic)
			if decodeErr != nil {
				return nil, &broker.DecodeError{Cause: decodeErr}
			}
			return typed.Handle(callbackCtx, &forwardedDelivery{original: delivery, message: message})
		}, broker.WithRequirements(typed.Requirements()...))
	}
	err = consumer.Run(runCtx, handler)
	if accepted && onlyCancellation(err) {
		return nil
	}
	return err
}

func onlyCancellation(err error) bool {
	if err == nil || err == context.Canceled {
		return true
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		for _, cause := range joined.Unwrap() {
			if !onlyCancellation(cause) {
				return false
			}
		}
		return true
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		cause := wrapped.Unwrap()
		return cause != nil && onlyCancellation(cause)
	}
	return false
}
