package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws"
	nativeSNS "github.com/aws/aws-sdk-go-v2/service/sns"
	nativeSQS "github.com/aws/aws-sdk-go-v2/service/sqs"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/velmie/broker"
	"github.com/velmie/broker/otelbroker"
	"github.com/velmie/broker/sns"
	"github.com/velmie/broker/sqs"
)

func runRoute(ctx context.Context, queues *nativeSQS.Client, topics *nativeSNS.Client, kind string,
	tracer trace.Tracer, logger *slog.Logger) (result error) {
	ctx, cancel := context.WithTimeout(ctx, routeBudget)
	defer cancel()
	topology, err := createRoute(ctx, queues, topics, kind)
	defer func() { result = errors.Join(result, topology.close(queues, topics)) }()
	if err != nil {
		return err
	}
	publisher, err := routePublisher(queues, topics, topology, kind, tracer)
	if err != nil {
		return err
	}
	system := "aws_sqs"
	if kind != directRoute {
		system = "aws_sns"
	}
	publisher = broker.WrapPublisher(publisher,
		otelbroker.TracePublishing(system, sourceName, otelbroker.WithTracer(tracer), otelbroker.WithPropagator(propagation.TraceContext{})),
		broker.StampInstanceID("command-client"), broker.ValidateIDHeader("Command-Id"))
	publish := broker.NewTypedPublisher[order](broker.EncoderFunc(json.Marshal), publisher,
		func(context.Context, order) (broker.MessageMetadata, error) {
			return broker.MessageMetadata{ID: exampleID, Headers: []broker.Header{{Name: "Command-Id", Value: []byte(exampleID)}}}, nil
		})
	if publishErr := publish(ctx, order{Amount: exampleAmount}); publishErr != nil {
		return publishErr
	}
	logger.InfoContext(ctx, "publication accepted", "route", kind, "source", sourceName)
	expectedTopic := ""
	if kind == envelopeRoute {
		expectedTopic = topology.topic
	}
	effects := 0
	err = consume(ctx, queues, topology.queue, expectedTopic, logger, tracer, func(context.Context, broker.Delivery, order) error {
		effects++
		return nil
	})
	if err != nil {
		return err
	}
	if effects != 1 {
		return errors.New("expected one business effect")
	}
	readback, err := queues.ReceiveMessage(ctx, &nativeSQS.ReceiveMessageInput{QueueUrl: aws.String(topology.queue), WaitTimeSeconds: 1})
	if err != nil {
		return wrapCommand("empty_readback", err)
	}
	if len(readback.Messages) != 0 {
		return wrapCommand("empty_readback", errors.New("queue was not empty after accepted deletion"))
	}
	logger.InfoContext(ctx, "route completed with empty readback", "route", kind)
	return nil
}

func routePublisher(queues *nativeSQS.Client, topics *nativeSNS.Client,
	route *topology, kind string, tracer trace.Tracer) (broker.Publisher, error) {
	if kind != directRoute {
		return sns.NewPublisher(topics, sns.PublisherConfig{TopicARN: route.topic, Source: sourceName, RawSQSDelivery: kind == rawRoute,
			OperationTimeout: operationBudget, Observer: snsObserver(tracer)})
	}
	return sqs.NewPublisher(queues, sqs.PublisherConfig{QueueURL: route.queue, Source: sourceName, OperationTimeout: operationBudget,
		Observer: &sqs.Observer{Record: func(ctx context.Context, event sqs.Observation) {
			recordNative(ctx, tracer, "aws_sqs", event.Operation, event.Outcome, event.Duration, event.Err)
		}}})
}
