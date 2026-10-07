package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	nativeSNS "github.com/aws/aws-sdk-go-v2/service/sns"
	nativeSQS "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/velmie/broker/sns"
)

func createRoute(ctx context.Context,
	topics *nativeSNS.Client,
	queues *nativeSQS.Client,
	raw bool) (topicARN,
	queueURL string,
	cleanup func() error,
	result error) {
	var subscriptionARN string
	cleanup = func() error {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), operationBudget)
		defer cancel()
		var failures []error
		if subscriptionARN != "" {
			_, err := topics.Unsubscribe(cleanupCtx, &nativeSNS.UnsubscribeInput{SubscriptionArn: aws.String(subscriptionARN)})
			failures = append(failures, topologyError("Unsubscribe", err))
		}
		if queueURL != "" {
			_, err := queues.DeleteQueue(cleanupCtx, &nativeSQS.DeleteQueueInput{QueueUrl: aws.String(queueURL)})
			failures = append(failures, topologyError("DeleteQueue", err))
		}
		if topicARN != "" {
			_, err := topics.DeleteTopic(cleanupCtx, &nativeSNS.DeleteTopicInput{TopicArn: aws.String(topicARN)})
			failures = append(failures, topologyError("DeleteTopic", err))
		}
		return errors.Join(failures...)
	}
	name := fmt.Sprintf("broker-orders-%d", time.Now().UnixNano())
	topic, err := topics.CreateTopic(ctx, &nativeSNS.CreateTopicInput{Name: aws.String(name)})
	if err != nil {
		return "", "", cleanup, topologyError("CreateTopic", err)
	}
	topicARN = aws.ToString(topic.TopicArn)
	queue, err := queues.CreateQueue(ctx, &nativeSQS.CreateQueueInput{QueueName: aws.String(name)})
	if err != nil {
		return topicARN, "", cleanup, topologyError("CreateQueue", err)
	}
	queueURL = aws.ToString(queue.QueueUrl)
	attrs,
		err := queues.GetQueueAttributes(ctx,
		&nativeSQS.GetQueueAttributesInput{QueueUrl: queue.QueueUrl,
			AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn}})
	if err != nil {
		return topicARN, queueURL, cleanup, topologyError("GetQueueAttributes", err)
	}
	policy,
		err := json.Marshal(map[string]any{"Version": "2012-10-17",
		"Statement": []any{map[string]any{"Effect": "Allow",
			"Principal": map[string]string{"Service": "sns.amazonaws.com"},
			"Action":    "sqs:SendMessage",
			"Resource":  attrs.Attributes["QueueArn"],
			"Condition": map[string]any{"ArnEquals": map[string]string{"aws:SourceArn": topicARN}}}}})
	if err != nil {
		return topicARN, queueURL, cleanup, topologyError("EncodeQueuePolicy", err)
	}
	_,
		err = queues.SetQueueAttributes(ctx,
		&nativeSQS.SetQueueAttributesInput{QueueUrl: queue.QueueUrl,
			Attributes: map[string]string{"Policy": string(policy)}})
	if err != nil {
		return topicARN, queueURL, cleanup, topologyError("SetQueueAttributes", err)
	}
	sub,
		err := topics.Subscribe(ctx,
		&nativeSNS.SubscribeInput{TopicArn: topic.TopicArn,
			Protocol:              aws.String("sqs"),
			Endpoint:              aws.String(attrs.Attributes["QueueArn"]),
			ReturnSubscriptionArn: true,
			Attributes:            map[string]string{"RawMessageDelivery": fmt.Sprint(raw)}})
	if err != nil {
		return topicARN, queueURL, cleanup, topologyError("Subscribe", err)
	}
	subscriptionARN = aws.ToString(sub.SubscriptionArn)
	return topicARN, queueURL, cleanup, nil
}
func topologyError(operation string, err error) error {
	if err == nil {
		return nil
	}
	return &sns.OperationError{Source: "orders/topology", Operation: operation, Cause: err}
}
