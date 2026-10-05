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
)

type topology struct{ queue, topic, subscription string }

func createRoute(ctx context.Context, queues *nativeSQS.Client, topics *nativeSNS.Client, kind string) (*topology, error) {
	route := &topology{}
	name := fmt.Sprintf("broker-traced-orders-%d", time.Now().UnixNano())
	queue, err := queues.CreateQueue(ctx, &nativeSQS.CreateQueueInput{QueueName: aws.String(name)})
	if err != nil {
		return route, wrapCommand("CreateQueue", err)
	}
	route.queue = aws.ToString(queue.QueueUrl)
	if kind == directRoute {
		return route, nil
	}
	topic, err := topics.CreateTopic(ctx, &nativeSNS.CreateTopicInput{Name: aws.String(name)})
	if err != nil {
		return route, wrapCommand("CreateTopic", err)
	}
	route.topic = aws.ToString(topic.TopicArn)
	attrs, err := queues.GetQueueAttributes(ctx, &nativeSQS.GetQueueAttributesInput{QueueUrl: queue.QueueUrl,
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn}})
	if err != nil {
		return route, wrapCommand("GetQueueAttributes", err)
	}
	policy, err := json.Marshal(map[string]any{"Version": "2012-10-17", "Statement": []any{map[string]any{
		"Effect": "Allow", "Principal": map[string]string{"Service": "sns.amazonaws.com"}, "Action": "sqs:SendMessage",
		"Resource": attrs.Attributes["QueueArn"], "Condition": map[string]any{"ArnEquals": map[string]string{"aws:SourceArn": route.topic}}}}})
	if err != nil {
		return route, err
	}
	_, err = queues.SetQueueAttributes(ctx, &nativeSQS.SetQueueAttributesInput{QueueUrl: queue.QueueUrl,
		Attributes: map[string]string{"Policy": string(policy)}})
	if err != nil {
		return route, wrapCommand("SetQueueAttributes", err)
	}
	subscription, err := topics.Subscribe(ctx, &nativeSNS.SubscribeInput{TopicArn: topic.TopicArn, Protocol: aws.String("sqs"),
		Endpoint: aws.String(attrs.Attributes["QueueArn"]), ReturnSubscriptionArn: true,
		Attributes: map[string]string{"RawMessageDelivery": fmt.Sprint(kind == rawRoute)}})
	if err != nil {
		return route, wrapCommand("Subscribe", err)
	}
	route.subscription = aws.ToString(subscription.SubscriptionArn)
	return route, nil
}

func (r *topology) close(queues *nativeSQS.Client, topics *nativeSNS.Client) error {
	var failures []error
	if r.subscription != "" {
		ctx, cancel := context.WithTimeout(context.Background(), operationBudget)
		_, err := topics.Unsubscribe(ctx, &nativeSNS.UnsubscribeInput{SubscriptionArn: aws.String(r.subscription)})
		cancel()
		failures = append(failures, wrapCommand("Unsubscribe", err))
	}
	if r.queue != "" {
		ctx, cancel := context.WithTimeout(context.Background(), operationBudget)
		_, err := queues.DeleteQueue(ctx, &nativeSQS.DeleteQueueInput{QueueUrl: aws.String(r.queue)})
		cancel()
		failures = append(failures, wrapCommand("DeleteQueue", err))
	}
	if r.topic != "" {
		ctx, cancel := context.WithTimeout(context.Background(), operationBudget)
		_, err := topics.DeleteTopic(ctx, &nativeSNS.DeleteTopicInput{TopicArn: aws.String(r.topic)})
		cancel()
		failures = append(failures, wrapCommand("DeleteTopic", err))
	}
	return errors.Join(failures...)
}
