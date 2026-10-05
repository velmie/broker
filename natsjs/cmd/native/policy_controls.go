package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	policyResourceMaxDeliver      = 3
	policyResourceMaxAckPending   = 2
	policyResourceMaxRequestBatch = 2
	policyResourceMaxWaiting      = 2
	policyRatePayloadBytes        = 16 * 1024
	policyRateBitsPerSecond       = 1024 * 1024
	policyPullMaxBytes            = 1024
	policyPullWait                = 500 * time.Millisecond
	policyExcessivePullWait       = 1500 * time.Millisecond
	policyExcessivePullBytes      = 2048
	policyFlowPendingMessages     = 1024
	policyFlowPendingBytes        = 16 * 1024 * 1024
	policyHeartbeat               = 100 * time.Millisecond
	policyFlowPayloadBytes        = 64 * 1024
)

func policyRate(ctx context.Context, js nats.JetStreamContext) (result error) {
	stream, cleanup, err := newStream(ctx, js)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, cleanup()) }()
	info, err := js.StreamInfo(stream, nats.Context(ctx))
	if err != nil {
		return operationError("rate", "stream-info", err)
	}
	info.Config.MaxMsgSize = 64 * 1024
	if _, err = js.UpdateStream(&info.Config, nats.Context(ctx)); err != nil {
		return operationError("rate", "set-message-size", err)
	}
	payload := make([]byte, policyRatePayloadBytes)
	for i := 0; i < 16; i++ {
		if _, err = js.Publish(stream+".events", payload, nats.Context(ctx)); err != nil {
			return operationError("rate", "publish", err)
		}
	}
	started := time.Now()
	sub, err := js.SubscribeSync(stream+".events", nats.BindStream(stream), nats.AckNone(),
		nats.RateLimit(policyRateBitsPerSecond), nats.Context(ctx))
	if err != nil {
		return operationError("rate", "subscribe", err)
	}
	defer func() { result = errors.Join(result, operationError("rate", "unsubscribe", sub.Unsubscribe())) }()
	for i := 0; i < 16; i++ {
		message, receiveErr := sub.NextMsgWithContext(ctx)
		if receiveErr != nil {
			return operationError("rate", "receive", receiveErr)
		}
		if len(message.Data) != len(payload) {
			return errors.New("rate: incomplete payload")
		}
	}
	if elapsed := time.Since(started); elapsed < 500*time.Millisecond {
		return fmt.Errorf("rate: 256 KiB exceeded 64 KiB burst without observed delay: %s", elapsed)
	}
	consumer, err := sub.ConsumerInfo()
	if err != nil {
		return operationError("rate", "consumer-info", err)
	}
	if consumer.Config.RateLimit != 1024*1024 {
		return errors.New("rate: native configured limit differs")
	}
	return nil
}

func policyLimits(ctx context.Context, js nats.JetStreamContext) (result error) {
	stream, cleanup, err := newStream(ctx, js)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, cleanup()) }()
	sub, err := js.PullSubscribe(stream+".events", "limits", nats.BindStream(stream),
		nats.AckExplicit(), nats.MaxDeliver(policyResourceMaxDeliver), nats.MaxAckPending(policyResourceMaxAckPending),
		nats.MaxRequestBatch(policyResourceMaxRequestBatch),
		nats.MaxRequestExpires(time.Second), nats.MaxRequestMaxBytes(policyPullMaxBytes), nats.PullMaxWaiting(policyResourceMaxWaiting),
		nats.InactiveThreshold(time.Minute), nats.Description("Bounded native pull requests"),
		nats.ConsumerMemoryStorage(), nats.ConsumerReplicas(1), nats.Context(ctx))
	if err != nil {
		return operationError("limits", "subscribe", err)
	}
	defer func() { result = errors.Join(result, operationError("limits", "unsubscribe", sub.Unsubscribe())) }()
	info, err := sub.ConsumerInfo()
	if err != nil {
		return operationError("limits", "consumer-info", err)
	}
	if verificationErr := verifyResourceLimits(&info.Config); verificationErr != nil {
		return verificationErr
	}

	for _, request := range []struct {
		batch   int
		options []nats.PullOpt
		cause   string
	}{
		{3, []nats.PullOpt{nats.MaxWait(policyPullWait)}, "nats: Exceeded MaxRequestBatch of 2"},
		{1, []nats.PullOpt{nats.MaxWait(policyExcessivePullWait)}, "nats: Exceeded MaxRequestExpires of 1s"},
		{1, []nats.PullOpt{nats.MaxWait(policyPullWait), nats.PullMaxBytes(policyExcessivePullBytes)},
			"nats: Exceeded MaxRequestMaxBytes of 1024"},
	} {
		_, err = sub.Fetch(request.batch, request.options...)
		if err == nil || err.Error() != request.cause {
			return errors.Join(fmt.Errorf("limits: missing native rejection %s", request.cause), err)
		}
	}

	return nil
}

func policyFlow(ctx context.Context, connection *nats.Conn, js nats.JetStreamContext) (result error) {
	stream, cleanup, err := newStream(ctx, js)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, cleanup()) }()
	delivery := nats.NewInbox()
	// A passive core subscription observes protocol frames. The JetStream SDK
	// owns flow-control responses and filters these frames from application data.
	wire, err := connection.SubscribeSync(delivery)
	if err != nil {
		return operationError("flow", "observe-protocol", err)
	}
	defer func() {
		result = errors.Join(result, operationError("flow", "unsubscribe-observer", wire.Unsubscribe()))
	}()
	if err = wire.SetPendingLimits(policyFlowPendingMessages, policyFlowPendingBytes); err != nil {
		return operationError("flow", "observer-limits", err)
	}
	sub, err := js.SubscribeSync(stream+".events", nats.BindStream(stream), nats.DeliverSubject(delivery),
		nats.AckNone(), nats.EnableFlowControl(), nats.IdleHeartbeat(policyHeartbeat), nats.Context(ctx))
	if err != nil {
		return operationError("flow", "subscribe", err)
	}
	defer func() { result = errors.Join(result, operationError("flow", "unsubscribe", sub.Unsubscribe())) }()
	if err = sub.SetPendingLimits(policyFlowPendingMessages, policyFlowPendingBytes); err != nil {
		return operationError("flow", "application-limits", err)
	}
	for heartbeats := 0; heartbeats < 2; {
		message, receiveErr := wire.NextMsgWithContext(ctx)
		if receiveErr != nil {
			return operationError("flow", "heartbeat", receiveErr)
		}
		if message.Header.Get("Status") == "100" && message.Header.Get("Description") == "Idle Heartbeat" {
			heartbeats++
		}
	}
	payload := make([]byte, policyFlowPayloadBytes)
	for i := 0; i < 128; i++ {
		if _, err = js.Publish(stream+".events", payload, nats.Context(ctx)); err != nil {
			return operationError("flow", "publish", err)
		}
	}
	for i := 0; i < 128; i++ {
		message, receiveErr := sub.NextMsgWithContext(ctx)
		if receiveErr != nil {
			return operationError("flow", "receive", receiveErr)
		}
		if len(message.Data) != len(payload) {
			return errors.New("flow: incomplete payload")
		}
	}
	if windowErr := policyFlowWindow(ctx, wire); windowErr != nil {
		return windowErr
	}

	if _, err = js.Publish(stream+".events", []byte("continued"), nats.Context(ctx)); err != nil {
		return operationError("flow", "publish-after-window", err)
	}
	message, err := sub.NextMsgWithContext(ctx)
	if err != nil {
		return operationError("flow", "receive-after-window", err)
	}
	if string(message.Data) != "continued" {
		return errors.New("flow: delivery did not continue after protocol window")
	}
	return nil
}

func policyFlowWindow(ctx context.Context, wire *nats.Subscription) error {
	flowSeen := false
	dataAfterFlow := false
	for received := 0; received < 128; {
		message, receiveErr := wire.NextMsgWithContext(ctx)
		if receiveErr != nil {
			return operationError("flow", "protocol-window", receiveErr)
		}
		if message.Header.Get("Status") == "100" {
			if message.Reply != "" && message.Header.Get("Description") == "FlowControl Request" {
				flowSeen = true
			}
			continue
		}
		received++
		if flowSeen {
			dataAfterFlow = true
		}
	}
	if !dataAfterFlow {
		return errors.New("flow: no observed data continuation after native flow-control request")
	}
	return nil
}

func verifyResourceLimits(c *nats.ConsumerConfig) error {
	for _, field := range []struct {
		name    string
		matches bool
	}{
		{"MaxDeliver", c.MaxDeliver == policyResourceMaxDeliver},
		{"MaxAckPending", c.MaxAckPending == policyResourceMaxAckPending},
		{"MaxRequestBatch", c.MaxRequestBatch == policyResourceMaxRequestBatch},
		{"MaxRequestExpires", c.MaxRequestExpires == time.Second},
		{"MaxRequestMaxBytes", c.MaxRequestMaxBytes == policyPullMaxBytes},
		{"MaxWaiting", c.MaxWaiting == policyResourceMaxWaiting},
		{"InactiveThreshold", c.InactiveThreshold == time.Minute},
		{"Description", c.Description == "Bounded native pull requests"},
		{"MemoryStorage", c.MemoryStorage}, {"Replicas", c.Replicas == 1},
	} {
		if !field.matches {
			return operationError("limits", "verify-resource", &fieldError{Field: field.name,
				Cause: errors.New("native resource configuration differs"),
			})
		}
	}

	return nil
}
