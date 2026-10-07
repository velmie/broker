package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	policyAckNoneMode    = "none"
	policyOrderedMode    = "ordered"
	policyAckAll         = "all"
	policyBackoffMode    = "backoff"
	policyStartNew       = "new"
	policyEventType      = "created"
	policyAckWait        = 10 * time.Second
	policyDeliveryLimit  = 3
	policyStartSequence  = 3
	policyNoExtraWait    = 40 * time.Millisecond
	policyReplayInterval = 800 * time.Millisecond
)

func runPolicies(ctx context.Context, connection *nats.Conn) error {
	js, err := connection.JetStream(nats.MaxWait(operationTimeout))
	if err != nil {
		return operationError("policies", "jetstream", err)
	}
	for _, run := range []func(context.Context, nats.JetStreamContext) error{
		policyAcknowledgments, policyStarts, policyReplay, policyHeaders, policyRate, policyLimits,
	} {
		if err := run(ctx, js); err != nil {
			return err
		}
	}
	return policyFlow(ctx, connection, js)
}

func policyAcknowledgments(ctx context.Context, js nats.JetStreamContext) (result error) {
	stream, cleanup, err := newStream(ctx, js)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, cleanup()) }()
	for i := 1; i <= 3; i++ {
		if _, err = js.Publish(stream+".events", []byte(fmt.Sprint(i)), nats.Context(ctx)); err != nil {
			return operationError(stream, "publish", err)
		}
	}
	for _, mode := range []string{policyAckNoneMode, policyAckAll, policyOrderedMode, policyBackoffMode} {
		if err := policyAcknowledgment(ctx, js, stream, mode); err != nil {
			return err
		}
	}
	return nil
}

func policyAcknowledgment(ctx context.Context, js nats.JetStreamContext, stream, mode string) (result error) {
	options := []nats.SubOpt{nats.BindStream(stream), nats.DeliverAll(), nats.Context(ctx)}
	switch mode {
	case policyAckNoneMode:
		options = append(options, nats.AckNone())
	case policyAckAll:
		options = append(options, nats.AckAll(), nats.AckWait(policyAckWait))
	case policyOrderedMode:
		options = append(options, nats.OrderedConsumer())
	case policyBackoffMode:
		options = append(options, nats.AckExplicit(),
			nats.BackOff([]time.Duration{250 * time.Millisecond, 500 * time.Millisecond}),
			nats.MaxDeliver(policyDeliveryLimit), nats.MaxAckPending(1))
	}
	sub, err := js.SubscribeSync(stream+".events", options...)
	if err != nil {
		return operationError(mode, "subscribe", err)
	}
	defer func() { result = errors.Join(result, operationError(mode, "unsubscribe", sub.Unsubscribe())) }()
	if mode == policyBackoffMode {
		return policyBackoff(ctx, sub)
	}
	var last *nats.Msg
	var batch []*nats.Msg
	var sequence uint64
	for i := 1; i <= 3; i++ {
		last, err = sub.NextMsgWithContext(ctx)
		if err != nil {
			return operationError(mode, "receive", err)
		}
		metadata, metaErr := last.Metadata()
		if metaErr != nil {
			return operationError(mode, "metadata", metaErr)
		}
		if metadata.Sequence.Stream <= sequence || string(last.Data) != fmt.Sprint(i) {
			return fmt.Errorf("%s: unexpected ordered effect at %d", mode, i)
		}
		sequence = metadata.Sequence.Stream
		batch = append(batch, last)
	}
	return policyAcknowledgmentState(ctx, sub, mode, batch, sequence)
}

func policyAcknowledgmentState(ctx context.Context, sub *nats.Subscription, mode string, batch []*nats.Msg, sequence uint64) error {
	info, err := sub.ConsumerInfo()
	if err != nil {
		return operationError(mode, "consumer-info", err)
	}
	if mode == policyAckAll {
		// Keep the batch unconfirmed until all application effects succeed.
		if info.NumAckPending != 3 || info.AckFloor.Stream != 0 {
			return fmt.Errorf("ack-all: unconfirmed batch pending=%d floor=%d", info.NumAckPending, info.AckFloor.Stream)
		}
		if batchErr := policyConfirmBatch(ctx, batch, func(message *nats.Msg) error {
			if len(message.Data) == 0 {
				return errors.New("empty effect")
			}
			return nil
		}); batchErr != nil {
			return batchErr
		}
		info, err = sub.ConsumerInfo()
		if err != nil {
			return operationError(mode, "consumer-info", err)
		}
		if info.NumAckPending != 0 || info.AckFloor.Stream != sequence {
			return fmt.Errorf("ack-all: cumulative floor=%d pending=%d", info.AckFloor.Stream, info.NumAckPending)
		}
	} else if info.Config.AckPolicy != nats.AckNonePolicy || info.NumAckPending != 0 {
		return fmt.Errorf("%s: unexpected acknowledgment policy or pending state", mode)
	}
	if mode == policyOrderedMode && (info.Config.Durable != "" || !info.Config.MemoryStorage ||
		info.Config.Replicas != 1 || info.Config.MaxDeliver != 1) {
		return errors.New("ordered: ephemeral memory consumer contract differs")
	}
	return nil
}

func policyBackoff(ctx context.Context, sub *nats.Subscription) error {
	first, err := sub.NextMsgWithContext(ctx)
	if err != nil {
		return operationError(policyBackoffMode, "first-receive", err)
	}
	started := time.Now()
	repeated, err := sub.NextMsgWithContext(ctx)
	if err != nil {
		return operationError(policyBackoffMode, "redelivery", err)
	}
	firstMeta, err := first.Metadata()
	if err != nil {
		return operationError(policyBackoffMode, "metadata", err)
	}
	repeatedMeta, err := repeated.Metadata()
	if err != nil {
		return operationError(policyBackoffMode, "metadata", err)
	}
	if firstMeta.NumDelivered != 1 || repeatedMeta.NumDelivered != 2 || firstMeta.Sequence.Stream != repeatedMeta.Sequence.Stream ||
		time.Since(started) < 150*time.Millisecond {
		return errors.New("backoff: native timeout redelivery was not observed")
	}
	return operationError(policyBackoffMode, "ack", repeated.AckSync(nats.Context(ctx)))
}

func policyStarts(ctx context.Context, js nats.JetStreamContext) (result error) {
	stream, cleanup, err := newStream(ctx, js)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, cleanup()) }()
	for index, subject := range []string{"a", "b", "a", "b"} {
		if _, err = js.Publish(stream+"."+subject, []byte(fmt.Sprint(index+1)), nats.Context(ctx)); err != nil {
			return operationError(stream, "publish", err)
		}
	}
	stored, err := js.GetMsg(stream, policyStartSequence, nats.Context(ctx))
	if err != nil {
		return operationError(stream, "get-message", err)
	}
	cases := []struct {
		name   string
		option nats.SubOpt
		want   []string
	}{
		{policyAckAll, nats.DeliverAll(), []string{"1", "2", "3", "4"}},
		{"last", nats.DeliverLast(), []string{"4"}},
		{"last-per-subject", nats.DeliverLastPerSubject(), []string{"3", "4"}},
		{"sequence", nats.StartSequence(policyStartSequence), []string{"3", "4"}},
		{"time", nats.StartTime(stored.Time), []string{"3", "4"}},
		{policyStartNew, nats.DeliverNew(), []string{"5"}},
	}
	for _, test := range cases {
		if err := policyStart(ctx, js, stream, test.name, test.option, test.want); err != nil {
			return err
		}
	}
	return nil
}

func policyStart(ctx context.Context, js nats.JetStreamContext, stream, name string, option nats.SubOpt, want []string) (result error) {
	sub, err := js.SubscribeSync(stream+".>", nats.BindStream(stream), nats.AckNone(), option, nats.Context(ctx))
	if err != nil {
		return operationError(name, "subscribe", err)
	}
	defer func() { result = errors.Join(result, operationError(name, "unsubscribe", sub.Unsubscribe())) }()
	if name == policyStartNew {
		if _, err = js.Publish(stream+".a", []byte("5"), nats.Context(ctx)); err != nil {
			return operationError(name, "publish", err)
		}
	}
	for _, expected := range want {
		message, receiveErr := sub.NextMsgWithContext(ctx)
		if receiveErr != nil {
			return operationError(name, "receive", receiveErr)
		}
		if string(message.Data) != expected {
			return fmt.Errorf("%s: expected sequence %s", name, expected)
		}
	}
	if _, err = sub.NextMsg(policyNoExtraWait); !errors.Is(err, nats.ErrTimeout) {
		return errors.Join(fmt.Errorf("%s: unexpected extra delivery", name), err)
	}
	return nil
}

func policyReplay(ctx context.Context, js nats.JetStreamContext) (result error) {
	stream, cleanup, err := newStream(ctx, js)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, cleanup()) }()
	if _, err = js.Publish(stream+".events", []byte("first"), nats.Context(ctx)); err != nil {
		return operationError("replay", "publish", err)
	}
	timer := time.NewTimer(policyReplayInterval)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
		return ctx.Err()
	}
	if _, err = js.Publish(stream+".events", []byte("second"), nats.Context(ctx)); err != nil {
		return operationError("replay", "publish", err)
	}
	instant, err := policyReplaySpacing(ctx, js, stream, nats.ReplayInstant())
	if err != nil {
		return err
	}
	original, err := policyReplaySpacing(ctx, js, stream, nats.ReplayOriginal())
	if err != nil {
		return err
	}
	if original < 500*time.Millisecond || original < instant+200*time.Millisecond {
		return fmt.Errorf("replay: original=%s instant=%s", original, instant)
	}
	return nil
}

func policyReplaySpacing(
	ctx context.Context, js nats.JetStreamContext, stream string, option nats.SubOpt,
) (spacing time.Duration, result error) {
	sub, err := js.SubscribeSync(stream+".events", nats.BindStream(stream), nats.AckNone(), option, nats.Context(ctx))
	if err != nil {
		return 0, operationError("replay", "subscribe", err)
	}
	defer func() { result = errors.Join(result, operationError("replay", "unsubscribe", sub.Unsubscribe())) }()
	first, err := sub.NextMsgWithContext(ctx)
	if err != nil {
		return 0, operationError("replay", "receive-first", err)
	}
	started := time.Now()
	second, err := sub.NextMsgWithContext(ctx)
	if err != nil {
		return 0, operationError("replay", "receive-second", err)
	}
	if string(first.Data) != "first" || string(second.Data) != "second" {
		return 0, errors.New("replay: incorrect effects")
	}
	return time.Since(started), nil
}

func policyHeaders(ctx context.Context, js nats.JetStreamContext) (result error) {
	stream, cleanup, err := newStream(ctx, js)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, cleanup()) }()
	message := &nats.Msg{Subject: stream + ".events", Data: []byte("payload"), Header: nats.Header{"Event-Type": []string{policyEventType}}}
	if _, err = js.PublishMsg(message, nats.Context(ctx)); err != nil {
		return operationError("headers", "publish", err)
	}
	sub, err := js.SubscribeSync(stream+".events", nats.BindStream(stream), nats.AckNone(), nats.HeadersOnly(), nats.Context(ctx))
	if err != nil {
		return operationError("headers", "subscribe", err)
	}
	defer func() { result = errors.Join(result, operationError("headers", "unsubscribe", sub.Unsubscribe())) }()
	message, err = sub.NextMsgWithContext(ctx)
	if err != nil {
		return operationError("headers", "receive", err)
	}
	if len(message.Data) != 0 || message.Header.Get("Event-Type") != policyEventType || message.Header.Get("Nats-Msg-Size") != "7" {
		return errors.New("headers: body was not removed or metadata was lost")
	}
	return nil
}

// A cumulative acknowledgment is valid only after every effect in this batch.
func policyConfirmBatch(ctx context.Context, messages []*nats.Msg, effect func(*nats.Msg) error) error {
	for _, message := range messages {
		if err := effect(message); err != nil {
			return operationError("ack-all", "effect", err)
		}
	}
	return operationError("ack-all", "ack", messages[len(messages)-1].AckSync(nats.Context(ctx)))
}
