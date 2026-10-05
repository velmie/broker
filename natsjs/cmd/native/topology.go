package main

import (
	"context"
	"errors"
	"slices"
	"strconv"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"

	"github.com/velmie/broker"
	"github.com/velmie/broker/natsjs/v3"
)

const (
	topologyDescription   = "explicit native topology"
	correlationHeaderName = "Correlation-Id"
	topologyCurrentID     = "current-2"
	topologyFinalValue    = 3
)

func runTopology(ctx context.Context, nc *nats.Conn) (result error) {
	js, err := nc.JetStream(nats.MaxWait(operationTimeout))
	if err != nil {
		return operationError("topology", "configure", err)
	}
	stream := "BROKER_NATIVE_" + nuid.Next()
	defer func() { result = errors.Join(result, deleteStream(js, stream)) }()
	subject := stream + ".orders"
	if createErr := ensureStream(ctx, js, stream, []string{subject}); createErr != nil {
		return createErr
	}
	info, err := js.StreamInfo(stream, nats.Context(ctx))
	if err != nil {
		return operationError(stream, "stream_info", err)
	}
	info.Config.Description, info.Config.MaxMsgs = topologyDescription, 37
	if _, err = js.UpdateStream(&info.Config, nats.Context(ctx)); err != nil {
		return operationError(stream, "configure_stream", err)
	}
	if updateErr := ensureStream(ctx, js, stream, []string{subject, stream + ".receipts"}); updateErr != nil {
		return updateErr
	}
	info, err = js.StreamInfo(stream, nats.Context(ctx))
	if err != nil {
		return operationError(stream, "stream_info", err)
	}
	if info.Config.Description != topologyDescription || info.Config.MaxMsgs != 37 || len(info.Config.Subjects) != 2 {
		return operationError(stream, "reconcile_stream", errors.New("subject update changed unrelated stream policy"))
	}
	return verifyWireContinuity(ctx, nc, js, stream, subject)
}

func verifyWireContinuity(ctx context.Context, nc *nats.Conn, js nats.JetStreamContext, stream, subject string) error {
	var err error
	// The physical name is stable across local receiving generations. It does
	// not depend on a Coordinator registration label or an SDK subscription ID.
	const durable = "orders-worker"
	if _, err = js.AddConsumer(stream, &nats.ConsumerConfig{Durable: durable, FilterSubject: subject,
		AckPolicy: nats.AckExplicitPolicy}, nats.Context(ctx)); err != nil {
		return operationError(stream+"/"+durable, "create_consumer", err)
	}
	config := natsjs.ConsumerConfig{Stream: stream, Consumer: durable, Subject: subject}
	// This is the single-value baseline wire format. It does not execute an old
	// adapter binary. A native and a managed caller share the same durable here.
	if _, err = js.PublishMsg(&nats.Msg{Subject: subject, Header: nats.Header{
		nats.MsgIdHdr: {"baseline-1"}, correlationHeaderName: {"command-1"},
	}, Data: []byte(`{"value":1}`)}, nats.Context(ctx)); err != nil {
		return operationError(subject, "publish_baseline_format", err)
	}
	if consumeErr := consumeManaged(ctx, nc, config, 1); consumeErr != nil {
		return consumeErr
	}
	publisher, err := natsjs.NewPublisher(nc, natsjs.PublisherConfig{Subject: subject})
	if err != nil {
		return err
	}
	if publishErr := publisher.Publish(ctx, broker.Message{ID: topologyCurrentID, Body: []byte(`{"value":2}`),
		Headers: []broker.Header{{Name: correlationHeaderName, Value: []byte("command-2")}}}); publishErr != nil {
		return publishErr
	}
	if consumeErr := consumeNative(ctx, js, config); consumeErr != nil {
		return consumeErr
	}
	if _, err = js.PublishMsg(&nats.Msg{Subject: subject, Header: nats.Header{
		nats.MsgIdHdr: {"baseline-3"}, correlationHeaderName: {"command-3"},
	}, Data: []byte(`{"value":3}`)}, nats.Context(ctx)); err != nil {
		return operationError(subject, "publish_baseline_format", err)
	}
	if consumeErr := consumeManaged(ctx, nc, config, topologyFinalValue); consumeErr != nil {
		return consumeErr
	}
	consumer, err := js.ConsumerInfo(stream, durable, nats.Context(ctx))
	if err != nil {
		return operationError(stream+"/"+durable, "consumer_info", err)
	}
	if consumer.NumAckPending != 0 || consumer.AckFloor.Stream != 3 || consumer.Config.Durable != durable {
		return operationError(stream+"/"+durable, "progress_readback", errors.New("durable progress was not preserved"))
	}
	return nil
}

func ensureStream(ctx context.Context, js nats.JetStreamContext, stream string, subjects []string) error {
	callCtx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()
	info, err := js.StreamInfo(stream, nats.Context(callCtx))
	if err != nil && !errors.Is(err, nats.ErrStreamNotFound) {
		return operationError(stream, "stream_info", err)
	}
	config := nats.StreamConfig{Name: stream, Storage: nats.MemoryStorage}
	if info != nil {
		config = info.Config
		config.Subjects = slices.Clone(config.Subjects)
	}
	changed := false
	for _, subject := range subjects {
		if !slices.Contains(config.Subjects, subject) {
			config.Subjects = append(config.Subjects, subject)
			changed = true
		}
	}
	if info == nil {
		_, err = js.AddStream(&config, nats.Context(callCtx))
		return operationError(stream, "create_stream", err)
	}
	if changed {
		_, err = js.UpdateStream(&config, nats.Context(callCtx))
		return operationError(stream, "append_subjects", err)
	}
	return nil
}

//nolint:gocritic // Copy the fixed command configuration before adding its observer.
func consumeManaged(ctx context.Context, nc *nats.Conn, config natsjs.ConsumerConfig, value int) error {
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	confirmed := false
	config.Observer.Record = func(_ context.Context, event natsjs.Observation) {
		if event.Operation == natsjs.OperationAck && event.Outcome == natsjs.OutcomeConfirmed {
			confirmed = true
			stop()
		}
	}
	consumer, err := natsjs.NewConsumer(nc, config)
	if err != nil {
		return err
	}
	type command struct{ Value int }
	handler := broker.NewTypedDeliveryHandler(broker.DecodeJSON[command],
		func(_ context.Context, delivery broker.Delivery, command command) (broker.Disposition, error) {
			if command.Value != value || delivery.Message().ID != "baseline-"+strconv.Itoa(value) {
				return nil, errors.New("baseline-format message or durable progress changed")
			}
			correlation := ""
			for _, header := range delivery.Message().Headers {
				if header.Name == correlationHeaderName {
					correlation = string(header.Value)
				}
			}
			if correlation != "command-"+strconv.Itoa(value) {
				return nil, errors.New("baseline correlation changed")
			}
			return broker.Handled{}, nil
		})
	err = consumer.Run(runCtx, handler)
	if confirmed && onlyNativeCancellation(err) {
		return nil
	}
	return err
}

//nolint:gocritic // The command passes its immutable binding as one value.
func consumeNative(ctx context.Context, js nats.JetStreamContext, config natsjs.ConsumerConfig) (result error) {
	subscription, err := js.PullSubscribe(config.Subject, config.Consumer, nats.Bind(config.Stream, config.Consumer), nats.ManualAck())
	if err != nil {
		return operationError(config.Stream, "bind_native", err)
	}
	defer func() {
		result = errors.Join(result, operationError(config.Stream, "unsubscribe_native", subscription.Unsubscribe()))
	}()
	messages, err := subscription.Fetch(1, nats.Context(ctx))
	if err != nil {
		return operationError(config.Stream, "fetch_native", err)
	}
	if len(messages) != 1 || messages[0].Header.Get(nats.MsgIdHdr) != topologyCurrentID ||
		messages[0].Header.Get(correlationHeaderName) != "command-2" || string(messages[0].Data) != `{"value":2}` {
		return operationError(config.Stream, "decode_native", errors.New("current message representation or progress changed"))
	}
	ackCtx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()
	return operationError(config.Stream, "ack_native", messages[0].AckSync(nats.Context(ackCtx)))
}

func onlyNativeCancellation(err error) bool {
	if err == context.Canceled {
		return true
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		causes := joined.Unwrap()
		for _, cause := range causes {
			if !onlyNativeCancellation(cause) {
				return false
			}
		}
		return len(causes) > 0
	}
	if cause := errors.Unwrap(err); cause != nil {
		return onlyNativeCancellation(cause)
	}
	return false
}
