package natsjs_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/velmie/broker"
	"github.com/velmie/broker/natsjs/v4"
)

func TestProcessorUsesNativeAckNoneAndOrderedConsumers(t *testing.T) {
	for _, ordered := range []bool{false, true} {
		name := "ack_none"
		if ordered {
			name = "ordered"
		}
		t.Run(name, func(t *testing.T) {
			nc, js, config := provision(t, time.Second)
			options := []nats.SubOpt{nats.BindStream(config.Stream), nats.AckNone()}
			if ordered {
				options = []nats.SubOpt{nats.BindStream(config.Stream), nats.OrderedConsumer()}
			}
			sub, err := js.SubscribeSync(config.Subjects[0], options...)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if closeErr := sub.Unsubscribe(); closeErr != nil {
					t.Error(closeErr)
				}
			})
			info, err := sub.ConsumerInfo()
			if err != nil {
				t.Fatal(err)
			}
			source := config.Stream + "/" + info.Name
			var subjects []string
			var metadata []nats.MsgMetadata
			failure := errors.New("effect failed without acknowledgment policy")
			handler := broker.NewTypedDeliveryHandler(broker.DecodeJSON[struct{ Fail bool }],
				func(_ context.Context, delivery broker.Delivery, value struct{ Fail bool }) (broker.Disposition, error) {
					subjects = append(subjects, delivery.(natsjs.SubjectReader).Subject())
					metadata = append(metadata, delivery.(natsjs.MetadataReader).Metadata())
					if delivery.Source() != source {
						t.Error("source identity changed")
					}
					if value.Fail {
						return nil, failure
					}
					return broker.Handled{}, nil
				})
			processor, err := natsjs.NewProcessor(handler, source)
			if err != nil {
				t.Fatal(err)
			}
			processNoAckPair(t, js, sub, processor, config.Subjects[0], failure)
			if subjects[0] != config.Subjects[0] || subjects[1] != config.Subjects[0] || metadata[0].Sequence.Stream != 1 ||
				metadata[1].Sequence.Stream != 2 {
				t.Fatalf("detached metadata: %v %+v", subjects, metadata)
			}
			info, err = sub.ConsumerInfo()
			if err != nil {
				t.Fatal(err)
			}
			if info.Config.AckPolicy != nats.AckNonePolicy || info.NumAckPending != 0 || info.Delivered.Stream != 2 {
				t.Fatalf("native no-ack policy changed: %+v", info)
			}
			if _, err = sub.NextMsg(100 * time.Millisecond); !errors.Is(err, nats.ErrTimeout) {
				t.Fatalf("AckNone processing failure produced another delivery: %v", err)
			}
			if err = nc.Flush(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestProcessorNativeMultipleFilters(t *testing.T) {
	nc, js, config := provision(t, time.Second)
	stream, err := js.StreamInfo(config.Stream)
	if err != nil {
		t.Fatal(err)
	}
	stream.Config.Subjects = []string{config.Stream + ".*"}
	if _, err = js.UpdateStream(&stream.Config); err != nil {
		t.Fatal(err)
	}
	filters := []string{config.Stream + ".created", config.Stream + ".updated"}
	remote, err := js.AddConsumer(config.Stream, &nats.ConsumerConfig{
		Durable: "filtered", AckPolicy: nats.AckExplicitPolicy, FilterSubjects: filters,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(remote.Config.FilterSubjects, filters) {
		t.Fatalf("filters changed: %+v", remote.Config)
	}
	sub, err := js.PullSubscribe("", "", nats.Bind(config.Stream, remote.Name))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := sub.Unsubscribe(); closeErr != nil {
			t.Error(closeErr)
		}
	})
	var subjects []string
	processor, err := natsjs.NewProcessor(broker.NewHandler(func(_ context.Context, delivery broker.Delivery) (broker.Disposition, error) {
		subjects = append(subjects, delivery.(natsjs.SubjectReader).Subject())
		return broker.Handled{}, nil
	}), config.Stream+"/"+remote.Name)
	if err != nil {
		t.Fatal(err)
	}
	for _, subject := range []string{filters[0], config.Stream + ".excluded", filters[1]} {
		if _, err = js.Publish(subject, []byte(`{}`)); err != nil {
			t.Fatal(err)
		}
	}
	messages, err := sub.Fetch(2, nats.MaxWait(testTimeout))
	if err != nil {
		t.Fatal(err)
	}
	for _, message := range messages {
		if err = processor.Process(context.Background(), message); err != nil {
			t.Fatal(err)
		}
		if err = message.AckSync(); err != nil {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(subjects, filters) {
		t.Fatalf("received subjects=%v", subjects)
	}
	if _, err = sub.Fetch(1, nats.MaxWait(100*time.Millisecond)); !errors.Is(err, nats.ErrTimeout) {
		t.Fatalf("excluded delivered: %v", err)
	}
	stream, err = js.StreamInfo(config.Stream)
	if err != nil || stream.State.Msgs != 3 {
		t.Fatalf("native stream retention: %+v %v", stream, err)
	}
	if err = nc.Flush(); err != nil {
		t.Fatal(err)
	}
}

func TestProcessorNativeBackOffAndExplicitDelayedNak(t *testing.T) {
	for _, delayedNak := range []bool{false, true} {
		name := "timeout"
		if delayedNak {
			name = "delayed_nak"
		}
		t.Run(name, func(t *testing.T) { checkNativeRedelivery(t, delayedNak) })
	}
}

func processNoAckPair(t *testing.T, js nats.JetStreamContext, sub *nats.Subscription, processor *natsjs.Processor,
	subject string, failure error,
) {
	t.Helper()
	for _, body := range []string{`{"Fail":false}`, `{"Fail":true}`} {
		if _, err := js.Publish(subject, []byte(body)); err != nil {
			t.Fatal(err)
		}
		message, receiveErr := sub.NextMsg(testTimeout)
		if receiveErr != nil {
			t.Fatal(receiveErr)
		}
		processErr := processor.Process(context.Background(), message)
		if (body == `{"Fail":true}` && !errors.Is(processErr, failure)) || (body == `{"Fail":false}` && processErr != nil) {
			t.Fatalf("processing cause: %v", processErr)
		}
	}
}

func checkNativeRedelivery(t *testing.T, delayedNak bool) {
	t.Helper()
	nc, js, config := provision(t, time.Second)
	const backoff = 2 * time.Second
	remote, err := js.AddConsumer(config.Stream, &nats.ConsumerConfig{
		Durable: "backoff", FilterSubject: config.Subjects[0], AckPolicy: nats.AckExplicitPolicy, MaxDeliver: 3,
		BackOff: []time.Duration{backoff, backoff},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(remote.Config.BackOff, []time.Duration{backoff, backoff}) {
		t.Fatalf("backoff=%v", remote.Config.BackOff)
	}
	sub, err := js.PullSubscribe(config.Subjects[0], "", nats.Bind(config.Stream, remote.Name))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := sub.Unsubscribe(); closeErr != nil {
			t.Error(closeErr)
		}
	})
	failure := errors.New("retryable business failure")
	var attempts []uint64
	processor, err := natsjs.NewProcessor(broker.NewHandler(func(_ context.Context, delivery broker.Delivery) (broker.Disposition, error) {
		attempts = append(attempts, delivery.(natsjs.MetadataReader).Metadata().NumDelivered)
		if len(attempts) == 1 {
			return nil, failure
		}
		return broker.Handled{}, nil
	}), config.Stream+"/"+remote.Name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = js.Publish(config.Subjects[0], []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	messages, err := sub.Fetch(1, nats.MaxWait(testTimeout))
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if err = processor.Process(context.Background(), messages[0]); !errors.Is(err, failure) {
		t.Fatalf("cause lost: %v", err)
	}
	if delayedNak {
		if err = messages[0].NakWithDelay(100 * time.Millisecond); err != nil {
			t.Fatal(err)
		}
	}
	wait := testTimeout
	if delayedNak {
		wait = time.Second
	}
	messages, err = sub.Fetch(1, nats.MaxWait(wait))
	if err != nil {
		t.Fatal(err)
	}
	if !delayedNak && time.Since(started) < backoff-100*time.Millisecond {
		t.Fatal("timeout ignored native BackOff")
	}
	if err = processor.Process(context.Background(), messages[0]); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(attempts, []uint64{1, 2}) {
		t.Fatalf("attempts=%v", attempts)
	}
	if err = messages[0].AckSync(); err != nil {
		t.Fatal(err)
	}
	if err = nc.Flush(); err != nil {
		t.Fatal(err)
	}
}
