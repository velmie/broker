package natsjs_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/velmie/broker"
	"github.com/velmie/broker/natsjs/v3"
)

func TestProcessorBatchMakesCumulativeConfirmationConditionalOnEveryEffect(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "failure"}[fail], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
			defer cancel()
			subscription, messages, source := cumulativeBatch(t, ctx)
			marker := errors.New("second effect failed")
			var effects int
			processor, err := natsjs.NewProcessor(broker.NewTypedHandler(broker.DecodeJSON[int],
				func(_ context.Context, value int) error {
					effects++
					if value != effects {
						t.Errorf("batch order changed: value=%d effect=%d", value, effects)
					}
					if fail && value == 2 {
						return marker
					}
					return nil
				}), source)
			if err != nil {
				t.Fatal(err)
			}
			processErr := processor.ProcessBatch(ctx, messages)
			before, err := subscription.ConsumerInfo()
			if err != nil {
				t.Fatal(err)
			}
			if before.NumAckPending != len(messages) || before.AckFloor.Stream != 0 {
				t.Fatal("processing performed native settlement")
			}
			if fail {
				if !errors.Is(processErr, marker) || effects != 2 {
					t.Fatalf("failed batch did not stop: effects=%d err=%v", effects, processErr)
				}
				return // No later cumulative ACK may cover the failed effect.
			}
			if processErr != nil || effects != len(messages) {
				t.Fatalf("successful effects=%d err=%v", effects, processErr)
			}
			if err = messages[len(messages)-1].AckSync(nats.Context(ctx)); err != nil {
				t.Fatal(err)
			}
			after, err := subscription.ConsumerInfo()
			if err != nil || after.NumAckPending != 0 || after.AckFloor.Stream != uint64(len(messages)) {
				t.Fatalf("cumulative confirmation: %+v err=%v", after, err)
			}
		})
	}
}

func cumulativeBatch(t *testing.T, ctx context.Context) (*nats.Subscription, []*nats.Msg, string) {
	t.Helper()
	_, js, config := provision(t, time.Second)
	subscription, err := js.SubscribeSync(config.Subject, nats.BindStream(config.Stream),
		nats.Durable("cumulative"), nats.AckAll(), nats.AckWait(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := subscription.Unsubscribe(); closeErr != nil {
			t.Error(closeErr)
		}
	})
	var messages []*nats.Msg
	for _, body := range []string{"1", "2", "3"} {
		if _, err = js.Publish(config.Subject, []byte(body)); err != nil {
			t.Fatal(err)
		}
		message, receiveErr := subscription.NextMsgWithContext(ctx)
		if receiveErr != nil {
			t.Fatal(receiveErr)
		}
		messages = append(messages, message)
	}
	return subscription, messages, config.Stream + "/cumulative"
}
