package broker_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/velmie/broker"
)

func TestTypedCallbackNestedDecodeCauseUsesOuterHandleStage(t *testing.T) {
	cause := errors.New("response decode failed")
	inner := &broker.DecodeError{Cause: cause}
	calls := 0
	h := broker.NewTypedHandler(broker.DecodeJSON[command], func(context.Context, command) error {
		return inner
	}, broker.WithRedelivery(time.Second, func(err error) bool {
		calls++
		return errors.Is(err, cause)
	}))
	result, err := h.Handle(context.Background(), delivery{`{}`})
	retry, ok := result.(broker.RetryAfter)
	if err != nil || !ok || calls != 1 || !errors.Is(retry.Cause, cause) {
		t.Fatalf("outer callback boundary not classified once: result=%T err=%v calls=%d", result, err, calls)
	}
}

func TestRetryPolicyUsesOnlyFirstExplicitBoundary(t *testing.T) {
	cause := errors.New("remote outcome unknown")
	handle := &broker.StageError{Stage: broker.StageHandle, Cause: cause}
	decode := &broker.DecodeError{Cause: cause}
	for _, tc := range []struct {
		name     string
		failure  error
		eligible bool
	}{
		{"transparent_handle", fmt.Errorf("callback: %w", handle), true},
		{"handle_join", &broker.StageError{Stage: broker.StageHandle, Cause: errors.Join(decode, cause)}, true},
		{"decode_wrapping_handle", &broker.DecodeError{Cause: handle}, false},
		{"publish_wrapping_handle", &broker.StageError{Stage: broker.StagePublish, Cause: handle}, false},
		{"unmarked_join", errors.Join(handle, decode), false},
		{"unmarked", cause, false},
	} {
		for _, accept := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/accept=%t", tc.name, accept), func(t *testing.T) {
				calls := 0
				original := broker.Handled{}
				callback := func(context.Context, broker.Delivery) (broker.Disposition, error) { return original, tc.failure }
				h := broker.NewHandler(callback, broker.WithRedelivery(time.Second, func(err error) bool {
					calls++
					if err != tc.failure {
						t.Error("classifier did not receive full error")
					}
					return accept
				}))
				result, err := h.Handle(context.Background(), delivery{})
				wantCalls := 0
				if tc.eligible {
					wantCalls = 1
				}
				if calls != wantCalls {
					t.Fatalf("classifier calls=%d want=%d", calls, wantCalls)
				}
				if tc.eligible && accept {
					retry, ok := result.(broker.RetryAfter)
					if !ok || err != nil || retry.Cause != tc.failure {
						t.Fatal(result, err)
					}
				} else if err != tc.failure || result != original {
					t.Fatal("rejected error/result changed", result, err)
				}
			})
		}
	}
}
