package natsjs

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestRetryableFetchError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "disconnect",
			err:  nats.ErrFetchDisconnected,
			want: true,
		},
		{
			name: "leadership",
			err:  nats.ErrConsumerLeadershipChanged,
			want: true,
		},
		{
			name: "no responders wrapped",
			err:  fmt.Errorf("receive: %w", nats.ErrNoResponders),
			want: true,
		},
		{
			name: "reconnect buffer",
			err:  nats.ErrReconnectBufExceeded,
			want: true,
		},
		{
			name: "shutdown",
			err:  errors.New("nats: Server Shutdown"),
			want: true,
		},
		{
			name: "max waiting",
			err:  errors.New("nats: Exceeded MaxWaiting"),
			want: true,
		},
		{
			name: "closed",
			err:  nats.ErrConnectionClosed,
		},
		{
			name: "invalid subscription",
			err:  nats.ErrBadSubscription,
		},
		{
			name: "deleted",
			err:  nats.ErrConsumerDeleted,
		},
		{
			name: "permission",
			err:  nats.ErrPermissionViolation,
		},
		{
			name: "slow",
			err:  nats.ErrSlowConsumer,
		},
		{
			name: "unknown",
			err:  errors.New("unknown"),
		},
		{
			name: "shutdown near match",
			err:  errors.New("nats: Server Shutdown again"),
		},
		{
			name: "max waiting near match",
			err:  errors.New("nats: Exceeded MaxWaiting again"),
		},
		{
			name: "native timeout",
			err:  nats.ErrTimeout,
		},
		{
			name: "context timeout",
			err:  context.DeadlineExceeded,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := retryableFetchError(tt.err); got != tt.want {
				t.Fatalf("retryableFetchError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

type consumerInfoStub struct {
	nats.JetStreamContext
	errors []error
}

func (s *consumerInfoStub) ConsumerInfo(_, _ string, _ ...nats.JSOpt) (*nats.ConsumerInfo, error) {
	err := s.errors[0]
	s.errors = s.errors[1:]
	return nil, err
}

func TestReconnectProbeRetriesTemporaryLookupFailure(t *testing.T) {
	for _, temporary := range []error{nats.ErrTimeout, nats.ErrJetStreamNotEnabled, nats.ErrReconnectBufExceeded} {
		t.Run(temporary.Error(), func(t *testing.T) {
			consumer := &Consumer{
				config: ConsumerConfig{
					Stream:           "orders",
					Consumer:         "worker",
					OperationTimeout: time.Second,
				},
			}
			js := &consumerInfoStub{errors: []error{temporary, nats.ErrConsumerNotFound}}
			if err := consumer.probePullConsumer(context.Background(), js); !errors.Is(err, temporary) || !retryableLookupError(err) {
				t.Fatalf("temporary lookup: %v", err)
			}
			if err := consumer.probePullConsumer(context.Background(), js); !errors.Is(err, nats.ErrConsumerNotFound) || retryableLookupError(err) {
				t.Fatalf("terminal lookup: %v", err)
			}
		})
	}
}
