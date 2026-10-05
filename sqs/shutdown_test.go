package sqs_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/velmie/broker"
	adapter "github.com/velmie/broker/sqs"
)

func TestSDKShutdownPoliciesAndPrefetchedAdmission(t *testing.T) {
	for _, mode := range []string{"graceful", "cancel", "grace expiry"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			entered, release := make(chan struct{}), make(chan struct{})
			var deletes atomic.Int32
			var callbacks atomic.Int32
			client, url := newWire(t, func(_ context.Context, op string, _ request) (int, any) {
				if op == "DeleteMessage" {
					deletes.Add(1)
					return 200, request{}
				}
				return 200, map[string]any{"Messages": []any{native("{}", nil), native("{}", nil)}}
			})
			consumer := newConsumer(t, client, url, func(c *adapter.ConsumerConfig) {
				c.BatchSize = 2
				if mode == "cancel" {
					c.Shutdown.Mode = broker.ShutdownCancel
				}
				if mode == "grace expiry" {
					c.Shutdown.GracePeriod = time.Millisecond
				}
			})
			stopped := errors.New("owner stopped")
			done := make(chan error, 1)
			go func() {
				done <- consumer.Run(ctx, broker.NewHandler(func(cbCtx context.Context, _ broker.Delivery) (broker.Disposition, error) {
					callbacks.Add(1)
					close(entered)
					if mode == "graceful" {
						<-release
						if cbCtx.Err() != nil {
							t.Error("graceful callback canceled")
						}
					} else {
						<-cbCtx.Done()
						if !errors.Is(context.Cause(cbCtx), stopped) {
							t.Error("cancellation cause lost")
						}
					}
					return broker.Handled{}, nil
				}))
			}()
			await(t, entered)
			cancel(stopped)
			if mode == "graceful" {
				select {
				case <-done:
					t.Fatal("Run did not join callback")
				default:
				}
				close(release)
			}
			if err := await(t, done); !errors.Is(err, stopped) {
				t.Fatalf("run %v", err)
			}
			expected := int32(0)
			if mode == "graceful" {
				expected = 1
			}
			if deletes.Load() != expected || callbacks.Load() != 1 {
				t.Fatalf("deletes %d callbacks %d", deletes.Load(), callbacks.Load())
			}
		})
	}
}

func TestSDKAdmittedDeleteSurvivesForcedStopAndJoins(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered, release := make(chan struct{}), make(chan struct{})
	var receives atomic.Int32
	client, url := newWire(t, func(requestCtx context.Context, op string, _ request) (int, any) {
		if op == "DeleteMessage" {
			close(entered)
			select {
			case <-release:
			case <-requestCtx.Done():
				t.Error("admitted delete canceled by lifecycle")
			}
			return 200, request{}
		}
		receives.Add(1)
		return 200, map[string]any{"Messages": []any{native("{}", nil)}}
	})
	consumer := newConsumer(t, client, url, func(c *adapter.ConsumerConfig) { c.Shutdown.Mode = broker.ShutdownCancel })
	done := make(chan error, 1)
	go func() {
		done <- consumer.Run(ctx,
			broker.NewHandler(func(context.Context,
				broker.Delivery) (broker.Disposition,
				error) {
				return broker.Handled{},
					nil
			}))
	}()
	await(t, entered)
	cancel()
	select {
	case err := <-done:
		t.Fatalf("returned before delete joined: %v", err)
	default:
	}
	close(release)
	if err := await(t, done); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if receives.Load() != 1 {
		t.Fatal("receive after stop")
	}
}

func TestSDKConcurrentRunRejectedAndReceiveCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered, finished := make(chan struct{}), make(chan struct{})
	client, url := newWire(t, func(requestCtx context.Context, _ string, _ request) (int, any) {
		close(entered)
		<-requestCtx.Done()
		close(finished)
		return 200, request{}
	})
	consumer := newConsumer(t, client, url, nil)
	handler := broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
		t.Error("unexpected callback")
		return broker.Handled{}, nil
	})
	done := make(chan error, 1)
	go func() { done <- consumer.Run(ctx, handler) }()
	await(t, entered)
	if err := consumer.Run(ctx, handler); err == nil {
		t.Fatal("concurrent run allowed")
	}
	cancel()
	if err := await(t, done); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	await(t, finished)
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestSDKNetworkCausePreservedAndPublicationCancellation(t *testing.T) {
	marker := errors.New("network failure")
	client := awssqs.NewFromConfig(aws.Config{Region: "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider("synthetic",
			"synthetic",
			""),
		RetryMaxAttempts: 1,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response,
			error) {
			return nil,
				marker
		})}})
	publisher, err := adapter.NewPublisher(client, adapter.PublisherConfig{QueueURL: "http://localhost/queue", Source: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	if err = publisher.Publish(context.Background(), broker.Message{Body: []byte("ok")}); !errors.Is(err, marker) {
		t.Fatalf("original network cause lost: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err = publisher.Publish(ctx, broker.Message{Body: []byte("ok")}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled publication %v", err)
	}
	consumer := newConsumer(t, client, "http://localhost/queue", nil)
	if err = consumer.Run(context.Background(), broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
		t.Error("callback")
		return broker.Handled{}, nil
	})); !errors.Is(err, marker) {
		t.Fatal(err)
	}
}

func TestSDKDeleteFailureUnknownAndObserverFailureDoesNotReplay(t *testing.T) {
	observerCause := errors.New("observer failure")
	for _, deleteFailure := range []bool{false, true} {
		t.Run(map[bool]string{false: "accepted", true: "unknown"}[deleteFailure], func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var callbacks, deletes atomic.Int32
			var recorded adapter.Observation
			client, url := newWire(t, func(_ context.Context, op string, _ request) (int, any) {
				if op == "DeleteMessage" {
					deletes.Add(1)
					cancel()
					if deleteFailure {
						return 500, request{"__type": "InternalError", "message": "service marker"}
					}
					return 200, request{}
				}
				return 200, map[string]any{"Messages": []any{native("{}", nil)}}
			})
			consumer := newConsumer(t, client, url, func(c *adapter.ConsumerConfig) {
				c.Observer = &adapter.Observer{Record: func(_ context.Context, o adapter.Observation) {
					if o.Operation == adapter.OperationDelete {
						recorded = o
						panic(observerCause)
					}
				}}
			})
			err := consumer.Run(ctx, broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
				callbacks.Add(1)
				return broker.Handled{}, nil
			}))
			if !errors.Is(err, observerCause) || !errors.Is(err, context.Canceled) || callbacks.Load() != 1 || deletes.Load() != 1 {
				t.Fatalf("err=%v callbacks=%d deletes=%d", err, callbacks.Load(), deletes.Load())
			}
			expected := adapter.OutcomeAccepted
			if deleteFailure {
				expected = adapter.OutcomeUnknown
				if recorded.Err == nil {
					t.Fatal("original SDK cause missing")
				}
			}
			if recorded.Outcome != expected {
				t.Errorf("outcome %s", recorded.Outcome)
			}
		})
	}
}

func TestSDKLateReceiveCannotStartCallback(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered, release := make(chan struct{}), make(chan struct{})
	client := awssqs.NewFromConfig(aws.Config{Region: "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider("synthetic",
			"synthetic",
			""),
		RetryMaxAttempts: 1,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response,
			error) {
			close(entered)
			<-release
			body := `{"Messages":[{"Body":"{}","MessageId":"native","ReceiptHandle":"receipt"}]}`
			return &http.Response{StatusCode: http.StatusOK,
					Header: http.Header{"Content-Type": []string{"application/x-amz-json-1.0"}},
					Body:   io.NopCloser(strings.NewReader(body))},
				nil
		})}}, func(o *awssqs.Options) { o.DisableMessageChecksumValidation = true })
	consumer := newConsumer(t, client, "http://localhost/queue", nil)
	done := make(chan error, 1)
	go func() {
		done <- consumer.Run(ctx, broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
			t.Error("late receive admitted callback")
			return broker.Handled{}, nil
		}))
	}()
	await(t, entered)
	cancel()
	select {
	case <-done:
		t.Fatal("did not join receive")
	default:
	}
	close(release)
	if err := await(t, done); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestSDKOperationBudgets(t *testing.T) {
	for _, operation := range []string{"SendMessage", "DeleteMessage"} {
		t.Run(operation, func(t *testing.T) {
			client, url := newWire(t, func(ctx context.Context, op string, _ request) (int, any) {
				if op == operation {
					<-ctx.Done()
					return 200, request{}
				}
				return 200, map[string]any{"Messages": []any{native("{}", nil)}}
			})
			var err error
			if operation == "SendMessage" {
				publisher,
					constructorErr := adapter.NewPublisher(client,
					adapter.PublisherConfig{QueueURL: url,
						Source:           "orders",
						OperationTimeout: time.Millisecond})
				if constructorErr != nil {
					t.Fatal(constructorErr)
				}
				err = publisher.Publish(context.Background(), broker.Message{Body: []byte("body")})
			} else {
				consumer := newConsumer(t, client, url, func(c *adapter.ConsumerConfig) { c.OperationTimeout = time.Millisecond })
				err = consumer.Run(context.Background(),
					broker.NewHandler(func(context.Context,
						broker.Delivery) (broker.Disposition,
						error) {
						return broker.Handled{},
							nil
					}))
			}
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("operation did not retain budget expiry: %v", err)
			}
		})
	}
}
