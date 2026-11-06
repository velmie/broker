package sqs_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"

	"github.com/velmie/broker"
	adapter "github.com/velmie/broker/sqs"
)

func TestServiceResultComposition(t *testing.T) {
	for _, tc := range []struct {
		name      string
		callback  func(context.Context) error
		wantCause bool
	}{
		{
			name:     "cooperative",
			callback: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		},
		{
			name:      "joined failure",
			callback:  func(ctx context.Context) error { <-ctx.Done(); return errors.Join(ctx.Err(), errServiceFailure) },
			wantCause: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parent, stop := context.WithCancel(context.Background())
			defer stop()
			entered := make(chan struct{})
			client, url := newWire(t, func(context.Context, string, request) (int, any) {
				return 200, map[string]any{"Messages": []any{native("{}", nil)}}
			})
			consumer := newConsumer(t, client, url, func(c *adapter.ConsumerConfig) { c.Shutdown.Mode = broker.ShutdownCancel })
			result := make(chan error, 1)
			go func() {
				result <- consumer.RunService(parent, broker.NewHandler(func(ctx context.Context, _ broker.Delivery) (broker.Disposition, error) {
					close(entered)
					return nil, tc.callback(ctx)
				}))
			}()
			await(t, entered)
			stop()
			err := await(t, result)
			if tc.wantCause && !errors.Is(err, errServiceFailure) {
				t.Fatalf("missing callback failure: %v", err)
			}
			if !tc.wantCause && err != nil {
				t.Fatalf("cooperative shutdown: %v", err)
			}
		})
	}
}

var errServiceFailure = errors.New("callback failure")

func TestServiceGraceExpiry(t *testing.T) {
	parent, stop := context.WithCancel(context.Background())
	defer stop()
	entered := make(chan struct{})
	client, url := newWire(t, func(context.Context, string, request) (int, any) {
		return 200, map[string]any{"Messages": []any{native("{}", nil)}}
	})
	consumer := newConsumer(t, client, url, func(c *adapter.ConsumerConfig) { c.Shutdown.GracePeriod = time.Millisecond })
	result := make(chan error, 1)
	go func() {
		result <- consumer.RunService(parent, broker.NewHandler(func(ctx context.Context, _ broker.Delivery) (broker.Disposition, error) {
			close(entered)
			<-ctx.Done()
			return nil, ctx.Err()
		}))
	}()
	await(t, entered)
	stop()
	if err := await(t, result); !errors.Is(err, adapter.ErrGracePeriodExceeded) {
		t.Fatalf("grace expiry: %v", err)
	}
}

func TestServiceForcedStopSuppressesUnadmittedDisposition(t *testing.T) {
	parent, stop := context.WithCancel(context.Background())
	defer stop()
	entered := make(chan struct{})
	client, url := newWire(t, func(context.Context, string, request) (int, any) {
		return 200, map[string]any{"Messages": []any{native("{}", nil)}}
	})
	consumer := newConsumer(t, client, url, func(c *adapter.ConsumerConfig) { c.Shutdown.Mode = broker.ShutdownCancel })
	result := make(chan error, 1)
	go func() {
		result <- consumer.RunService(parent, broker.NewHandler(func(ctx context.Context, _ broker.Delivery) (broker.Disposition, error) {
			close(entered)
			<-ctx.Done()
			return nil, nil
		}))
	}()
	await(t, entered)
	stop()
	if err := await(t, result); err != nil {
		t.Fatalf("suppressed disposition: %v", err)
	}
}

func TestServiceReceiveFailureConcurrentStop(t *testing.T) {
	parent, stop := context.WithCancel(context.Background())
	defer stop()
	client, url := newWire(t, func(context.Context, string, request) (int, any) {
		return 400, request{"__type": "QueueDoesNotExist", "message": "missing queue"}
	})
	consumer := newConsumer(t, client, url, func(c *adapter.ConsumerConfig) {
		c.Observer = &adapter.Observer{
			Record: func(_ context.Context, event adapter.Observation) {
				if event.Operation == adapter.OperationReceive {
					stop()
				}
			},
		}
	})
	err := consumer.RunService(parent, broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
		t.Error("unexpected callback")
		return broker.Handled{}, nil
	}))
	var apiError smithy.APIError
	if !errors.As(err, &apiError) || apiError.ErrorCode() != "QueueDoesNotExist" {
		t.Fatalf("SDK failure lost: %v", err)
	}
}

func TestServiceStopBeforeSDKRetryAttempt(t *testing.T) {
	parent, stop := context.WithCancel(context.Background())
	defer stop()
	client := awssqs.NewFromConfig(aws.Config{
		Region:      "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""),
	}, func(options *awssqs.Options) {
		options.BaseEndpoint = aws.String("http://localhost:1")
		options.APIOptions = append(options.APIOptions, func(stack *middleware.Stack) error {
			return stack.Finalize.Insert(middleware.FinalizeMiddlewareFunc("stop-before-retry", func(
				ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler,
			) (middleware.FinalizeOutput, middleware.Metadata, error) {
				stop()
				return next.HandleFinalize(ctx, in)
			}), "Retry", middleware.Before)
		})
	})
	consumer := newConsumer(t, client, "http://localhost:1/queue", nil)
	err := consumer.RunService(parent, broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
		t.Error("unexpected callback")
		return broker.Handled{}, nil
	}))
	if err != nil {
		t.Fatalf("stop before SDK attempt: %v", err)
	}
}

func TestServiceRetainsIndependentDeliveryCancellation(t *testing.T) {
	cause := errors.New("instrumentation deadline")
	client, url := newWire(t, func(context.Context, string, request) (int, any) {
		return 200, map[string]any{"Messages": []any{native("{}", nil)}}
	})
	consumer := newConsumer(t, client, url, func(c *adapter.ConsumerConfig) {
		c.Observer = &adapter.Observer{
			Begin: func(parent context.Context, _ adapter.DeliveryInfo) context.Context {
				ctx, cancel := context.WithCancelCause(parent)
				cancel(cause)
				return ctx
			},
		}
	})
	handler := broker.NewHandler(func(ctx context.Context, _ broker.Delivery) (broker.Disposition, error) {
		return nil, ctx.Err()
	})
	err := consumer.RunService(context.Background(), handler)
	if !errors.Is(err, cause) || !errors.Is(err, context.Canceled) {
		t.Fatalf("independent cancellation causes lost: %v", err)
	}
}

func TestServiceObserverPanicIncludesCauseText(t *testing.T) {
	cause := errors.New("observer failure details")
	parent, stop := context.WithCancel(context.Background())
	defer stop()
	client, url := newWire(t, func(context.Context, string, request) (int, any) {
		return 200, map[string]any{"Messages": []any{native("{}", nil)}}
	})
	consumer := newConsumer(t, client, url, func(c *adapter.ConsumerConfig) {
		c.Observer = &adapter.Observer{
			Begin: func(ctx context.Context, _ adapter.DeliveryInfo) context.Context { panic(cause) },
		}
	})
	err := consumer.RunService(parent, broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
		stop()
		return broker.Handled{}, nil
	}))
	if !errors.Is(err, cause) || !strings.Contains(err.Error(), cause.Error()) {
		t.Fatalf("observer cause missing from result: %v", err)
	}
}

func TestServiceStopDuringSDKRetryReleaseFailure(t *testing.T) {
	parent, stop := context.WithCancel(context.Background())
	defer stop()
	entered := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		close(entered)
		<-release
	}))
	defer func() {
		close(release)
		server.Close()
	}()
	client := awssqs.NewFromConfig(aws.Config{
		Region:      "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""),
		Retryer:     func() aws.Retryer { return failingReleaseRetryer{} },
	}, func(options *awssqs.Options) { options.BaseEndpoint = aws.String(server.URL) })
	consumer := newConsumer(t, client, server.URL+"/queue", nil)
	result := make(chan error, 1)
	go func() {
		result <- consumer.RunService(parent, broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
			t.Error("unexpected callback")
			return broker.Handled{}, nil
		}))
	}()
	await(t, entered)
	stop()
	if err := await(t, result); err != nil {
		t.Fatalf("canceled receive: %v", err)
	}
}

type failingReleaseRetryer struct {
	aws.NopRetryer
}

func (failingReleaseRetryer) GetAttemptToken(context.Context) (func(error) error, error) {
	return func(error) error { return errors.New("release failed") }, nil
}

func TestServiceStopDuringSDKReceive(t *testing.T) {
	parent, stop := context.WithCancel(context.Background())
	defer stop()
	entered := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		close(entered)
		<-release
	}))
	defer func() {
		close(release)
		server.Close()
	}()
	client := awssqs.NewFromConfig(aws.Config{
		Region:           "us-east-1",
		Credentials:      credentials.NewStaticCredentialsProvider("test", "test", ""),
		RetryMaxAttempts: 1,
	}, func(options *awssqs.Options) { options.BaseEndpoint = aws.String(server.URL) })
	consumer := newConsumer(t, client, server.URL+"/queue", nil)
	result := make(chan error, 1)
	go func() {
		result <- consumer.RunService(parent, broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
			t.Error("unexpected callback")
			return broker.Handled{}, nil
		}))
	}()
	await(t, entered)
	stop()
	if err := await(t, result); err != nil {
		t.Fatalf("stop during SDK receive: %v", err)
	}
}

func TestServiceStopDuringCredentialRetrieval(t *testing.T) {
	parent, stop := context.WithCancel(context.Background())
	defer stop()
	entered := make(chan struct{})
	client := awssqs.NewFromConfig(aws.Config{
		Region: "us-east-1",
		Credentials: aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
			close(entered)
			<-ctx.Done()
			return aws.Credentials{}, errors.Join(errors.New("credential provider failure"), ctx.Err())
		}),
		RetryMaxAttempts: 1,
	}, func(options *awssqs.Options) { options.BaseEndpoint = aws.String("http://localhost:1") })
	consumer := newConsumer(t, client, "http://localhost:1/queue", nil)
	result := make(chan error, 1)
	go func() {
		result <- consumer.RunService(parent, broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
			t.Error("unexpected callback")
			return broker.Handled{}, nil
		}))
	}()
	await(t, entered)
	stop()
	if err := await(t, result); err != nil {
		t.Fatalf("stop during credential retrieval: %v", err)
	}
}

func TestServiceStopDuringCachedCredentialRetrieval(t *testing.T) {
	parent, stop := context.WithCancel(context.Background())
	defer stop()
	entered := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	provider := aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
		close(entered)
		<-release
		return aws.Credentials{
			AccessKeyID:     "test",
			SecretAccessKey: "test",
		}, nil
	})
	client := awssqs.NewFromConfig(aws.Config{
		Region:           "us-east-1",
		Credentials:      aws.NewCredentialsCache(provider),
		RetryMaxAttempts: 1,
	}, func(options *awssqs.Options) { options.BaseEndpoint = aws.String("http://localhost:1") })
	consumer := newConsumer(t, client, "http://localhost:1/queue", nil)
	result := make(chan error, 1)
	go func() {
		result <- consumer.RunService(parent, broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
			t.Error("unexpected callback")
			return broker.Handled{}, nil
		}))
	}()
	await(t, entered)
	stop()
	if err := await(t, result); err != nil {
		t.Fatalf("stop during cached credential retrieval: %v", err)
	}
}

func TestServiceCredentialFailureWithoutStop(t *testing.T) {
	cause := errors.New("credentials unavailable")
	client := awssqs.NewFromConfig(aws.Config{
		Region: "us-east-1",
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{}, errors.Join(cause, context.Canceled)
		}),
		RetryMaxAttempts: 1,
	}, func(options *awssqs.Options) { options.BaseEndpoint = aws.String("http://localhost:1") })
	consumer := newConsumer(t, client, "http://localhost:1/queue", nil)
	handler := broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
		t.Error("unexpected callback")
		return broker.Handled{}, nil
	})
	err := consumer.RunService(context.Background(), handler)
	if !errors.Is(err, cause) || !errors.Is(err, context.Canceled) {
		t.Fatalf("credential failure lost: %v", err)
	}
}

func TestServiceReceiveTimeoutBeforeParentStop(t *testing.T) {
	parent, stop := context.WithCancel(context.Background())
	defer stop()
	client := awssqs.NewFromConfig(aws.Config{
		Region:      "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""),
	}, func(options *awssqs.Options) {
		options.BaseEndpoint = aws.String("http://localhost:1")
		options.APIOptions = append(options.APIOptions, func(stack *middleware.Stack) error {
			return stack.Finalize.Insert(middleware.FinalizeMiddlewareFunc("wait-for-receive-timeout", func(
				ctx context.Context, _ middleware.FinalizeInput, _ middleware.FinalizeHandler,
			) (middleware.FinalizeOutput, middleware.Metadata, error) {
				<-ctx.Done()
				stop()
				return middleware.FinalizeOutput{}, middleware.Metadata{}, ctx.Err()
			}), "Retry", middleware.Before)
		})
	})
	consumer := newConsumer(t, client, "http://localhost:1/queue", func(c *adapter.ConsumerConfig) {
		c.WaitTimeSeconds = 1
		c.ReceiveTimeout = 1100 * time.Millisecond
	})
	handler := broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
		t.Error("unexpected callback")
		return broker.Handled{}, nil
	})
	err := consumer.RunService(parent, handler)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("independent receive timeout lost: %v", err)
	}
}

func TestServiceForcedStopRetainsAdmittedRenewalFailure(t *testing.T) {
	parent, stop := context.WithCancel(context.Background())
	defer stop()
	entered := make(chan struct{})
	release := make(chan struct{})
	callbackCanceled := make(chan struct{})
	client, url := newWire(t, func(_ context.Context, operation string, _ request) (int, any) {
		if operation == "ChangeMessageVisibility" {
			close(entered)
			<-release
			return 400, request{"__type": "ReceiptHandleIsInvalid", "message": "renewal failed"}
		}
		return 200, map[string]any{"Messages": []any{native("{}", nil)}}
	})
	options := client.Options()
	options.Retryer = failingReleaseRetryer{}
	client = awssqs.New(options)
	consumer := newConsumer(t, client, url, func(c *adapter.ConsumerConfig) {
		c.Shutdown.Mode = broker.ShutdownCancel
		c.VisibilityTimeout = time.Second
	})
	result := make(chan error, 1)
	go func() {
		result <- consumer.RunService(parent, broker.NewHandler(func(ctx context.Context, _ broker.Delivery) (broker.Disposition, error) {
			<-ctx.Done()
			close(callbackCanceled)
			return nil, ctx.Err()
		}, broker.WithKeepAlive(time.Millisecond)))
	}()
	await(t, entered)
	stop()
	await(t, callbackCanceled)
	close(release)
	err := await(t, result)
	var apiError smithy.APIError
	if !errors.As(err, &apiError) || apiError.ErrorCode() != "ReceiptHandleIsInvalid" ||
		!strings.Contains(err.Error(), "failed to release initial token") {
		t.Fatalf("admitted renewal failure lost: %v", err)
	}
}
