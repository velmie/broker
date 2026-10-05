package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/velmie/broker"
	adapter "github.com/velmie/broker/azuresb"
)

func TestNativePersistentStartupFailureIsBoundedAndJoined(t *testing.T) {
	var dials, created, closed atomic.Int32
	client, err := azservicebus.NewClientFromConnectionString(testConnection, &azservicebus.ClientOptions{
		RetryOptions: azservicebus.RetryOptions{MaxRetries: -1},
		NewWebSocketConn: func(context.Context, azservicebus.NewWebSocketConnArgs) (net.Conn, error) {
			dials.Add(1)
			return nil, io.EOF
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if closeErr := client.Close(context.Background()); closeErr != nil {
			t.Error(closeErr)
		}
	}()
	factory := func() (adapter.Receiver, error) {
		if created.Load() != closed.Load() {
			t.Error("replacement preceded receiver close")
		}
		receiver, createErr := client.NewReceiverForQueue("queue.proof", nil)
		if createErr != nil {
			return nil, createErr
		}
		created.Add(1)
		return &countedReceiver{Receiver: receiver, closed: &closed}, nil
	}
	provider := sdktrace.NewTracerProvider()
	defer func() { _ = provider.Shutdown(context.Background()) }()
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{ReplaceAttr: redactAttribute}))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = consume(ctx, factory, &options{id: "order-test", receiveMode: peekLockMode}, logger, provider.Tracer("test"),
		func(context.Context, broker.Delivery, order) error { t.Error("callback on failed startup"); return nil })
	var native *azservicebus.Error
	if !errors.As(err, &native) || native.Code != azservicebus.CodeConnectionLost {
		t.Fatalf("missing original SDK connection failure: %v", err)
	}
	if created.Load() != 3 || closed.Load() != 3 || dials.Load() != 3 {
		t.Fatalf("generations/dials/close: %d/%d/%d", created.Load(), dials.Load(), closed.Load())
	}
}

type countedReceiver struct {
	*azservicebus.Receiver
	closed *atomic.Int32
}

func (r *countedReceiver) Close(ctx context.Context) error {
	err := r.Receiver.Close(ctx)
	r.closed.Add(1)
	return err
}

const testConnection = "Endpoint=sb://127.0.0.1:15672;SharedAccessKeyName=RootManageSharedAccessKey;" +
	"SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;"

func TestStartupClassifierRejectsEveryUnsafeBranchAndPostEntry(t *testing.T) {
	connection := &azservicebus.Error{Code: azservicebus.CodeConnectionLost}
	receive := &adapter.OperationError{Source: sourceName, Operation: adapter.OperationReceive, Cause: connection}
	if !retryStartup(context.Background(), false, receive) {
		t.Fatal("startup connection failure rejected")
	}
	if retryStartup(context.Background(), true, receive) {
		t.Fatal("post-entry recovery allowed")
	}
	stopped, cancel := context.WithCancel(context.Background())
	cancel()
	if retryStartup(stopped, false, receive) {
		t.Fatal("owner cancellation allowed recovery")
	}
	for _, failure := range []error{
		connection, context.DeadlineExceeded, errors.Join(receive, context.Canceled), errors.Join(receive, errors.New("independent")),
		errors.Join(receive, &adapter.OperationError{Source: sourceName, Operation: adapter.OperationClose, Cause: connection}),
		&adapter.OperationError{Source: sourceName, Operation: adapter.OperationComplete, Cause: connection},
		&adapter.OperationError{Source: sourceName, Operation: adapter.OperationProcess, Cause: connection},
		&adapter.OperationError{Source: sourceName, Operation: adapter.OperationAcquire, Cause: connection},
		&adapter.OperationError{Source: sourceName, Operation: adapter.OperationReceive, Cause: errors.Join(connection, io.EOF)},
		&adapter.OperationError{Operation: adapter.OperationReceive,
			Cause: errors.Join(connection, &adapter.OperationError{Operation: adapter.OperationClose, Cause: connection})},
		errors.Join(receive, &adapter.ObserverPanicError{Value: connection}),
	} {
		if retryStartup(context.Background(), false, failure) {
			t.Fatalf("unsafe recovery: %T", failure)
		}
	}
}
