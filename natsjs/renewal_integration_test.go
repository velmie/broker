package natsjs_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/velmie/broker"
	"github.com/velmie/broker/natsjs/v3"
)

func TestCallbackReturnJoinsActiveRenewalWithoutCancelingIt(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprintf("callback_failure_%v", fail), func(t *testing.T) {
			nc, js, config := provision(t, testTimeout)
			config.OperationTimeout = 2 * time.Second
			ctx, stop := context.WithCancel(context.Background())
			defer stop()
			admitted, send, release, processed := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
			var renewalConfirmed, terminalConfirmed int
			config.Observer.Record = func(_ context.Context, event natsjs.Observation) {
				if event.Operation == natsjs.OperationRenew && event.Outcome == natsjs.OutcomeStarted {
					close(admitted)
					<-send
				}
				if event.Operation == natsjs.OperationRenew && event.Outcome == natsjs.OutcomeConfirmed {
					renewalConfirmed++
				}
				if event.Operation == natsjs.OperationProcess && event.Outcome != natsjs.OutcomeStarted {
					close(processed)
				}
				if event.Operation == natsjs.OperationAck && event.Outcome == natsjs.OutcomeConfirmed {
					terminalConfirmed++
					stop()
				}
			}
			cause := errors.New("application failure")
			h := broker.NewTypedHandler(broker.DecodeJSON[struct{}], func(context.Context, struct{}) error {
				<-release
				if fail {
					return cause
				}
				return nil
			},
				broker.WithKeepAlive(50*time.Millisecond),
				broker.WithKeepAlive(50*time.Millisecond), //nolint:gocritic // One worker for equal requirements.
			)
			result := startConsumer(t, nc, js, config, ctx, h, `{}`)
			receive(t, admitted)
			resume := pauseServer(t)
			close(send)
			close(release)
			receive(t, processed)
			select {
			case err := <-result:
				t.Fatalf("returned with renewal active: %v", err)
			default:
			}
			resume()
			err := receive(t, result)
			expectedCause := context.Canceled
			expectedTerminals := 1
			if fail {
				expectedCause = cause
				expectedTerminals = 0
			}
			if !errors.Is(err, expectedCause) {
				t.Fatalf("result: %v", err)
			}
			if renewalConfirmed != 1 {
				t.Fatalf("renewal confirmations=%d", renewalConfirmed)
			}
			if terminalConfirmed != expectedTerminals {
				t.Fatalf("terminal confirmations=%d", terminalConfirmed)
			}
		})
	}
}

func TestRenewalFailureCancelsWorkAndSuppressesAllResults(t *testing.T) {
	for _, disposition := range terminalResults() {
		t.Run(disposition.name, func(t *testing.T) {
			nc, js, config := provision(t, testTimeout)
			config.OperationTimeout = 200 * time.Millisecond
			admitted, send := make(chan struct{}), make(chan struct{})
			canceled := make(chan error, 1)
			var terminals int
			config.Observer.Record = func(_ context.Context, event natsjs.Observation) {
				if event.Operation == natsjs.OperationRenew && event.Outcome == natsjs.OutcomeStarted {
					close(admitted)
					<-send
				}
				if isTerminal(event.Operation) && event.Outcome == natsjs.OutcomeStarted {
					terminals++
				}
			}
			callbackCause := errors.New("independent callback failure")
			h := broker.NewHandler(func(work context.Context, _ broker.Delivery) (broker.Disposition, error) {
				<-work.Done()
				canceled <- context.Cause(work)
				if disposition.name == "handled" {
					return disposition.value, callbackCause
				}
				return disposition.value, nil
			}, broker.WithKeepAlive(20*time.Millisecond), broker.WithRequirements(broker.RedeliveryRequirement{}, natsjs.TerminationRequirement{}))
			result := startConsumer(t, nc, js, config, context.Background(), h, `{}`)
			receive(t, admitted)
			resume := pauseServer(t)
			close(send)
			if cause := receive(t, canceled); !errors.Is(cause, context.DeadlineExceeded) {
				t.Fatalf("work cancellation lost renewal cause: %v", cause)
			}
			err := receive(t, result)
			resume()
			if !errors.Is(err, context.DeadlineExceeded) || terminals != 0 {
				t.Fatalf("result=%v terminals=%d", err, terminals)
			}
			if disposition.name == "handled" && !errors.Is(err, callbackCause) {
				t.Fatalf("independent callback cause lost: %v", err)
			}
			assertPending(t, js, config, 1)
		})
	}
}

func TestForcedRenewalCancellationKeepsShutdownCause(t *testing.T) {
	nc, js, config := provision(t, testTimeout)
	config.Shutdown.Mode = broker.ShutdownCancel
	ctx, stop := context.WithCancelCause(context.Background())
	defer stop(context.Canceled)
	admitted, send := make(chan struct{}), make(chan struct{})
	var renewalOutcome string
	config.Observer.Record = func(_ context.Context, event natsjs.Observation) {
		if event.Operation != natsjs.OperationRenew {
			return
		}
		if event.Outcome == natsjs.OutcomeStarted {
			close(admitted)
			<-send
		} else {
			renewalOutcome = event.Outcome
		}
	}
	h := broker.NewTypedHandler(broker.DecodeJSON[struct{}], func(work context.Context, _ struct{}) error {
		<-work.Done()
		return nil
	}, broker.WithKeepAlive(20*time.Millisecond))
	result := startConsumer(t, nc, js, config, ctx, h, `{}`)
	receive(t, admitted)
	resume := pauseServer(t)
	cause := errors.New("owner requested shutdown")
	stop(cause)
	close(send)
	err := receive(t, result)
	resume()
	if !errors.Is(err, cause) || errors.Is(err, context.DeadlineExceeded) || renewalOutcome != natsjs.OutcomeCanceled {
		t.Fatalf("shutdown attribution: %v, renewal=%s", err, renewalOutcome)
	}
	assertPending(t, js, config, 1)
}

func TestTerminalTimeoutIsUnknownWithoutReplayOrOppositeAction(t *testing.T) {
	for _, disposition := range terminalResults() {
		t.Run(disposition.name, func(t *testing.T) {
			nc, js, config := provision(t, testTimeout)
			config.OperationTimeout = 200 * time.Millisecond
			admitted, send := make(chan struct{}), make(chan struct{})
			var calls, commands, unknown int
			var log bytes.Buffer
			config.Observer.Record = func(_ context.Context, event natsjs.Observation) {
				if !isTerminal(event.Operation) {
					return
				}
				if event.Outcome == natsjs.OutcomeStarted {
					commands++
					close(admitted)
					<-send
				}
				if event.Outcome == natsjs.OutcomeUnknown {
					unknown++
				}
			}
			h := broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
				calls++
				return disposition.value, nil
			},
				broker.WithRequirements(broker.RedeliveryRequirement{}, natsjs.TerminationRequirement{})).WithMiddleware(
				broker.LogProcessing(slog.New(slog.NewJSONHandler(&log, nil)), broker.ProcessingLogConfig{LogSuccess: true}),
			)
			result := startConsumer(t, nc, js, config, context.Background(), h, `{}`)
			receive(t, admitted)
			resume := pauseServer(t)
			close(send)
			err := receive(t, result)
			resume()
			if !errors.Is(err, context.DeadlineExceeded) || calls != 1 || commands != 1 || unknown != 1 {
				t.Fatalf("unknown outcome: %v calls=%d commands=%d unknown=%d", err, calls, commands, unknown)
			}
			var record map[string]any
			if decodeErr := json.Unmarshal(log.Bytes(), &record); decodeErr != nil {
				t.Fatalf("expected one processing record: %v", decodeErr)
			}
			outcome := map[string]string{"handled": "handled", "retry": "retry_requested", "terminate": "returned"}[disposition.name]
			if record["operation"] != "process" || record["outcome"] != outcome {
				t.Fatalf("processing log claimed transport outcome: %s", log.String())
			}
			// The timed-out request may still reach the server. Readback proves
			// why the adapter must not claim rollback or issue an opposite action.
			if disposition.name != "retry" {
				awaitReadback(t, func() error {
					info, err := js.ConsumerInfo(config.Stream, config.Consumer)
					if err != nil {
						return err
					}
					if info.NumAckPending != 0 {
						return fmt.Errorf("pending=%d", info.NumAckPending)
					}
					return nil
				})
			} else {
				sub, err := js.PullSubscribe(config.Subject, config.Consumer, nats.Bind(config.Stream, config.Consumer))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if cleanupErr := sub.Unsubscribe(); cleanupErr != nil {
						t.Error(cleanupErr)
					}
				})
				messages, err := sub.Fetch(1, nats.MaxWait(2*time.Second))
				if err != nil {
					t.Fatalf("retry readback: %v", err)
				}
				metadata, err := messages[0].Metadata()
				if err != nil || metadata.NumDelivered != 2 {
					t.Fatalf("redelivery metadata: %+v, %v", metadata, err)
				}
			}
		})
	}
}
