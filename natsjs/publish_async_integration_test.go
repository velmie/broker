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

func TestEncodedAsyncFuturesRetainIndividualOutcomes(t *testing.T) {
	nc, readback, config := provision(t, time.Second)
	js, err := nc.JetStream(nats.PublishAsyncTimeout(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer js.CleanupPublisher()
	original := broker.Message{ID: "success", Body: []byte("body"), Headers: []broker.Header{{Name: "X-Tag", Value: []byte("value")}}}
	native, err := natsjs.EncodeMessage(config.Subject, original)
	if err != nil {
		t.Fatal(err)
	}
	success, err := js.PublishMsgAsync(native)
	if err != nil {
		t.Fatal(err)
	}
	original.Body[0] = 'B'
	original.Headers[0].Value[0] = 'V'
	rejected, err := natsjs.EncodeMessage(config.Subject, broker.Message{ID: "rejected", Body: []byte("rejected")})
	if err != nil {
		t.Fatal(err)
	}
	failure, err := js.PublishMsgAsync(rejected, nats.ExpectStream("wrong-stream"))
	if err != nil {
		t.Fatal(err)
	}
	receive(t, js.PublishAsyncComplete())
	if js.PublishAsyncPending() != 0 {
		t.Fatal("completed publisher retains pending futures")
	}
	ack, err := awaitEncodedFuture(t, success)
	if err != nil || ack.Stream != config.Stream || ack.Sequence != 1 {
		t.Fatalf("success ACK=%+v err=%v", ack, err)
	}
	_, err = awaitEncodedFuture(t, failure)
	var api *nats.APIError
	if !errors.As(err, &api) || api.ErrorCode != 10060 {
		t.Fatalf("completion hid native failure: %v", err)
	}
	stored, err := readback.GetMsg(config.Stream, ack.Sequence)
	if err != nil {
		t.Fatal(err)
	}
	if string(stored.Data) != "body" || stored.Header.Get("X-Tag") != "value" {
		t.Fatalf("detached async message changed: %+v", stored)
	}
	info, err := readback.StreamInfo(config.Stream)
	if err != nil || info.State.Msgs != 1 {
		t.Fatalf("rejected publication stored: %+v %v", info, err)
	}
}

func TestEncodedAsyncTimeoutDoesNotProveAbsenceOfStorage(t *testing.T) {
	nc, readback, config := provision(t, time.Second)
	makeStreamSilent(t, readback, config.Stream)
	js, err := nc.JetStream(nats.PublishAsyncTimeout(150 * time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer js.CleanupPublisher()
	message, err := natsjs.EncodeMessage(config.Subject, broker.Message{ID: "stored", Body: []byte("event")})
	if err != nil {
		t.Fatal(err)
	}
	future, err := js.PublishMsgAsync(message)
	if err != nil {
		t.Fatal(err)
	}
	canceled, stop := context.WithCancel(context.Background())
	stop()
	select {
	case <-canceled.Done():
	case <-future.Ok():
		t.Fatal("unexpected ACK from NoAck stream")
	}
	_, err = awaitEncodedFuture(t, future)
	if !errors.Is(err, nats.ErrAsyncPublishTimeout) {
		t.Fatalf("native timeout cause lost: %v", err)
	}
	receive(t, js.PublishAsyncComplete())
	info, err := readback.StreamInfo(config.Stream)
	if err != nil || info.State.Msgs != 1 {
		t.Fatalf("timed-out publication storage: %+v %v", info, err)
	}
	if err = nc.Flush(); err != nil {
		t.Fatal(err)
	}
}

func TestEncodedAsyncCleanupIsolatesPublisherContexts(t *testing.T) {
	nc, _, config := provision(t, time.Second)
	subject := nats.NewInbox()
	observed, err := nc.SubscribeSync(subject)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := observed.Unsubscribe(); closeErr != nil {
			t.Error(closeErr)
		}
	})
	if err = nc.Flush(); err != nil {
		t.Fatal(err)
	}
	first, err := nc.JetStream(nats.PublishAsyncMaxPending(1))
	if err != nil {
		t.Fatal(err)
	}
	defer first.CleanupPublisher()
	second, err := nc.JetStream(nats.PublishAsyncMaxPending(1))
	if err != nil {
		t.Fatal(err)
	}
	defer second.CleanupPublisher()
	message, err := natsjs.EncodeMessage(subject, broker.Message{Body: []byte("first")})
	if err != nil {
		t.Fatal(err)
	}
	abandoned, err := first.PublishMsgAsync(message)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = observed.NextMsg(testTimeout); err != nil {
		t.Fatal(err)
	}
	message, err = natsjs.EncodeMessage(subject, broker.Message{Body: []byte("second")})
	if err != nil {
		t.Fatal(err)
	}
	remaining, err := second.PublishMsgAsync(message)
	if err != nil {
		t.Fatal(err)
	}
	request, err := observed.NextMsg(testTimeout)
	if err != nil {
		t.Fatal(err)
	}
	assertAsyncBackpressure(t, first, subject)
	first.CleanupPublisher()
	_, err = awaitEncodedFuture(t, abandoned)
	if !errors.Is(err, nats.ErrJetStreamPublisherClosed) {
		t.Fatalf("cleanup cause: %v", err)
	}
	if first.PublishAsyncPending() != 0 || second.PublishAsyncPending() != 1 {
		t.Fatal("cleanup affected another publisher")
	}
	// This test-owned responder holds an ACK to exercise independent SDK sessions.
	if err = request.Respond([]byte(`{"stream":"CONTROLLED","seq":1}`)); err != nil {
		t.Fatal(err)
	}
	ack, err := awaitEncodedFuture(t, remaining)
	if err != nil || ack.Stream != "CONTROLLED" || ack.Sequence != 1 {
		t.Fatalf("remaining future=%+v err=%v", ack, err)
	}
	assertAsyncConnectionUsable(t, second, config.Subject, config.Stream)
}

func awaitEncodedFuture(t *testing.T, future nats.PubAckFuture) (*nats.PubAck, error) {
	t.Helper()
	select {
	case ack := <-future.Ok():
		return ack, nil
	case err := <-future.Err():
		return nil, err
	case <-time.After(testTimeout):
		t.Fatal("future did not resolve")
	}
	return nil, nil
}

func makeStreamSilent(t *testing.T, js nats.JetStreamContext, stream string) {
	t.Helper()
	info, err := js.StreamInfo(stream)
	if err != nil {
		t.Fatal(err)
	}
	info.Config.NoAck = true
	if _, err = js.UpdateStream(&info.Config); err != nil {
		t.Fatal(err)
	}
}

func assertAsyncConnectionUsable(t *testing.T, js nats.JetStreamContext, subject, stream string) {
	t.Helper()
	actual, err := natsjs.EncodeMessage(subject, broker.Message{Body: []byte("real-stream")})
	if err != nil {
		t.Fatal(err)
	}
	future, err := js.PublishMsgAsync(actual)
	if err != nil {
		t.Fatal(err)
	}
	ack, err := awaitEncodedFuture(t, future)
	if err != nil || ack.Stream != stream {
		t.Fatalf("borrowed connection/session unusable: %+v %v", ack, err)
	}
}

func assertAsyncBackpressure(t *testing.T, js nats.JetStreamContext, subject string) {
	t.Helper()
	stalled, err := natsjs.EncodeMessage(subject, broker.Message{Body: []byte("stalled")})
	if err != nil {
		t.Fatal(err)
	}
	_, err = js.PublishMsgAsync(stalled, nats.StallWait(20*time.Millisecond))
	if !errors.Is(err, nats.ErrTooManyStalledMsgs) {
		t.Fatalf("backpressure cause: %v", err)
	}
}
