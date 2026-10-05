package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestNativePolicies(t *testing.T) {
	connection := nativeConnection(t)
	ctx, cancel := context.WithTimeout(context.Background(), 70*time.Second)
	defer cancel()
	if err := runPolicies(ctx, connection); err != nil {
		t.Fatal(err)
	}
	if connection.IsClosed() {
		t.Fatal("policy profile closed borrowed connection")
	}
}

func TestNativeCumulativeFailureRemainsUnconfirmed(t *testing.T) {
	connection := nativeConnection(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	js, err := connection.JetStream(nats.MaxWait(operationTimeout))
	if err != nil {
		t.Fatal(err)
	}
	stream, cleanup, err := newStream(ctx, js)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if cleanupErr := cleanup(); cleanupErr != nil {
			t.Error(cleanupErr)
		}
	}()
	sub, err := js.SubscribeSync(stream+".events", nats.BindStream(stream), nats.AckAll(), nats.AckWait(10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if unsubscribeErr := sub.Unsubscribe(); unsubscribeErr != nil {
			t.Error(unsubscribeErr)
		}
	}()
	var batch []*nats.Msg
	for i := 0; i < 3; i++ {
		if _, err = js.Publish(stream+".events", []byte("effect"), nats.Context(ctx)); err != nil {
			t.Fatal(err)
		}
		message, receiveErr := sub.NextMsgWithContext(ctx)
		if receiveErr != nil {
			t.Fatal(receiveErr)
		}
		batch = append(batch, message)
	}
	sentinel := errors.New("effect unavailable")
	effects := 0
	err = policyConfirmBatch(ctx, batch, func(*nats.Msg) error {
		effects++
		if effects == 2 {
			return sentinel
		}
		return nil
	})
	if !errors.Is(err, sentinel) || effects != 2 {
		t.Fatalf("original failure or sequential stop lost: effects=%d err=%v", effects, err)
	}
	info, err := sub.ConsumerInfo()
	if err != nil {
		t.Fatal(err)
	}
	if info.AckFloor.Stream != 0 || info.NumAckPending != 3 {
		t.Fatalf("failed cumulative batch was confirmed: floor=%d pending=%d", info.AckFloor.Stream, info.NumAckPending)
	}
}
