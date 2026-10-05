package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/velmie/broker"
	"github.com/velmie/broker/natsjs/v3"
)

func TestBoundPublisherNativeExpectations(t *testing.T) {
	nc := nativeConnection(t)
	js, err := nc.JetStream(nats.MaxWait(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	for _, rejectedHeader := range []string{"", nats.ExpectedStreamHdr, nats.ExpectedLastSeqHdr,
		nats.ExpectedLastSubjSeqHdr, nats.ExpectedLastMsgIdHdr} {
		t.Run(rejectedHeader, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			stream, cleanup, createErr := newStream(ctx, js)
			if createErr != nil {
				t.Fatal(createErr)
			}
			defer func() {
				if cleanupErr := cleanup(); cleanupErr != nil {
					t.Error(cleanupErr)
				}
			}()
			subject := stream + ".orders"
			if _, publishErr := js.PublishMsg(&nats.Msg{Subject: subject, Data: []byte("seed"),
				Header: nats.Header{nats.MsgIdHdr: {"seed"}}}, nats.Context(ctx)); publishErr != nil {
				t.Fatal(publishErr)
			}
			publisher, publisherErr := natsjs.NewPublisher(nc, natsjs.PublisherConfig{Subject: subject})
			if publisherErr != nil {
				t.Fatal(publisherErr)
			}
			headers := []broker.Header{
				{Name: nats.ExpectedStreamHdr, Value: []byte(stream)},
				{Name: nats.ExpectedLastSeqHdr, Value: []byte("1")},
				{Name: nats.ExpectedLastSubjSeqHdr, Value: []byte("1")},
				{Name: nats.ExpectedLastMsgIdHdr, Value: []byte("seed")},
			}
			for index := range headers {
				if headers[index].Name == rejectedHeader {
					headers[index].Value = []byte("999")
				}
			}
			publishErr := publisher.Publish(ctx, broker.Message{ID: "candidate", Body: []byte("candidate"), Headers: headers})
			wantMessages := uint64(2)
			if rejectedHeader == "" {
				if publishErr != nil {
					t.Fatal(publishErr)
				}
			} else {
				var api *nats.APIError
				var stage *broker.StageError
				if !errors.As(publishErr, &api) || !errors.As(publishErr, &stage) || stage.Stage != broker.StagePublish {
					t.Fatalf("native expectation rejection lost its cause or stage: %v", publishErr)
				}
				wantMessages = 1
			}
			info, infoErr := js.StreamInfo(stream, nats.Context(ctx))
			if infoErr != nil || info.State.Msgs != wantMessages {
				t.Fatalf("expectation acceptance readback: %+v, %v", info, infoErr)
			}
		})
	}
}
