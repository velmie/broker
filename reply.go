package broker

import (
	"context"
	"fmt"
)

// Encoder defines how to encode message data
type Encoder interface {
	Encode(v any) ([]byte, error)
}

// EncoderFunc wraps the decoding function to use it as a Encoder
// json.Marshal can be used as a encoder
type EncoderFunc func(v any) ([]byte, error)

func (f EncoderFunc) Encode(v any) ([]byte, error) {
	return f(v)
}

// CreateReplyHandler creates a handler that replies to topic specified by the message header with a message that
// contains the data returned by the passed consumerFunc
func CreateReplyHandler[REQ any, RESP any](
	dec Decoder,
	enc Encoder,
	pub Publisher,
	consumerFunc func(ctx context.Context, target REQ) (RESP, error),
	middleware ...Middleware,
) Handler {
	runConsumerFunc := func(ctx context.Context, event Event, target REQ) error {
		resp, err := consumerFunc(ctx, target)
		if err != nil {
			return err
		}
		reqMsg := event.Message()
		if replyTopic := reqMsg.Header.GetReplyTopic(); replyTopic != "" {
			msg := NewMessage()
			body, eErr := enc.Encode(resp)
			if eErr != nil {
				return fmt.Errorf("cannot encode message body: %w", eErr)
			}
			msg.Body = body
			msg.Header.SetReplyMessageID(reqMsg.ID)

			if pErr := pub.Publish(replyTopic, msg); pErr != nil {
				return fmt.Errorf("cannot publish message to the %q topic: %w", replyTopic, pErr)
			}
		}
		return nil
	}

	return createHandler(dec, runConsumerFunc, middleware...)
}
