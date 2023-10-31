package broker

import (
	"context"
	"fmt"
)

//go:generate go run go.uber.org/mock/mockgen@v0.3.0 -source decoder.go -destination ./mock/decoder.go

// Decoder defines how to decode the given data into a value
type Decoder interface {
	Decode(data []byte, v any) error
}

// DecoderFunc wraps the decoding function to use it as a Decoder
// json.Unmarshal can be used as a decoder
type DecoderFunc func(data []byte, v any) error

func (f DecoderFunc) Decode(data []byte, v any) error {
	return f(data, v)
}

// CreateHandler creates an event handler that uses a Decoder to decode event data into
// a concrete value, which is then passed to the consumer function
func CreateHandler[T any](
	dec Decoder,
	consumerFunc func(ctx context.Context, target T) error,
	middleware ...Middleware,
) Handler {
	runConsumerFunc := func(ctx context.Context, _ Event, target T) error {
		return consumerFunc(ctx, target)
	}

	return createHandler(dec, runConsumerFunc, middleware...)
}

func createHandler[T any](
	dec Decoder,
	consumerFunc func(ctx context.Context, event Event, target T) error,
	middleware ...Middleware,
) Handler {
	h := func(event Event) error {
		var target = new(T)

		if err := dec.Decode(event.Message().Body, target); err != nil {
			err = fmt.Errorf("failed to decode message body: %s", err)
			return err
		}
		if err := consumerFunc(context.Background(), event, *target); err != nil {
			return err
		}
		return nil
	}

	for _, mw := range middleware {
		h = mw(h)
	}

	return h
}
