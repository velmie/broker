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

// CreateDecodingHandler creates an event handler that uses a Decoder to decode event data into
// a concrete value, which is then passed to the consumer function
func CreateDecodingHandler[T any](
	dec Decoder,
	consumerFunc func(ctx context.Context, target T) error,
	options ...DecodeOption,
) Handler {
	opts := &decodingOptions{
		logBodyErr: true,
		logBodyMax: 1024,
	}
	for _, o := range options {
		o(opts)
	}

	logErr := func(err error, event Event) {
		if opts.logger != nil {
			msg := event.Message()
			msgBody := msg.Body
			if opts.logBodyErr {
				body := msgBody
				if opts.logBodyMax > 0 && len(body) > opts.logBodyMax {
					body = body[:opts.logBodyMax]
				}
				opts.logger.Error(err.Error(), "topic", event.Topic(), "messageId", msg.ID, "body", string(body))
			} else {
				opts.logger.Error(err.Error(), "topic", event.Topic(), "messageId", msg.ID)
			}
		}
	}

	return func(event Event) error {
		var target = new(T)

		if err := dec.Decode(event.Message().Body, target); err != nil {
			err = fmt.Errorf("failed to decode message body: %s", err)
			logErr(err, event)
			return err
		}
		if err := consumerFunc(context.Background(), *target); err != nil {
			logErr(err, event)
			return err
		}

		return nil
	}
}

type decodingOptions struct {
	logger     Logger
	logBodyErr bool
	logBodyMax int
}

// DecodeOption sets decoding option
type DecodeOption func(o *decodingOptions)

// DecodeUseLogger sets the logger for decoding errors
func DecodeUseLogger(l Logger) DecodeOption {
	return func(o *decodingOptions) {
		o.logger = l
	}
}

// DecodeLogBodyIfErr sets the flag for logging message body if an error occurs
func DecodeLogBodyIfErr(log bool) DecodeOption {
	return func(o *decodingOptions) {
		o.logBodyErr = log
	}
}

// DecodeLogBodyMax sets the maximum body length to log
func DecodeLogBodyMax(max int) DecodeOption {
	return func(o *decodingOptions) {
		o.logBodyMax = max
	}
}
