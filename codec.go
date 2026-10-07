package broker

import (
	"encoding/json"
	"errors"
	"reflect"
)

// DecoderFunc owns allocation and must return independent mutable DTO data for
// each call. Its concurrency must match the configured consumers.
type DecoderFunc[T any] func([]byte) (T, error)

// Encoder encodes an already constructed value.
type Encoder interface{ Encode(any) ([]byte, error) }
type EncoderFunc func(any) ([]byte, error)

// CorrelationIDSetter is an optional DTO metadata binding contract.
type CorrelationIDSetter interface{ SetCorrelationID(string) }

// DecodeJSON unmarshals into a fresh value with ordinary encoding/json semantics.
// JSON null can return a nil pointer. Typed handlers reject that result before
// binding or callback invocation. Domain validation remains application-owned.
func DecodeJSON[T any](body []byte) (T, error) {
	var value T
	err := json.Unmarshal(body, &value)
	return value, err
}

func (f EncoderFunc) Encode(value any) ([]byte, error) { return f(value) }

func decode[T any](decoder DecoderFunc[T], body []byte) (T, error) {
	value, err := decoder(body)
	if err != nil {
		var zero T
		return zero, &DecodeError{Cause: err}
	}
	rv := reflect.ValueOf(value)
	if rv.IsValid() && rv.Kind() == reflect.Pointer && rv.IsNil() {
		return value, &DecodeError{Cause: errors.New("decoded pointer is nil")}
	}
	return value, nil
}

func bindCorrelationID[T any](value *T, headers []Header) error {
	id, found, err := singleTextHeader(headers, "Correlation-Id")
	if err != nil {
		return err
	}
	if !found {
		return nil
	}
	if setter, ok := any(*value).(CorrelationIDSetter); ok {
		setter.SetCorrelationID(id)
	} else if setter, ok := any(value).(CorrelationIDSetter); ok {
		setter.SetCorrelationID(id)
	}
	return nil
}
