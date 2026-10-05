package sqs_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"testing"

	"github.com/velmie/broker"
	adapter "github.com/velmie/broker/sqs"
)

func TestSDKMalformedAttributesAndAttempts(t *testing.T) {
	cases := []struct {
		name   string
		change func(map[string]any)
	}{
		{"zero attempt", func(m map[string]any) { m["Attributes"] = map[string]string{"ApproximateReceiveCount": "0"} }},
		{"bad attempt", func(m map[string]any) { m["Attributes"] = map[string]string{"ApproximateReceiveCount": "invalid"} }},
		{"overflow attempt", func(m map[string]any) {
			m["Attributes"] = map[string]string{"ApproximateReceiveCount": "18446744073709551616"}
		}},
		{"missing receipt", func(m map[string]any) { delete(m, "ReceiptHandle") }},
		{"empty body", func(m map[string]any) { m["Body"] = "" }},
		{"invalid name", func(m map[string]any) {
			m["MessageAttributes"] = map[string]any{"secret\nname": attr("String", "secret-value")}
		}},
		{"list", func(m map[string]any) {
			m["MessageAttributes"] = map[string]any{"field": map[string]any{"DataType": "String",
				"StringValue":      "a",
				"StringListValues": []string{"secret"}}}
		}},
		{"both values", func(m map[string]any) {
			m["MessageAttributes"] = map[string]any{"field": map[string]any{"DataType": "String", "StringValue": "secret", "BinaryValue": "AP8="}}
		}},
		{"unknown type", func(m map[string]any) { m["MessageAttributes"] = map[string]any{"field": attr("Future", "secret")} }},
		{"number id", func(m map[string]any) { m["MessageAttributes"] = map[string]any{"id": attr("Number", "123")} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := native("{}", nil).(map[string]any)
			tc.change(m)
			client, url := newWire(t, func(context.Context, string, request) (int, any) { return 200, map[string]any{"Messages": []any{m}} })
			consumer := newConsumer(t, client, url, nil)
			err := consumer.Run(context.Background(), broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
				t.Error("malformed receive admitted")
				return broker.Handled{}, nil
			}))
			var field *adapter.FieldError
			if !errors.As(err, &field) {
				t.Fatalf("field diagnostics missing: %v", err)
			}
		})
	}
}

func TestSDKSNSRawAndEnvelopeWireFixtures(t *testing.T) {
	payload, err := os.ReadFile("testdata/sns-sqs-message.json")
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]any
	if err = json.Unmarshal(payload, &raw); err != nil {
		t.Fatal(err)
	}
	envelope, err := os.ReadFile("testdata/sns-envelope.json")
	if err != nil {
		t.Fatal(err)
	}
	// Old fixtures include the protocol receipt only in SQS's outer envelope.
	attributes := map[string]any{"id": attr("String", raw["domain_id"].(string))}
	for key, value := range raw["headers"].(map[string]any) {
		attributes[key] = attr("String", value.(string))
	}
	raw = native(raw["body"].(string), attributes).(map[string]any)
	for _, tc := range []struct {
		name    string
		message map[string]any
		wantID  string
		body    string
	}{
		{"raw", raw, "example-event-001", raw["Body"].(string)},
		{"envelope", native(string(envelope), nil).(map[string]any), "", string(envelope)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			delete(tc.message, "Attributes")
			stop := errors.New("test stop")
			client, url := newWire(t, func(context.Context, string, request) (int, any) {
				return 200, map[string]any{"Messages": []any{tc.message}}
			})
			consumer := newConsumer(t, client, url, nil)
			err := consumer.Run(context.Background(), broker.NewHandler(func(_ context.Context, d broker.Delivery) (broker.Disposition, error) {
				m := d.Message()
				if m.ID != tc.wantID || string(m.Body) != tc.body {
					t.Errorf("fixture changed id=%q body=%q", m.ID, m.Body)
				}
				if d.(broker.AttemptReader).Attempt().Known {
					t.Error("absent attempts became known")
				}
				return nil, stop
			}))
			if !errors.Is(err, stop) {
				t.Fatal(err)
			}
		})
	}
}

func TestSDKAttemptParseCausePreserved(t *testing.T) {
	for _, tc := range []struct {
		name, value string
		kind        error
	}{
		{"syntax", "SECRET-TOKEN\nhttps://private.example", strconv.ErrSyntax},
		{"range", "18446744073709551616", strconv.ErrRange},
	} {
		t.Run(tc.name, func(t *testing.T) {
			message := native("{}", nil).(map[string]any)
			message["Attributes"] = map[string]string{"ApproximateReceiveCount": tc.value}
			client, url := newWire(t, func(context.Context, string, request) (int, any) {
				return 200, map[string]any{"Messages": []any{message}}
			})
			var observed error
			consumer := newConsumer(t, client, url, func(c *adapter.ConsumerConfig) {
				c.Observer = &adapter.Observer{Record: func(_ context.Context, o adapter.Observation) { observed = o.Err }}
			})
			result := consumer.Run(context.Background(), broker.NewHandler(func(context.Context, broker.Delivery) (broker.Disposition, error) {
				t.Error("invalid attempt admitted")
				return broker.Handled{}, nil
			}))
			var original *strconv.NumError
			for _, err := range []error{result, observed} {
				var number *strconv.NumError
				var field *adapter.FieldError
				if !errors.As(err, &number) || !errors.Is(err, tc.kind) {
					t.Fatalf("parse cause replaced: %v", err)
				}
				if !errors.As(err, &field) || field.Field != "Attributes.ApproximateReceiveCount" {
					t.Fatalf("field missing: %v", err)
				}
				if number.Func != "ParseUint" || number.Num != tc.value {
					t.Fatalf("original parse details lost: %+v", number)
				}
				if original != nil && original != number {
					t.Fatal("observer and return do not retain same parse cause")
				}
				original = number
			}
		})
	}
}
