package natsjs_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/velmie/broker"
	"github.com/velmie/broker/natsjs/v4"
)

func TestConnectionValidationReportsPreciseFieldsAndRules(t *testing.T) {
	base := natsjs.ConnectionConfig{
		URL:            "nats://remote.example:4222",
		TLSMode:        "verify",
		Username:       "user",
		Password:       "password",
		ConnectTimeout: time.Second,
		ReconnectWait:  time.Second,
		InboxPrefix:    "_INBOX",
	}
	for _, tc := range []struct {
		name    string
		change  func(*natsjs.ConnectionConfig)
		field   string
		reason  string
		message string
	}{
		{
			name:    "empty url",
			change:  func(c *natsjs.ConnectionConfig) { c.URL = "" },
			field:   "url",
			reason:  "required",
			message: "required",
		},
		{
			name:    "missing password",
			change:  func(c *natsjs.ConnectionConfig) { c.Password = "" },
			field:   "password",
			reason:  "required",
			message: "required",
		},
		{
			name:    "disabled remote tls",
			change:  func(c *natsjs.ConnectionConfig) { c.TLSMode = "disabled" },
			field:   "tls_mode",
			reason:  "eq",
			message: "verify",
		},
		{
			name:    "invalid timeout",
			change:  func(c *natsjs.ConnectionConfig) { c.ConnectTimeout = 0 },
			field:   "connect_timeout",
			reason:  "gt",
			message: "0",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base
			tc.change(&cfg)
			err := cfg.Validate()
			joined, ok := err.(interface{ Unwrap() []error })
			if !ok || len(joined.Unwrap()) != 1 {
				t.Fatalf("expected one field error: %v", err)
			}
			var field *broker.FieldError
			if !errors.As(joined.Unwrap()[0], &field) || field.Field != tc.field || field.Reason != tc.reason ||
				field.Cause == nil || !strings.Contains(field.Cause.Error(), tc.message) {
				t.Fatalf("field violation: %#v (%v)", field, err)
			}
		})
	}
}

func TestOpenConnectionRejectsNilLogger(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cfg := natsjs.ConnectionConfig{
		URL:            "nats://127.0.0.1:4222",
		TLSMode:        "disabled",
		ConnectTimeout: time.Second,
		ReconnectWait:  time.Second,
		InboxPrefix:    "_INBOX",
	}
	if _, err := natsjs.OpenConnection(ctx, cfg, nil); err == nil || !strings.Contains(err.Error(), "logger") {
		t.Fatalf("expected missing logger error: %v", err)
	}
}

func TestTopologyValidationReportsRuleAndField(t *testing.T) {
	cfg := natsjs.DurableConsumerConfig{
		Stream:        "ORDERS",
		Durable:       "processor",
		Subject:       "orders.created",
		AckWait:       0,
		MaxDeliveries: 1,
		MaxAckPending: 1,
	}
	var field *broker.FieldError
	err := cfg.Validate()
	if !errors.As(err, &field) || field.Field != "ack_wait" || field.Reason != "gt" ||
		field.Cause == nil || !strings.Contains(field.Error(), "greater than 0") {
		t.Fatalf("expected ack_wait greater than zero: %#v (%v)", field, err)
	}
}

func TestTopologyProfilesValidateRequiredAndPositiveFields(t *testing.T) {
	cases := []struct {
		name     string
		validate func() error
		field    string
		reason   string
	}{
		{
			name: "limits subjects",
			validate: func() error {
				return (natsjs.LimitsStreamConfig{
					Name:            "ORDERS",
					DuplicateWindow: time.Minute,
				}).Validate()
			},
			field:  "subjects",
			reason: "required",
		},
		{
			name: "work queue max bytes",
			validate: func() error {
				return (natsjs.WorkQueueStreamConfig{
					Name:            "ORDERS",
					Subject:         "orders.>",
					MaxMessageBytes: 1,
					DuplicateWindow: time.Minute,
					MaxMessages:     1,
				}).Validate()
			},
			field:  "max_bytes",
			reason: "gt",
		},
		{
			name: "retained subject",
			validate: func() error {
				return (natsjs.RetainedStreamConfig{
					Stream:          "ORDERS",
					Subject:         "orders.*",
					MaxMessageBytes: 1,
				}).Validate()
			},
			field:  "subject",
			reason: "type",
		},
		{
			name: "consumer deliveries",
			validate: func() error {
				return (natsjs.DurableConsumerConfig{
					Stream:        "ORDERS",
					Durable:       "worker",
					Subject:       "orders.>",
					AckWait:       time.Second,
					MaxAckPending: 1,
				}).Validate()
			},
			field:  "max_deliveries",
			reason: "oneof",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var field *broker.FieldError
			err := tc.validate()
			if !errors.As(err, &field) || field.Field != tc.field || field.Reason != tc.reason || field.Cause == nil {
				t.Fatalf("expected %s %s, got %#v (%v)", tc.field, tc.reason, field, err)
			}
		})
	}
}
