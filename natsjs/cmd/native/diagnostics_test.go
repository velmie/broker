package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/velmie/broker"
	adapter "github.com/velmie/broker/natsjs/v3"
)

func TestNativeDiagnosticsKeepBranchContextWithoutSecrets(t *testing.T) {
	api := &nats.APIError{Code: 400, ErrorCode: 10060, Description: "credential=secret-native-value subject private.subject"}
	network := &net.OpError{Op: "dial",
		Net: "tcp",
		Addr: &net.TCPAddr{IP: net.ParseIP("192.0.2.1"),
			Port: 4222},
		Err: fmt.Errorf("token-secret: %w", syscall.ECONNREFUSED)}
	failure := errors.Join(operationError("async-publish", "expectation", api), operationError("connection", "connect", network))
	if !errors.Is(failure, api) || !errors.Is(failure, network) {
		t.Fatal("original causes lost")
	}
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{ReplaceAttr: redactAttribute}))
	logger.With(slog.Any("bound", failure)).Error("native failure", slog.Any("error", failure))
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(output.Bytes(), &fields); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"bound", "error"} {
		var tree struct {
			Causes []struct {
				Source, Operation string
				Cause             struct {
					Type      string
					Code      int
					ErrorCode uint16
				}
			}
		}
		if err := json.Unmarshal(fields[key], &tree); err != nil {
			t.Fatal(err)
		}
		if len(tree.Causes) != 2 ||
			tree.Causes[0].Source != "async-publish" ||
			tree.Causes[0].Operation != "expectation" ||
			tree.Causes[0].Cause.ErrorCode != 10060 ||
			tree.Causes[1].Source != "connection" ||
			tree.Causes[1].Operation != "connect" ||
			tree.Causes[1].Cause.Type != "*net.OpError" {
			t.Fatalf("branch context missing: %s", fields[key])
		}
	}
	for _, secret := range []string{"secret-native-value", "private.subject", "192.0.2.1", "token-secret"} {
		if strings.Contains(output.String(), secret) {
			t.Errorf("leaked %s", secret)
		}
	}
	if !strings.Contains(output.String(), fmt.Sprintf(`"errno":%d`, syscall.ECONNREFUSED)) {
		t.Fatal("native connection refusal code missing")
	}
	if !strings.Contains(output.String(), "dial") {
		t.Fatal("safe network stage missing")
	}
}

func TestNativeDiagnosticsBoundsUntrustedLabels(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{ReplaceAttr: redactAttribute}))
	err := operationError(strings.Repeat("x", 1000)+"\nsecret", strings.Repeat("y", 1000), fmt.Errorf("sensitive body"))
	logger.Error("failed", slog.Any("error", errors.Join(err, &broker.StageError{Stage: "secret-stage-input", Cause: err})))
	if strings.Contains(output.String(),
		strings.Repeat("x",
			129)) ||
		strings.Contains(output.String(),
			"secret") ||
		strings.Contains(output.String(),
			"sensitive body") {
		t.Fatal("diagnostics not bounded/redacted")
	}
}

func TestNativeDiagnosticsKeepPolicyFieldAndProcessingStage(t *testing.T) {
	for _, field := range []string{"MaxAckPending", "MemoryStorage"} {
		t.Run(field, func(t *testing.T) {
			config := nats.ConsumerConfig{
				MaxDeliver: policyResourceMaxDeliver, MaxAckPending: policyResourceMaxAckPending,
				MaxRequestBatch: policyResourceMaxRequestBatch, MaxRequestExpires: time.Second,
				MaxRequestMaxBytes: policyPullMaxBytes, MaxWaiting: policyResourceMaxWaiting,
				InactiveThreshold: time.Minute, Description: "Bounded native pull requests",
				MemoryStorage: true, Replicas: 1,
			}
			if field == "MaxAckPending" {
				config.MaxAckPending++
			} else {
				config.MemoryStorage = false
			}
			policyFailure := verifyResourceLimits(&config)
			original := fmt.Errorf("credential=secret-handler body=private-content")
			staged := &broker.StageError{Stage: broker.StageHandle, Cause: original}
			failure := errors.Join(operationError("policies", "run", policyFailure), operationError("iterator", "callback", staged))
			if !errors.Is(failure, original) || !errors.Is(failure, policyFailure) {
				t.Fatal("original cause discarded")
			}
			var output bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{ReplaceAttr: redactAttribute}))
			logger.Error("native profile failed", slog.Any("error", failure))
			for _, part := range []string{
				`"source":"limits"`, `"operation":"verify-resource"`,
				`"field":"` + field + `"`, `"stage":"handle"`, `"type":"*errors.errorString"`,
			} {
				if !strings.Contains(output.String(), part) {
					t.Errorf("lost diagnostic context %s: %s", part, output.String())
				}
			}
			for _, secret := range []string{"secret-handler", "private-content"} {
				if strings.Contains(output.String(), secret) {
					t.Errorf("sensitive cause serialized: %s", output.String())
				}
			}
		})
	}
}

func TestNativeDiagnosticsIdentifyValidationAndConnectionFailures(t *testing.T) {
	_, invalid := adapter.NewPublisher(&nats.Conn{}, adapter.PublisherConfig{Subject: "invalid subject"})
	cases := []struct {
		name    string
		failure error
		fields  []string
	}{
		{"validation", invalid, []string{`"field":"Subject"`, `"reason":"invalid_destination"`}},
		{"authorization", nats.ErrAuthorization, []string{`"reason":"authorization_violation"`}},
		{"no servers", nats.ErrNoServers, []string{`"reason":"no_servers"`}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{ReplaceAttr: redactAttribute}))
			logger.Error("failed", slog.Any("error", test.failure))
			for _, field := range test.fields {
				if !strings.Contains(output.String(), field) {
					t.Errorf("missing %s: %s", field, output.String())
				}
			}
		})
	}
}
