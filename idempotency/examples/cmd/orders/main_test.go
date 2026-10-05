package main

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/nats-io/nats.go"
)

func TestNativeCommittedReplayRunsBusinessOnce(t *testing.T) {
	endpoint := natsEndpoint(t)
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{ReplaceAttr: redactAttribute}))
	if err := run(context.Background(), endpoint, logger); err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{"order effect completed", "completion marker committed", "first acknowledgment suppressed",
		"stored completion replayed", "native acknowledgment confirmed", "native consumer empty"} {
		if count := strings.Count(output.String(), expected); count != 1 {
			t.Fatalf("expected one %q, got %d: %s", expected, count, output.String())
		}
	}
	assertTopologyRemoved(t, endpoint)
}

func TestCommandRejectsRemoteOrCredentialEndpoints(t *testing.T) {
	for _, endpoint := range []string{"", "nats://example.com:4222", "nats://user:private-token@127.0.0.1:4222"} {
		var output bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{ReplaceAttr: redactAttribute}))
		if err := run(context.Background(), endpoint, logger); err == nil {
			t.Fatal("invalid endpoint accepted")
		}
		if output.Len() != 0 {
			t.Fatal("invalid configuration caused processing effects")
		}
	}
}
func assertTopologyRemoved(t *testing.T, endpoint string) {
	t.Helper()
	nc, err := nats.Connect(endpoint, nats.FlusherTimeout(operationBudget), nats.NoReconnect())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := nc.JetStream(nats.MaxWait(operationBudget))
	if err != nil {
		t.Fatal(err)
	}
	info, err := js.AccountInfo()
	if err != nil {
		t.Fatal(err)
	}
	if info.Streams == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), operationBudget)
	defer cancel()
	for name := range js.StreamNames(nats.Context(ctx)) {
		if strings.HasPrefix(name, "BROKER_IDEMPO_") {
			t.Fatalf("task topology remained: %s", name)
		}
	}
	if ctx.Err() != nil {
		t.Fatal(ctx.Err())
	}
}
