package natsjs_test

import (
	"context"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestFiltersCommand(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", "run", "./cmd/filters", "-url", serverURL)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("filter example: %v\n%s", err, output)
	}
	for _, expected := range []string{"processed created", "processed paid", "acknowledged 2"} {
		if !strings.Contains(string(output), expected) {
			t.Fatalf("missing %q in %s", expected, output)
		}
	}
	if strings.Contains(string(output), "processed excluded") {
		t.Fatalf("excluded event processed: %s", output)
	}
	conn, err := nats.Connect(serverURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(conn.Close)
	js, err := conn.JetStream()
	if err != nil {
		t.Fatal(err)
	}
	for name := range js.StreamNames(nats.Context(ctx)) {
		if strings.HasPrefix(name, "BROKER_FILTERS_") {
			t.Fatalf("example left stream %q", name)
		}
	}
}
