package main

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func natsEndpoint(t *testing.T) string {
	t.Helper()
	if endpoint := os.Getenv("NATS_TEST_URL"); endpoint != "" {
		return endpoint
	}
	image := os.Getenv("NATS_TEST_IMAGE")
	if image == "" {
		image = "nats:2.15.0"
	}
	output, err := docker("run", "--rm", "-d", "-p", "127.0.0.1::4222", image, "--jetstream")
	if err != nil {
		t.Fatalf("start NATS: %v: %s", err, output)
	}
	container := strings.TrimSpace(string(output))
	t.Cleanup(func() {
		if cleanupOutput, cleanupErr := docker("rm", "-f", container); cleanupErr != nil {
			t.Errorf("NATS cleanup: %v: %s", cleanupErr, cleanupOutput)
		}
	})
	output, err = docker("port", container, "4222/tcp")
	if err != nil {
		t.Fatal(err)
	}
	endpoint := "nats://" + strings.TrimSpace(string(output))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for {
		nc, err := nats.Connect(endpoint, nats.Timeout(100*time.Millisecond), nats.NoReconnect())
		if err == nil {
			nc.Close()
			return endpoint
		}
		select {
		case <-ctx.Done():
			t.Fatal(err)
		case <-time.After(10 * time.Millisecond):
		}
	}
}
func docker(args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	//nolint:gosec // Test-owned Docker arguments, passed without a shell.
	return exec.CommandContext(ctx, "docker", args...).CombinedOutput()
}
