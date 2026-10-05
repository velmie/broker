package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	nativeSQS "github.com/aws/aws-sdk-go-v2/service/sqs"
)

// Runtime tests start their fixture lazily, so focused SDK HTTP tests need no Docker.
func emulatorEndpoint(t *testing.T) string {
	t.Helper()
	if endpoint := os.Getenv("AWS_TEST_ENDPOINT"); endpoint != "" {
		return endpoint
	}
	image := os.Getenv("SNS_TEST_IMAGE")
	if image == "" {
		image = "motoserver/moto:5.2.3"
	}
	output, err := docker("run", "--rm", "-d", "-p", "127.0.0.1::5000", image)
	if err != nil {
		t.Fatalf("start Moto: %v: %s", err, output)
	}
	container := strings.TrimSpace(string(output))
	t.Cleanup(func() {
		if result, cleanupErr := docker("rm", "-f", container); cleanupErr != nil {
			t.Errorf("cleanup Moto: %v: %s", cleanupErr, result)
		}
	})
	output, err = docker("port", container, "5000/tcp")
	if err != nil {
		t.Fatalf("Moto port: %v: %s", err, output)
	}
	endpoint := "http://" + strings.TrimSpace(string(output))
	queues, _ := newClients(endpoint)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for {
		_, err = queues.ListQueues(ctx, &nativeSQS.ListQueuesInput{})
		if err == nil {
			return endpoint
		}
		select {
		case <-ctx.Done():
			t.Fatalf("Moto readiness: %v", err)
		case <-time.After(50 * time.Millisecond):
		}
	}
}
func docker(args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	//nolint:gosec // Test-owned arguments are passed directly to Docker without a shell.
	output, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	if err != nil {
		return output, fmt.Errorf("Docker operation: %w", err)
	}
	return output, nil
}
