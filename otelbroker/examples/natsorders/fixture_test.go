package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/velmie/broker/natsjs/v3"
)

const testTimeout = 5 * time.Second

var serverURL, containerID string

func TestMain(m *testing.M) {
	serverImage := os.Getenv("NATS_TEST_IMAGE")
	if serverImage == "" {
		serverImage = "nats:2.15.0"
	}
	output, err := docker("run", "--rm", "-d", "-p", "127.0.0.1::4222", serverImage, "--jetstream")
	if err != nil {
		fmt.Fprintf(os.Stderr, "start NATS: %v\n%s", err, output)
		os.Exit(1)
	}
	containerID = strings.TrimSpace(string(output))
	output, err = docker("port", containerID, "4222/tcp")
	if err == nil {
		serverURL = "nats://" + strings.TrimSpace(string(output))
		err = waitForServer()
	}
	code := 1
	if err == nil {
		code = m.Run()
	} else {
		fmt.Fprintf(os.Stderr, "NATS port: %v\n%s", err, output)
	}
	if output, err = docker("rm", "-f", containerID); err != nil {
		fmt.Fprintf(os.Stderr, "NATS cleanup: %v\n%s", err, output)
		code = 1
	}
	os.Exit(code)
}

func provision(t *testing.T, ackWait time.Duration) (*nats.Conn, natsjs.ConsumerConfig) {
	t.Helper()
	nc, err := nats.Connect(serverURL, nats.Timeout(testTimeout), nats.FlusherTimeout(time.Second), nats.NoReconnect())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := nc.JetStream(nats.MaxWait(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	name := strings.ReplaceAll(t.Name(), "/", "_")
	subject := name + ".orders"
	if _, err = js.AddStream(&nats.StreamConfig{Name: name, Subjects: []string{subject}, Storage: nats.MemoryStorage}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cleanupErr := js.DeleteStream(name); cleanupErr != nil {
			t.Error(cleanupErr)
		}
	})
	remote := &nats.ConsumerConfig{Durable: "worker", FilterSubject: subject, AckPolicy: nats.AckExplicitPolicy, AckWait: ackWait}
	if _, err = js.AddConsumer(name, remote); err != nil {
		t.Fatal(err)
	}
	return nc, natsjs.ConsumerConfig{
		Stream: name, Consumer: "worker", Subject: subject, FetchTimeout: 100 * time.Millisecond, OperationTimeout: time.Second,
	}
}

func docker(args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	//nolint:gosec // Test-owned Docker arguments are passed directly without a shell.
	output, err := exec.CommandContext(ctx, "docker", args...).Output()
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		err = fmt.Errorf("%w: %s", err, exit.Stderr)
	}
	return output, err
}

func waitForServer() error {
	timer := time.NewTimer(testTimeout)
	defer timer.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		nc, err := nats.Connect(serverURL, nats.Timeout(100*time.Millisecond), nats.NoReconnect())
		if err == nil {
			nc.Close()
			return nil
		}
		select {
		case <-timer.C:
			return err
		case <-ticker.C:
		}
	}
}
