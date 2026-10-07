package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

var nativeServerURL string

func TestMain(m *testing.M) {
	output, err := docker("run", "--rm", "-d", "-p", "127.0.0.1::4222", nativeImage(), "--jetstream")
	if err != nil {
		fmt.Fprintf(os.Stderr, "start native profile server: %v: %s\n", err, output)
		os.Exit(1)
	}
	container := strings.TrimSpace(string(output))
	nativeServerURL, err = nativeServerAddress(container)
	code := 1
	if err == nil {
		code = m.Run()
	} else {
		fmt.Fprintf(os.Stderr, "native profile server readiness: %v\n", err)
	}
	if output, err = docker("rm", "-f", container); err != nil {
		fmt.Fprintf(os.Stderr, "remove native profile server: %v: %s\n", err, output)
		code = 1
	}
	os.Exit(code)
}

func TestNativeCommandAll(t *testing.T) {
	nc := nativeConnection(t)
	audit, err := nc.SubscribeSync("$JS.API.CONSUMER.>")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cleanupErr := audit.Unsubscribe(); cleanupErr != nil {
			t.Error(cleanupErr)
		}
	})
	if err = nc.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{ReplaceAttr: redactAttribute}))
	if err = run(ctx, nativeServerURL, "all", logger); err != nil {
		t.Fatal(err)
	}
	for _, profile := range []string{"async-publish", "push", "iterator", "policies", "topology", "connection"} {
		if !strings.Contains(output.String(), `"profile":"`+profile+`"`) {
			t.Errorf("missing completed profile %s", profile)
		}
	}
	assertNativePoliciesObserved(t, audit)
	js, err := nc.JetStream(nats.MaxWait(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	info, err := js.AccountInfo()
	if err != nil || info.Streams != 0 || info.Consumers != 0 {
		t.Fatalf("native command left topology: %+v, %v", info, err)
	}
}

func nativeConnection(t *testing.T) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect(nativeServerURL, nats.Timeout(time.Second), nats.FlusherTimeout(time.Second), nats.NoReconnect())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	return nc
}

func startNativeServer(t *testing.T, configDir string) (url, container string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err = listener.Close(); err != nil {
		t.Fatal(err)
	}
	// An explicit host port survives container restart. Docker reallocates an
	// automatically selected publish port on restart, changing the endpoint.
	args := []string{"run", "--rm", "-d", "-p", address + ":4222"}
	if configDir != "" {
		args = append(args, "--volume", filepath.Clean(configDir)+":/etc/nats/profile:ro", nativeImage(),
			"--config", "/etc/nats/profile/nats.conf")
	} else {
		args = append(args, nativeImage(), "--jetstream")
	}
	output, err := docker(args...)
	if err != nil {
		t.Fatalf("start configured native server: %v: %s", err, output)
	}
	container = strings.TrimSpace(string(output))
	t.Cleanup(func() {
		if t.Failed() {
			logs, logErr := docker("logs", "--tail", "60", container)
			t.Logf("configured native server diagnostics: %v\n%s", logErr, logs)
		}
		if cleanupOutput, cleanupErr := docker("rm", "-f", container); cleanupErr != nil {
			t.Errorf("remove configured native server: %v: %s", cleanupErr, cleanupOutput)
		}
	})
	url, err = nativeServerAddress(container)
	if err != nil {
		t.Fatal(err)
	}
	return url, container
}

func nativeServerAddress(container string) (string, error) {
	output, err := docker("port", container, "4222/tcp")
	if err != nil {
		return "", fmt.Errorf("native server port: %w", err)
	}
	address := strings.TrimSpace(string(output))
	deadline := time.Now().Add(10 * time.Second)
	for {
		connection, connectErr := net.DialTimeout("tcp", address, time.Second)
		if connectErr == nil {
			if closeErr := connection.Close(); closeErr != nil {
				return "", closeErr
			}
			return "nats://" + address, nil
		}
		if time.Now().After(deadline) {
			return "", connectErr
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func nativeImage() string {
	if image := os.Getenv("NATS_TEST_IMAGE"); image != "" {
		return image
	}
	return "nats:2.15.0"
}

func docker(args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	// #nosec G702 -- Fixed Docker operations use test-owned paths and IDs without a shell.
	command := exec.CommandContext(ctx, "docker", args...)
	if args[0] == "logs" {
		return command.CombinedOutput()
	}
	output, err := command.Output()
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		err = fmt.Errorf("Docker operation: %w: %s", err, exit.Stderr)
	}
	return output, err
}

func assertNativePoliciesObserved(t *testing.T, audit *nats.Subscription) {
	t.Helper()
	ack := map[nats.AckPolicy]bool{}
	delivery := map[nats.DeliverPolicy]bool{}
	var headers, flow, backoff, rate, replay bool
	for {
		message, err := audit.NextMsg(20 * time.Millisecond)
		if errors.Is(err, nats.ErrTimeout) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(message.Subject, ".CREATE.") {
			continue
		}
		var request struct{ Config nats.ConsumerConfig }
		if err = json.Unmarshal(message.Data, &request); err != nil {
			t.Fatal(err)
		}
		config := request.Config
		ack[config.AckPolicy], delivery[config.DeliverPolicy] = true, true
		headers, flow = headers || config.HeadersOnly, flow || config.FlowControl
		backoff, rate = backoff || len(config.BackOff) > 0, rate || config.RateLimit > 0
		replay = replay || config.ReplayPolicy == nats.ReplayOriginalPolicy
	}
	for _, policy := range []nats.AckPolicy{nats.AckNonePolicy, nats.AckAllPolicy, nats.AckExplicitPolicy} {
		if !ack[policy] {
			t.Errorf("no actual native consumer with ACK policy %v", policy)
		}
	}
	for _, policy := range []nats.DeliverPolicy{nats.DeliverAllPolicy, nats.DeliverLastPolicy, nats.DeliverLastPerSubjectPolicy,
		nats.DeliverNewPolicy, nats.DeliverByStartSequencePolicy, nats.DeliverByStartTimePolicy} {
		if !delivery[policy] {
			t.Errorf("no actual native consumer with delivery policy %v", policy)
		}
	}
	if !headers || !flow || !backoff || !rate || !replay {
		t.Errorf("missing actual native policy: headers=%v flow=%v backoff=%v rate=%v replay=%v", headers, flow, backoff, rate, replay)
	}
}
