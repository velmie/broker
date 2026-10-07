// Command native runs caller-owned JetStream sessions with distinct native policies.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	commandTimeout      = time.Minute
	profileAsyncPublish = "async-publish"
	profilePush         = "push"
	profileIterator     = "iterator"
	profilePolicies     = "policies"
	profileTopology     = "topology"
	profileConnection   = "connection"
)

func main() {
	url := flag.String("url", nats.DefaultURL, "NATS server URL or native comma-separated URL list")
	profile := flag.String("profile", "all", "all, async-publish, push, iterator, policies, topology, or connection")
	flag.Parse()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{ReplaceAttr: redactAttribute}))
	owner, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	ctx, cancel := context.WithTimeout(owner, commandTimeout)
	err := run(ctx, *url, *profile, logger)
	cancel()
	stop()
	if err != nil {
		logger.Error("native JetStream session failed", slog.Any("error", err))
		os.Exit(1)
	}
}

func run(ctx context.Context, url, profile string, logger *slog.Logger) (result error) {
	profiles := []string{profile}
	if profile == "all" {
		profiles = []string{profileAsyncPublish, profilePush, profileIterator, profilePolicies, profileTopology, profileConnection}
	} else {
		switch profile {
		case profileAsyncPublish, profilePush, profileIterator, profilePolicies, profileTopology, profileConnection:
		default:
			return operationError("native/command", "select_profile", errors.New("unknown native profile"))
		}
	}
	var asynchronous asyncFailures
	closed := make(chan struct{})
	nc, err := connectWithOptions(ctx, url,
		nats.Name("broker-native-examples"), nats.DrainTimeout(operationTimeout),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) { asynchronous.add(err) }),
		nats.ClosedHandler(func(*nats.Conn) { close(closed) }))
	if err != nil {
		return operationError("native/connection", "connect", err)
	}
	defer func() {
		// Every profile joins its subscriptions and futures before connection drain.
		result = errors.Join(result, operationError("native/connection", "drain", nc.Drain()))
		timer := time.NewTimer(operationTimeout)
		defer timer.Stop()
		select {
		case <-closed:
		case <-timer.C:
			result = errors.Join(result, operationError("native/connection", "drain", context.DeadlineExceeded))
			nc.Close()
			<-closed
		}
		result = errors.Join(result, asynchronous.err())
	}()
	for _, name := range profiles {
		switch name {
		case profileAsyncPublish:
			err = runAsyncPublish(ctx, nc)
		case profilePush:
			err = runPush(ctx, nc)
		case profileIterator:
			err = runIterator(ctx, nc)
		case profilePolicies:
			err = runPolicies(ctx, nc)
		case profileTopology:
			err = runTopology(ctx, nc)
		case profileConnection:
			err = runConnection(ctx, nc)
		}
		if err != nil {
			return operationError(name, "run", err)
		}
		logger.Info("native JetStream profile completed", "profile", name)
	}
	return nil
}

type asyncFailures struct {
	mu     sync.Mutex
	causes []error
}

func (f *asyncFailures) add(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.causes = append(f.causes, operationError("native/connection", "async", err))
}

func (f *asyncFailures) err() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return errors.Join(f.causes...)
}
