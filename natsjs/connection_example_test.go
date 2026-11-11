package natsjs_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/velmie/broker/natsjs/v4"
)

func ExampleOpenConnection() {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cfg := natsjs.ConnectionConfig{
		URL:            "nats://127.0.0.1:4222",
		TLSMode:        "disabled",
		ConnectTimeout: 2 * time.Second,
		ReconnectWait:  250 * time.Millisecond,
		MaxReconnects:  3,
		InboxPrefix:    "_INBOX",
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	conn, err := natsjs.OpenConnection(ctx, cfg, logger)
	if err == nil {
		defer conn.Close()
	}
	fmt.Println(errors.Is(err, context.Canceled))
	// Output: true
}
