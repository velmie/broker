package natsjs_test

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/velmie/broker/natsjs/v3"
)

func TestConnectionDoesNotReconnectToAdvertisedServers(t *testing.T) {
	primary, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer primary.Close()
	advertised, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer advertised.Close()
	connections := make(chan net.Conn, 1)
	go func() {
		conn, acceptErr := primary.Accept()
		if acceptErr != nil {
			return
		}
		connections <- conn
		_, _ = fmt.Fprintf(
			conn,
			"INFO {\"server_id\":\"test\",\"version\":\"2.9.0\",\"proto\":1,\"max_payload\":1048576,\"connect_urls\":[%q]}\r\n",
			advertised.Addr().String(),
		)
		scanner := bufio.NewScanner(conn)
		for scanner.Scan() {
			if strings.HasPrefix(scanner.Text(), "PING") {
				_, _ = io.WriteString(conn, "PONG\r\n")
			}
		}
	}()
	cfg := natsjs.ConnectionConfig{
		URL:            "nats://" + primary.Addr().String(),
		TLSMode:        "disabled",
		ConnectTimeout: time.Second,
		ReconnectWait:  10 * time.Millisecond,
		MaxReconnects:  1,
		InboxPrefix:    "_INBOX",
	}
	conn, err := natsjs.OpenConnection(context.Background(), cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	serverConn := <-connections
	if err = primary.Close(); err != nil {
		t.Fatal(err)
	}
	if err = serverConn.Close(); err != nil {
		t.Fatal(err)
	}
	if err = advertised.(*net.TCPListener).SetDeadline(time.Now().Add(500 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if discovered, acceptErr := advertised.Accept(); acceptErr == nil {
		discovered.Close()
		t.Fatal("connection dialed the advertised server")
	} else if timeout, ok := acceptErr.(net.Error); !ok || !timeout.Timeout() {
		t.Fatalf("advertised listener: %v", acceptErr)
	}
	if !conn.IsClosed() {
		t.Fatal("connection did not exhaust its configured reconnects")
	}
}
