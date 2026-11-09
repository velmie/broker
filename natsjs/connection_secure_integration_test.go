package natsjs_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/velmie/broker/natsjs/v3"
)

//nolint:gocyclo // The integration test owns one secure server lifecycle.
func TestConfiguredConnectionTLSAndCredentials(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: "nats.test",
		},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		DNSNames:              []string{"nats.test"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certPath := filepath.Join(dir, "ca.pem")
	keyPath := filepath.Join(dir, "key.pem")
	certificate := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: der,
	})
	if err = writeContainerFixture(certPath, certificate); err != nil {
		t.Fatal(err)
	}
	privateKey := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(key),
	})
	if err = writeContainerFixture(keyPath, privateKey); err != nil {
		t.Fatal(err)
	}
	conf := `port: 4222

tls { cert_file: "/config/ca.pem", key_file: "/config/key.pem" }
authorization {
  users = [{
    user: "test", password: "test-password"
    permissions: { publish: ["_INBOX.bookstore.>"], subscribe: ["_INBOX.bookstore.>"] }
  }]
}
`
	if err = writeContainerFixture(filepath.Join(dir, "nats.conf"), []byte(conf)); err != nil {
		t.Fatal(err)
	}
	image := os.Getenv("NATS_TEST_IMAGE")
	if image == "" {
		image = "nats:2.15.0"
	}
	out, err := docker("run", "--rm", "-d", "-p", "127.0.0.1::4222", "-v", dir+":/config:ro", image, "-c", "/config/nats.conf")
	if err != nil {
		t.Fatalf("start secure NATS: %v: %s", err, out)
	}
	id := strings.TrimSpace(string(out))
	t.Cleanup(func() {
		if cleanupOut, cleanupErr := docker("rm", "-f", id); cleanupErr != nil {
			t.Errorf("cleanup: %v: %s", cleanupErr, cleanupOut)
		}
	})
	out, err = docker("port", id, "4222/tcp")
	if err != nil {
		t.Fatal(err)
	}
	cfg := natsjs.ConnectionConfig{
		URL:            "nats://" + strings.TrimSpace(string(out)),
		Username:       "test",
		Password:       "test-password",
		TLSMode:        "verify",
		TLSCAFile:      certPath,
		TLSServerName:  "nats.test",
		ConnectTimeout: 2 * time.Second,
		ReconnectWait:  100 * time.Millisecond,
		InboxPrefix:    "_INBOX.bookstore",
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	var connErr error
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		conn, openErr := natsjs.OpenConnection(context.Background(), cfg, logger)
		connErr = openErr
		if openErr == nil {
			t.Cleanup(conn.Close)
			state, stateErr := conn.TLSConnectionState()
			if stateErr != nil || state.Version < tls.VersionTLS12 || len(state.VerifiedChains) == 0 {
				t.Fatalf("TLS verification: %v", stateErr)
			}
			inbox := conn.NewInbox()
			sub, subErr := conn.SubscribeSync(inbox)
			if subErr != nil {
				t.Fatal(subErr)
			}
			if err := conn.FlushTimeout(time.Second); err != nil {
				t.Fatal(err)
			}
			if err := conn.Publish(inbox, []byte("reply")); err != nil {
				t.Fatal(err)
			}
			message, err := sub.NextMsg(time.Second)
			if err != nil || string(message.Data) != "reply" {
				t.Fatalf("configured inbox permissions: %v", err)
			}
			if err := sub.Unsubscribe(); err != nil {
				t.Fatal(err)
			}
			conn.Close()
			connErr = nil
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if connErr != nil {
		t.Fatal(fmt.Errorf("secure startup: %w", connErr))
	}
	cfg.Password = "incorrect"
	if conn, err := natsjs.OpenConnection(context.Background(), cfg, logger); err == nil {
		conn.Close()
		t.Fatal("invalid credentials accepted")
	}
	cfg.Password = "test-password"
	cfg.TLSServerName = "wrong.test"
	if conn, err := natsjs.OpenConnection(context.Background(), cfg, logger); err == nil {
		conn.Close()
		t.Fatal("wrong TLS name accepted")
	}
}

func writeContainerFixture(path string, data []byte) error {
	// Test-only material must be readable by the server container.
	return os.WriteFile(path, data, 0o644) //nolint:gosec // Ephemeral container fixture.
}
