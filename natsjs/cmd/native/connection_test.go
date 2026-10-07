package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"log/slog"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestNativeConnectionOptionsAndBorrowing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), operationTimeout)
	defer cancel()
	conn,
		err := connectWithOptions(ctx,
		nativeServerURL,
		nats.Name("native-options"),
		nats.ReconnectWait(25*time.Millisecond),
		nats.MaxReconnects(3))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if conn.Opts.Name != "native-options" || conn.Opts.ReconnectWait != 25*time.Millisecond || conn.Opts.MaxReconnect != 3 {
		t.Fatal("native connection options replaced")
	}
	if err = runConnection(ctx, conn); err != nil {
		t.Fatal(err)
	}
	if err = conn.FlushWithContext(ctx); err != nil {
		t.Fatalf("session closed borrowed connection: %v", err)
	}
}

func TestNativeInitialRetryReadinessIsBounded(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err = listener.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	conn,
		err := connectWithOptions(ctx,
		"nats://"+address,
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(10*time.Millisecond),
		nats.Timeout(20*time.Millisecond))
	if conn != nil {
		conn.Close()
		t.Error("failed readiness returned live client")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline cause lost: %v", err)
	}
}

func TestNativeAuthenticationAndTLS(t *testing.T) {
	dir := t.TempDir()
	roots := writeSyntheticCertificate(t, dir)
	config := `port: 4222
jetstream {store_dir: "/data"}
authorization {users: [{user: "synthetic", password: "synthetic-secret", permissions: {publish: {allow: ["allowed.>"]}}}]}
tls {cert_file: "/etc/nats/profile/server.pem", key_file: "/etc/nats/profile/server.key"}
`
	if err := os.WriteFile(filepath.Join(dir, "nats.conf"), []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	url, _ := startNativeServer(t, dir)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	asyncErrors := make(chan error, 1)
	dialer := &nativeTLSDecisionDialer{checked: make(chan struct{})}
	conn,
		err := connectWithOptions(ctx,
		url,
		nats.SetCustomDialer(dialer),
		nats.ErrorHandler(func(_ *nats.Conn,
			_ *nats.Subscription,
			err error) {
			asyncErrors <- err
		}),
		nats.UserInfo("synthetic",
			"synthetic-secret"),
		nats.Secure(&tls.Config{RootCAs: roots,
			ServerName: "localhost",
			MinVersion: tls.VersionTLS12}))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err = conn.TLSConnectionState(); err != nil {
		t.Fatalf("TLS not active: %v", err)
	}
	select {
	case <-dialer.checked:
	default:
		t.Fatal("native custom dialer TLS decision was not forwarded")
	}
	if err = conn.Publish("private.subject", []byte("sensitive-payload")); err != nil {
		t.Fatal(err)
	}
	if err = conn.FlushWithContext(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case original := <-asyncErrors:
		wrapped := operationError("connection", "async-publish", original)
		if !errors.Is(wrapped, nats.ErrPermissionViolation) || !errors.Is(wrapped, original) {
			t.Fatalf("async permission cause lost: %v", wrapped)
		}
		var serialized bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&serialized, &slog.HandlerOptions{ReplaceAttr: redactAttribute}))
		logger.Error("native asynchronous rejection", slog.Any("error", wrapped))
		if !strings.Contains(serialized.String(), "permission_violation") || !strings.Contains(serialized.String(), "async-publish") ||
			strings.Contains(serialized.String(), "private.subject") {
			t.Fatalf("native async diagnostics: %s", serialized.String())
		}

	case <-ctx.Done():
		t.Fatal("native asynchronous error callback missing", ctx.Err())
	}
	denied,
		err := connectWithOptions(ctx,
		url,
		nats.UserInfo("synthetic",
			"wrong-secret"),
		nats.Secure(&tls.Config{RootCAs: roots,
			ServerName: "localhost",
			MinVersion: tls.VersionTLS12}))
	if denied != nil {
		denied.Close()
		t.Fatal("unauthorized connection returned")
	}
	if !errors.Is(err, nats.ErrAuthorization) {
		t.Fatalf("auth original cause lost: %v", err)
	}
}

func writeSyntheticCertificate(t *testing.T, dir string) *x509.CertPool {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1),
		Subject:     pkix.Name{CommonName: "synthetic-native-server"},
		DNSNames:    []string{"localhost"},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore:   time.Now().Add(-time.Minute),
		NotAfter:    time.Now().Add(time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	if err = os.WriteFile(filepath.Join(dir, "server.pem"), certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(dir,
		"server.key"),
		pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY",
			Bytes: x509.MarshalPKCS1PrivateKey(key)}),
		0o600); err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(certPEM) {
		t.Fatal("certificate rejected")
	}
	return roots
}

func TestNativeReconnectAfterServerRestart(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "nats.conf"), []byte("port: 4222\njetstream {store_dir: \"/data\"}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	url, id := startNativeServer(t, dir)
	disconnected := make(chan error, 4)
	reconnected := make(chan struct{}, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	conn, err := connectWithOptions(ctx, url, nats.ReconnectWait(20*time.Millisecond), nats.ReconnectJitter(0, 0), nats.MaxReconnects(-1),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			select {
			case disconnected <- err:
			default:
			}
		}),
		nats.ReconnectHandler(func(*nats.Conn) {
			select {
			case reconnected <- struct{}{}:
			default:
			}
		}))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if output, restartErr := docker("restart", id); restartErr != nil {
		t.Fatalf("restart: %v %s", restartErr, output)
	}
	restartedURL, addressErr := nativeServerAddress(id)
	if addressErr != nil {
		t.Fatal(addressErr)
	}
	if restartedURL != url {
		t.Fatalf("Docker restart changed mapped endpoint: %s -> %s", url, restartedURL)
	}
	select {
	case <-disconnected:
	case <-ctx.Done():
		t.Fatal("disconnect callback missing", ctx.Err())
	}
	select {
	case <-reconnected:
	case <-ctx.Done():
		t.Fatal("reconnect callback missing", ctx.Err())
	}
	if err = runConnection(ctx, conn); err != nil {
		t.Fatal(err)
	}
}

func TestNativeReadinessDeadlineAcrossServers(t *testing.T) {
	urls := make([]string, 3)
	for i := range urls {
		urls[i], _ = silentNativeEndpoint(t)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Millisecond)
	defer cancel()
	start := time.Now()
	conn, err := connectWithOptions(ctx, strings.Join(urls, ","), nats.DontRandomize(), nats.NoReconnect())
	if conn != nil {
		conn.Close()
		t.Fatal("silent servers returned usable connection")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("absolute deadline cause lost: %v", err)
	}
	var network net.Error
	if !errors.As(err, &network) {
		t.Fatalf("native network cause lost: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 400*time.Millisecond {
		t.Fatalf("per-server attempts exceeded absolute readiness budget: %s", elapsed)
	}
}

func TestNativeCancelAfterHandshakeStarts(t *testing.T) {
	url, accepted := silentNativeEndpoint(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		conn, err := connectWithOptions(ctx, url, nats.NoReconnect())
		if conn != nil {
			conn.Close()
			err = errors.Join(err, errors.New("canceled connection returned"))
		}
		result <- err
	}()
	select {
	case <-accepted:
	case <-time.After(time.Second):
		t.Fatal("handshake did not start")
	}
	start := time.Now()
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("original cancellation lost: %v", err)
		}
		if time.Since(start) > 400*time.Millisecond {
			t.Fatal("cancellation waited for native per-server timeout")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("initial setup did not join")
	}
}

func silentNativeEndpoint(t *testing.T) (url string, accepted <-chan struct{}) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	done := make(chan struct{})
	stop := make(chan struct{})
	go func() {
		defer close(done)
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		defer conn.Close()
		close(started)
		<-stop
	}()
	t.Cleanup(func() {
		close(stop)
		if closeErr := listener.Close(); closeErr != nil && !errors.Is(closeErr, net.ErrClosed) {
			t.Error(closeErr)
		}
		<-done
	})
	return "nats://" + listener.Addr().String(), started
}

type nativeTLSDecisionDialer struct {
	net.Dialer
	checked chan struct{}
}

func (d *nativeTLSDecisionDialer) SkipTLSHandshake() bool { close(d.checked); return false }

type lateNativeDialer struct {
	started chan struct{}
	release chan struct{}
	conn    net.Conn
}

func (d *lateNativeDialer) Dial(network, address string) (net.Conn, error) {
	conn, err := net.Dial(network, address)
	if err != nil {
		return nil, err
	}
	d.conn = conn
	close(d.started)
	<-d.release
	return conn, nil
}

func TestNativeLateCustomDialIsJoinedAndRejected(t *testing.T) {
	dialer := &lateNativeDialer{started: make(chan struct{}), release: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		conn, err := connectWithOptions(ctx, nativeServerURL, nats.SetCustomDialer(dialer))
		if conn != nil {
			conn.Close()
			err = errors.Join(err, errors.New("late canceled connection returned"))
		}
		result <- err
	}()
	select {
	case <-dialer.started:
	case <-time.After(time.Second):
		t.Fatal("custom dial not entered")
	}
	cancel()
	// A synchronous custom Dial is joined, not abandoned on cancellation.
	select {
	case err := <-result:
		t.Fatalf("returned before custom dial joined: %v", err)
	default:
	}
	close(dialer.release)
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("late custom result lost cancellation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("late custom result not joined")
	}
	if _, err := dialer.conn.Write([]byte("PING\r\n")); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("late native socket not closed: %v", err)
	}
}
