package natsjs_test

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"testing"

	"github.com/nats-io/nats-server/v2/server"

	natsserver "github.com/nats-io/nats-server/v2/test"
)

func runServerWithOptions(opts server.Options) *server.Server {
	return natsserver.RunServer(&opts)
}

func runBasicServer() *server.Server {
	opts := natsserver.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	return runServerWithOptions(opts)
}

func runServerOnPort(port int) *server.Server {
	opts := natsserver.DefaultTestOptions
	opts.Port = port
	opts.JetStream = true
	opts.NoHeaderSupport = false
	return runServerWithOptions(opts)
}

func restartBasicServer(t *testing.T, s *server.Server) *server.Server {
	opts := natsserver.DefaultTestOptions
	clientURL, err := url.Parse(s.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	port, err := strconv.Atoi(clientURL.Port())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	opts.Port = port
	opts.JetStream = true
	opts.StoreDir = s.JetStreamConfig().StoreDir
	s.Shutdown()
	s.WaitForShutdown()
	return runServerWithOptions(opts)
}

func shutdownServer(t *testing.T, s *server.Server) {
	t.Helper()
	var sd string
	if config := s.JetStreamConfig(); config != nil {
		sd = config.StoreDir
	}
	s.Shutdown()
	if sd != "" {
		if err := os.RemoveAll(sd); err != nil {
			t.Fatalf("Unable to remove storage %q: %v", sd, err)
		}
	}
	s.WaitForShutdown()
}

type stubLogger struct{}

func (s *stubLogger) Debug(v ...interface{}) {
	fmt.Println("DEBUG:")
	fmt.Println(v...)
}

func (s *stubLogger) Error(v ...interface{}) {
	fmt.Println("ERROR:")
	fmt.Println(v...)
}

func (s *stubLogger) Info(v ...interface{}) {
	fmt.Println("INFO:")
	fmt.Println(v...)
}
