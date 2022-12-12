package natsjs_test

import (
	"testing"

	"github.com/nats-io/nats-server/v2/server"

	natsserver "github.com/nats-io/nats-server/v2/test"
)

func runServerWithOptions(opts server.Options) *server.Server {
	return natsserver.RunServer(&opts)
}

func runBasicJetStreamServer() *server.Server {
	opts := natsserver.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	return runServerWithOptions(opts)
}

func shutdownJSServer(t *testing.T, s *server.Server) {
	t.Helper()
	s.Shutdown()
	s.WaitForShutdown()
}
