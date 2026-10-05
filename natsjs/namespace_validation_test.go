package natsjs_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/velmie/broker/natsjs/v3"
)

func TestConsumerNamespaceValidationIsLocal(t *testing.T) {
	// An unopened borrowed connection can validate local configuration. No server
	// or receive resource may be acquired by the constructor.
	connection := &nats.Conn{Opts: nats.Options{FlusherTimeout: time.Second}}
	for _, test := range []struct {
		name, domain, prefix string
		valid                bool
	}{
		{"default", "", "", true}, {"domain", "edge-west", "", true},
		{"domain native token", "edge/west", "", true},
		{"prefix", "", "$JS.edge.API", true}, {"trailing dot", "", "$JS.edge.API.", true},
		{"custom prefix", "", "management.jetstream", true},
		{"conflict", "edge", "$JS.edge.API", false},
		{"dotted domain", "edge.west", "", false}, {"wildcard domain", "*", "", false},
		{"tail wildcard domain", ">", "", false}, {"space domain", "edge west", "", false},
		{"control domain", "edge\x01west", "", false}, {"unicode space domain", "edge\u00a0west", "", false},
		{"invalid utf8 domain", "edge\xff", "", false},
		{"empty prefix token", "", "$JS..API", false}, {"double trailing dot", "", "$JS.edge.API..", false},
		{"dot only", "", ".", false}, {"wildcard prefix", "", "$JS.*.API", false},
		{"tail wildcard prefix", "", "$JS.>", false}, {"whitespace prefix", "", "$JS. edge.API", false},
		{"control prefix", "", "$JS.\x7f.API", false}, {"unicode whitespace prefix", "", "$JS.\u2003.API", false},
		{"invalid utf8 prefix", "", "$JS.\xff.API", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := natsjs.ConsumerConfig{Stream: "ORDERS", Consumer: "worker", Subject: "orders", Domain: test.domain, APIPrefix: test.prefix}
			consumer, err := natsjs.NewConsumer(connection, config)
			if test.valid {
				if err != nil || consumer == nil {
					t.Fatalf("valid namespace rejected: %v", err)
				}
				if err = consumer.Validate(); err != nil {
					t.Fatal(err)
				}
				return
			}
			var operation *natsjs.OperationError
			if !errors.As(err, &operation) || operation.Operation != "validate" || operation.Source != "ORDERS/worker" {
				t.Fatalf("missing local diagnostic: %v", err)
			}
			for _, value := range []string{test.domain, test.prefix} {
				if len(value) > 1 && strings.Contains(err.Error(), value) {
					t.Errorf("untrusted namespace interpolated: %q", err)
				}
			}
			if connection.NumSubscriptions() != 0 {
				t.Fatal("namespace validation acquired a subscription")
			}
		})
	}
}
