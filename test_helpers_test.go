package broker_test

import (
	"context"

	"github.com/velmie/broker"
)

type command struct{ ID string }
type delivery struct{ body string }

func (d delivery) Message() broker.Message { return broker.Message{Body: []byte(d.body)} }
func (delivery) Source() string            { return "orders" }

type consumer struct {
	validate func() error
	run      func(context.Context) error
}

func (c consumer) Validate(...broker.Requirement) error            { return c.validate() }
func (c consumer) Run(ctx context.Context, _ broker.Handler) error { return c.run(ctx) }
