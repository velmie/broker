package broker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
)

const registrationValidate = "validate"

// Coordinator is single-use. It validates every local registration before any
// Run starts. It does not provide a global remote-readiness barrier.
type Coordinator struct {
	mu      sync.Mutex
	used    bool
	entries []registration
}

func NewCoordinator() *Coordinator { return &Coordinator{} }

// Add registers a source under a diagnostic name made of letters, numbers,
// dots, dashes and underscores. Factories and handlers are required.
func (c *Coordinator) Add(name string, factory ConsumerFactory, handler Handler) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.used {
		return errors.New("coordinator already used")
	}
	if name == "" || strings.IndexFunc(name, func(r rune) bool {
		valid := r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '.' || r == '-' || r == '_'
		return !valid
	}) >= 0 {
		return errors.New("invalid registration name")
	}
	for _, entry := range c.entries {
		if entry.name == name {
			return fmt.Errorf("duplicate registration %q", name)
		}
	}
	c.entries = append(
		c.entries,
		registration{
			name:    name,
			factory: factory,
			handler: handler,
		},
	)
	return nil
}

// Run requests peer shutdown after the first erroneous Run return, joins all
// runs and preserves their causes. A successful finite source does not stop peers.
func (c *Coordinator) Run(parent context.Context) error {
	return c.run(parent, false)
}

// RunService validates that all consumers implement ServiceConsumer before
// starting any of them. A parent stop alone succeeds; independent run and
// cleanup failures retain their registration and causes. A successful finite
// source does not stop peers. The coordinator waits for every run to join.
func (c *Coordinator) RunService(parent context.Context) error {
	return c.run(parent, true)
}

func (c *Coordinator) run(parent context.Context, service bool) error {
	c.mu.Lock()
	if c.used {
		c.mu.Unlock()
		return errors.New("coordinator already used")
	}
	c.used = true
	entries := append([]registration(nil), c.entries...)
	c.mu.Unlock()
	if err := context.Cause(parent); err != nil && !service {
		return err
	}
	consumers := make([]Consumer, len(entries))
	for i, entry := range entries {
		consumer, err := entry.factory()
		if err != nil {
			return &RegistrationError{
				Name:      entry.name,
				Operation: "factory",
				Cause:     err,
			}
		}
		if err = consumer.Validate(entry.handler.Requirements()...); err != nil {
			return &RegistrationError{
				Name:      entry.name,
				Operation: registrationValidate,
				Cause:     err,
			}
		}
		consumers[i] = consumer
	}
	if service {
		for i, consumer := range consumers {
			if _, ok := consumer.(ServiceConsumer); !ok {
				return &RegistrationError{
					Name:      entries[i].name,
					Operation: registrationValidate,
					Cause:     errors.New("consumer does not support RunService"),
				}
			}
		}
		if parent.Err() != nil {
			return nil
		}
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	results := make(chan error, len(entries))
	for i, entry := range entries {
		go func(consumer Consumer, entry registration) {
			var err error
			if service {
				err = consumer.(ServiceConsumer).RunService(ctx, entry.handler)
			} else {
				err = consumer.Run(ctx, entry.handler)
			}
			if err != nil {
				err = &RegistrationError{
					Name:      entry.name,
					Operation: "run",
					Cause:     err,
				}
				cancel()
			}
			results <- err
		}(consumers[i], entry)
	}
	var all []error
	for range entries {
		if err := <-results; err != nil {
			all = append(all, err)
		}
	}
	if err := context.Cause(parent); err != nil && !service {
		all = append(all, err)
	}
	return errors.Join(all...)
}

type registration struct {
	name    string
	factory ConsumerFactory
	handler Handler
}
