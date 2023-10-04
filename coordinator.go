package broker

import (
	"context"
	"fmt"
	"sync"
)

// SubscribeFunc defines the signature for functions that subscribe to a topic
type SubscribeFunc func(ctx context.Context) (Subscription, error)

// SubscriptionCoordinator is responsible for managing and coordinating multiple named subscriptions
type SubscriptionCoordinator struct {
	se            []subscriptionEnvelope
	mu            sync.Mutex
	isset         map[string]struct{}
	subscriptions []Subscription
	l             Logger
}

// NewSubscriptionCoordinator initializes SubscriptionCoordinator
func NewSubscriptionCoordinator() *SubscriptionCoordinator {
	return &SubscriptionCoordinator{
		se:    make([]subscriptionEnvelope, 0),
		isset: make(map[string]struct{}),
	}
}

// SubscribeAll attempts to subscribe to all the topics defined in the SubscriptionCoordinator
func (s *SubscriptionCoordinator) SubscribeAll(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	done := ctx.Done()
	for _, ns := range s.se {
		select {
		case <-done:
			return ctx.Err()
		default:
		}
		if s.l != nil {
			s.l.Info(fmt.Sprintf("subscribing to %s", ns.name))
		}
		sub, err := ns.subscribe(ctx)
		if err != nil {
			err = fmt.Errorf("failed to subscribe to '%s': %w", ns.name, err)
			if s.l != nil {
				s.l.Error(err.Error())
			}
			return err
		}
		s.subscriptions = append(s.subscriptions, sub)
	}
	return nil
}

// UnsubscribeAll attempts to unsubscribe from all active subscriptions managed by the SubscriptionCoordinator
func (s *SubscriptionCoordinator) UnsubscribeAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, sub := range s.subscriptions {
		if err := sub.Unsubscribe(); err != nil {
			if s.l != nil {
				s.l.Error(fmt.Sprintf("failed to unsubscribe from '%s': %s", sub.Topic(), err))
			}
			continue
		}
		if s.l != nil {
			s.l.Info(fmt.Sprintf("unsubscribed from %s", sub.Topic()))
		}
	}
	s.subscriptions = make([]Subscription, 0)
}

// AddSubscription adds a new subscription function with a unique name
func (s *SubscriptionCoordinator) AddSubscription(name string, sf SubscribeFunc) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.isset[name]; ok {
		return fmt.Errorf("subscription with name %q is already added", name)
	}
	s.se = append(s.se, subscriptionEnvelope{name, sf})
	s.isset[name] = struct{}{}

	return nil
}

func (s *SubscriptionCoordinator) SetLogger(l Logger) {
	s.l = l
}

// subscriptionEnvelope is a container for a named subscription function
type subscriptionEnvelope struct {
	name      string
	subscribe SubscribeFunc
}
