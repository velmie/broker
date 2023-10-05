package broker

import (
	"context"
	"fmt"
	"sync"
)

// SubscribeFunc defines the signature for functions that Subscribe to a topic
type SubscribeFunc func(ctx context.Context) (Subscription, error)

// SubscriptionCoordinator is responsible for managing and coordinating multiple named subscriptions
type SubscriptionCoordinator struct {
	se            []SubscriptionEnvelope
	mu            sync.Mutex
	isset         map[string]struct{}
	subscriptions []Subscription
	l             Logger
}

// NewSubscriptionCoordinator initializes SubscriptionCoordinator
func NewSubscriptionCoordinator() *SubscriptionCoordinator {
	return &SubscriptionCoordinator{
		se:    make([]SubscriptionEnvelope, 0),
		isset: make(map[string]struct{}),
	}
}

// SubscribeAll attempts to Subscribe to all the topics defined in the SubscriptionCoordinator
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
			s.l.Info(fmt.Sprintf("subscribing to %s", ns.Name))
		}
		sub, err := ns.Subscribe(ctx)
		if err != nil {
			err = fmt.Errorf("failed to Subscribe to '%s': %w", ns.Name, err)
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

// AddSubscription adds a new subscription function with a unique Name
func (s *SubscriptionCoordinator) AddSubscription(name string, sf SubscribeFunc) error {
	return s.AddSubscriptionE(SubscriptionEnvelope{name, sf})
}

func (s *SubscriptionCoordinator) AddSubscriptionE(se SubscriptionEnvelope) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.isset[se.Name]; ok {
		return fmt.Errorf("subscription with Name %q is already added", se.Name)
	}
	s.se = append(s.se, se)
	s.isset[se.Name] = struct{}{}

	return nil
}

func (s *SubscriptionCoordinator) SetLogger(l Logger) {
	s.l = l
}

// SubscriptionEnvelope is a container for a named subscription function
type SubscriptionEnvelope struct {
	Name      string
	Subscribe SubscribeFunc
}

// CreateSubscribeFunc creates SubscribeFunc with the given parameters
func CreateSubscribeFunc(topic string, sub Subscriber, h Handler, opts ...SubscribeOption) SubscribeFunc {
	return func(ctx context.Context) (Subscription, error) {
		return sub.Subscribe(topic, h, opts...)
	}
}
