package idempotency_test

import (
	"context"
	"sync"
	"time"

	"github.com/velmie/idempo"
)

type storeSpy struct {
	inner idempo.Store

	mu sync.Mutex

	createCalls      int
	getCalls         int
	setResponseCalls int
	deleteCalls      int

	failSetResponseErr       error
	failSetResponseRemaining int

	deleteOnCreateOnce bool
	deletedOnCreate    bool
}

func newStoreSpy(inner idempo.Store) *storeSpy {
	return &storeSpy{inner: inner}
}

func (s *storeSpy) Create(
	ctx context.Context,
	key string,
	fp idempo.Fingerprint,
	ttl time.Duration,
) (entry *idempo.Entry, created bool, err error) {
	s.mu.Lock()
	s.createCalls++
	deleteOnCreate := s.deleteOnCreateOnce && !s.deletedOnCreate
	s.mu.Unlock()

	entry, created, err = s.inner.Create(ctx, key, fp, ttl)
	if err != nil {
		return nil, false, err
	}

	if !created && entry != nil && entry.Response == nil && deleteOnCreate {
		s.mu.Lock()
		if !s.deletedOnCreate {
			s.deletedOnCreate = true
			s.mu.Unlock()
			_ = s.Delete(ctx, key, entry.Token)
		} else {
			s.mu.Unlock()
		}
	}

	return entry, created, nil
}

func (s *storeSpy) Get(ctx context.Context, key string) (entry *idempo.Entry, err error) {
	s.mu.Lock()
	s.getCalls++
	s.mu.Unlock()

	return s.inner.Get(ctx, key)
}

func (s *storeSpy) SetResponse(
	ctx context.Context,
	key, token string,
	resp *idempo.Response,
	ttl time.Duration,
) (err error) {
	s.mu.Lock()
	s.setResponseCalls++
	shouldFail := s.failSetResponseRemaining > 0
	if shouldFail {
		s.failSetResponseRemaining--
	}
	failErr := s.failSetResponseErr
	s.mu.Unlock()

	if shouldFail {
		return failErr
	}

	return s.inner.SetResponse(ctx, key, token, resp, ttl)
}

func (s *storeSpy) Delete(ctx context.Context, key, token string) (err error) {
	s.mu.Lock()
	s.deleteCalls++
	s.mu.Unlock()

	return s.inner.Delete(ctx, key, token)
}

type storeSpySnapshot struct {
	CreateCalls      int
	GetCalls         int
	SetResponseCalls int
	DeleteCalls      int
}

func (s *storeSpy) Snapshot() storeSpySnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return storeSpySnapshot{
		CreateCalls:      s.createCalls,
		GetCalls:         s.getCalls,
		SetResponseCalls: s.setResponseCalls,
		DeleteCalls:      s.deleteCalls,
	}
}
