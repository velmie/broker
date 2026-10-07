package idempotency_test

import (
	"context"
	"time"

	"github.com/velmie/idempo"
)

type storeSpy struct {
	idempo.Store
	create func(context.Context, string, idempo.Fingerprint, time.Duration) (*idempo.Entry, bool, error)
	get    func(context.Context, string) (*idempo.Entry, error)
	commit func(context.Context, string, string, *idempo.Response, time.Duration) error
	unlock func(context.Context, string, string) error
}

func (s *storeSpy) Create(ctx context.Context, key string, fp idempo.Fingerprint, ttl time.Duration) (*idempo.Entry, bool, error) {
	if s.create != nil {
		return s.create(ctx, key, fp, ttl)
	}
	return s.Store.Create(ctx, key, fp, ttl)
}
func (s *storeSpy) Get(ctx context.Context, key string) (*idempo.Entry, error) {
	if s.get != nil {
		return s.get(ctx, key)
	}
	return s.Store.Get(ctx, key)
}
func (s *storeSpy) SetResponse(ctx context.Context, key, token string, response *idempo.Response, ttl time.Duration) error {
	if s.commit != nil {
		return s.commit(ctx, key, token, response, ttl)
	}
	return s.Store.SetResponse(ctx, key, token, response, ttl)
}
func (s *storeSpy) Delete(ctx context.Context, key, token string) error {
	if s.unlock != nil {
		return s.unlock(ctx, key, token)
	}
	return s.Store.Delete(ctx, key, token)
}
