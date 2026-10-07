package broker

import (
	"context"
	"errors"
	"time"
)

// HandlerMiddleware wraps processing and declares any additional requirements.
type HandlerMiddleware struct {
	Wrap         func(HandlerFunc) HandlerFunc
	Requirements []Requirement
}

// WithMiddleware composes first-outermost: a(b(h)). A later call wraps the
// current handler. All requirements are retained and their lists copied.
func (h Handler) WithMiddleware(middleware ...HandlerMiddleware) Handler {
	h.requirements = h.Requirements()
	for _, m := range middleware {
		h.requirements = append(h.requirements, m.Requirements...)
	}
	for i := len(middleware) - 1; i >= 0; i-- {
		h.callback = middleware[i].Wrap(h.callback)
	}
	return h
}

// WithKeepAlive declares adapter-owned renewal before decoding starts.
func WithKeepAlive(interval time.Duration) HandlerOption {
	return WithRequirements(KeepAliveRequirement{Interval: interval})
}

// WithRedelivery maps an explicitly staged business failure accepted by classifier
// to RetryAfter. It never replays the callback. The classifier is responsible for
// retry safety after partial application effects, including nested publication.
func WithRedelivery(delay time.Duration, classifier func(error) bool) HandlerOption {
	return func(h *Handler) {
		h.requirements = append(h.requirements, RedeliveryRequirement{})
		next := h.callback
		h.callback = func(ctx context.Context, delivery Delivery) (Disposition, error) {
			result, err := next(ctx, delivery)
			if err != nil && isProcessingError(err) && classifier(err) {
				return RetryAfter{Delay: delay, Cause: err}, nil
			}
			return result, err
		}
	}
}

func isProcessingError(err error) bool {
	for err != nil {
		if _, ok := err.(*DecodeError); ok {
			return false
		}
		if stage, ok := err.(*StageError); ok {
			return stage.Stage == StageHandle
		}
		err = errors.Unwrap(err)
	}
	return false
}
