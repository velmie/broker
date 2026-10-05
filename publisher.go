package broker

import "context"

// Publisher is destination-bound. Success means the adapter's documented
// publication guarantee, not downstream processing or cross-resource atomicity.
type Publisher interface {
	Publish(context.Context, Message) error
}

// PublisherFunc adapts a publication function to a destination-bound Publisher.
type PublisherFunc func(context.Context, Message) error

// PublisherMiddleware wraps a publisher. It must preserve context, message data
// and errors unless its documented policy changes them. Input data is read-only.
type PublisherMiddleware func(Publisher) Publisher

// MessageMetadata supplies application-owned identity and ordered headers.
// A logical ID is optional for ordinary publication. Adapter restrictions still
// apply. Header storage is read-only until the publication call returns.
type MessageMetadata struct {
	ID      string
	Headers []Header
}

// PublishFunc publishes an application value without exposing a raw message port.
type PublishFunc[T any] func(context.Context, T) error

// NewTypedPublisher resolves metadata, encodes and publishes in that order.
// It never generates an ID or retries. Repeated logical publications need stable
// application-supplied IDs. Failures retain their metadata, encode or publish
// stage and original cause. Success has the underlying publisher's guarantee.
// All arguments are required. Their concurrency must match the caller's use.
func NewTypedPublisher[T any](
	encoder Encoder, publisher Publisher, metadata func(context.Context, T) (MessageMetadata, error),
) PublishFunc[T] {
	return func(ctx context.Context, value T) error {
		fields, err := metadata(ctx, value)
		if err != nil {
			return &StageError{Stage: StageMetadata, Cause: err}
		}
		return publishValue(ctx, encoder, publisher, fields, value)
	}
}

// WrapPublisher composes first-outermost: a(b(p)). A later call wraps the current
// publisher. An empty list returns publisher. The publisher and wrappers are
// required to be non-nil. A wrapper must copy any caller storage it changes.
func WrapPublisher(publisher Publisher, middleware ...PublisherMiddleware) Publisher {
	for i := len(middleware) - 1; i >= 0; i-- {
		publisher = middleware[i](publisher)
	}
	return publisher
}

func (f PublisherFunc) Publish(ctx context.Context, message Message) error { return f(ctx, message) }

func publishValue(ctx context.Context, encoder Encoder, publisher Publisher, metadata MessageMetadata, value any) error {
	body, err := encoder.Encode(value)
	if err != nil {
		return &StageError{Stage: StageEncode, Cause: err}
	}
	if err = publisher.Publish(ctx, Message{ID: metadata.ID, Headers: metadata.Headers, Body: body}); err != nil {
		return &StageError{Stage: StagePublish, Cause: err}
	}
	return nil
}
