package broker

import (
	"context"
	"errors"
)

// NewTypedPublisherWithResult resolves metadata, encodes a value and publishes it
// through middleware while retaining the native result. It neither retries nor
// generates IDs. Failures preserve their stage and original cause. A result can
// accompany an error, including an error returned by middleware after publication.
//
// Wrappers are constructed for each invocation. On success, middleware must call
// next synchronously exactly once before returning. Skipping publication with an
// error preserves that error. A second call is rejected before publication, even
// when middleware ignores the rejection. A swallowed publication error remains
// an error. Asynchronous calls to next are outside this contract.
//
// All arguments are required. Concurrent invocations keep their results separate;
// encoder, metadata, publish and shared middleware closures must support the
// caller's concurrency. The middleware slice is copied. Shared middleware state
// belongs to the supplied closures; wrapper-created state is per invocation.
// Middleware must preserve read-only input ownership.
func NewTypedPublisherWithResult[T, R any](
	encoder Encoder, publish func(context.Context, Message) (R, error),
	metadata func(context.Context, T) (MessageMetadata, error), middleware ...PublisherMiddleware,
) func(context.Context, T) (R, error) {
	middleware = append([]PublisherMiddleware(nil), middleware...)
	return func(ctx context.Context, value T) (R, error) {
		var result R
		fields, err := metadata(ctx, value)
		if err != nil {
			return result, &StageError{Stage: StageMetadata, Cause: err}
		}
		body, err := encoder.Encode(value)
		if err != nil {
			return result, &StageError{Stage: StageEncode, Cause: err}
		}
		called := false
		var publicationErr, contractErr error
		terminal := PublisherFunc(func(ctx context.Context, message Message) error {
			if called {
				contractErr = publicationMiddlewareError("repeated_publication", "publisher middleware called next more than once")
				return contractErr
			}
			called = true
			result, publicationErr = publish(ctx, message)
			return publicationErr
		})
		err = WrapPublisher(terminal, middleware...).Publish(ctx, Message{ID: fields.ID, Headers: fields.Headers, Body: body})
		if !called && err == nil {
			contractErr = publicationMiddlewareError("missing_publication", "publisher middleware returned success without publication")
		}
		if publicationErr != nil && !errors.Is(err, publicationErr) {
			err = errors.Join(err, publicationErr)
		}
		if contractErr != nil && !errors.Is(err, contractErr) {
			err = errors.Join(err, contractErr)
		}
		if err != nil {
			return result, &StageError{Stage: StagePublish, Cause: err}
		}
		return result, nil
	}
}

func publicationMiddlewareError(reason, message string) error {
	return &FieldError{Field: "PublisherMiddleware", Reason: reason, Cause: errors.New(message)}
}
