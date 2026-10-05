package broker

import (
	"context"
	"errors"
)

// ReplyTarget identifies an approved destination and stable response metadata.
// Publisher must be usable and Metadata.ID nonempty. Headers remain read-only
// until reply publication returns. Request identity and response identity are
// distinct; the resolver supplies any desired correlation mapping explicitly.
type ReplyTarget struct {
	Publisher Publisher
	Metadata  MessageMetadata
}

// ReplyResolver validates routing and response identity before business work.
// It must select an authorized destination, never blindly trust an input header,
// and return the same response ID for repeated processing of the same request.
// A fixed destination can use a closure. Returning no destination is an error.
type ReplyResolver func(context.Context, Message) (ReplyTarget, error)

// NewReplyHandler decodes and optionally binds correlation metadata, resolves an
// approved reply target, invokes the business callback, then encodes and publishes
// its response. Only successful publication requests Handled. Only callback
// failures get StageHandle and can enter WithRedelivery's application classifier.
//
// All constructor arguments are required. A one-way request uses NewTypedHandler.
// Business effects, reply publication and request settlement are not atomic.
// Lost responses and redelivery can repeat effects or replies. Stable identity
// does not replace application idempotency. The adapter's publication guarantee
// applies; this helper does not wait for downstream response processing.
func NewReplyHandler[Q, R any](
	decoder DecoderFunc[Q], encoder Encoder, resolver ReplyResolver,
	callback func(context.Context, Q) (R, error), options ...HandlerOption,
) Handler {
	return NewTypedDeliveryHandler(decoder, func(ctx context.Context, delivery Delivery, request Q) (Disposition, error) {
		target, err := resolver(ctx, delivery.Message())
		if err != nil {
			return nil, &StageError{Stage: StageResolve, Cause: err}
		}
		if target.Publisher == nil {
			return nil, &StageError{Stage: StageResolve, Cause: errors.New("reply Publisher: missing destination")}
		}
		if target.Metadata.ID == "" {
			return nil, &StageError{Stage: StageResolve, Cause: errors.New("reply Metadata.ID: must not be empty")}
		}
		response, err := callback(ctx, request)
		if err != nil {
			return nil, &StageError{Stage: StageHandle, Cause: err}
		}
		if err := publishValue(ctx, encoder, target.Publisher, target.Metadata, response); err != nil {
			return nil, err
		}
		return Handled{}, nil
	}, options...)
}
