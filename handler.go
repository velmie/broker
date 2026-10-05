package broker

import "context"

// Handler describes processing and its declared requirements. Callbacks, codecs
// and middleware must support the concurrency of their configured consumers.
type Handler struct {
	callback          HandlerFunc
	requirements      []Requirement
	bindCorrelationID bool
}

// HandlerFunc chooses one disposition. A non-nil error suppresses that result
// and stops the run. Raw callback errors have no implicit retry classification.
type HandlerFunc func(context.Context, Delivery) (Disposition, error)

// HandlerOption configures a handler before it is used.
type HandlerOption func(*Handler)

// NewHandler constructs a descriptor. The callback and each option are required
// to be non-nil. A zero Handler is not executable.
func NewHandler(callback HandlerFunc, options ...HandlerOption) Handler {
	h := Handler{callback: callback}
	for _, option := range options {
		option(&h)
	}
	return h
}

// NewTypedHandler decodes a DTO and invokes an ordinary business callback.
// Business errors get an outer StageHandle. Success requests Handled.
func NewTypedHandler[T any](decoder DecoderFunc[T], callback func(context.Context, T) error, options ...HandlerOption) Handler {
	return NewTypedDeliveryHandler(decoder, func(ctx context.Context, _ Delivery, value T) (Disposition, error) {
		if err := callback(ctx, value); err != nil {
			return nil, &StageError{Stage: StageHandle, Cause: err}
		}
		return Handled{}, nil
	}, options...)
}

// NewTypedDeliveryHandler decodes a DTO while leaving metadata access and the
// disposition to the transport callback. Its callback errors remain unclassified.
func NewTypedDeliveryHandler[T any](
	decoder DecoderFunc[T], callback func(context.Context, Delivery, T) (Disposition, error), options ...HandlerOption,
) Handler {
	var h Handler
	h.callback = func(ctx context.Context, delivery Delivery) (Disposition, error) {
		message := delivery.Message()
		value, err := decode(decoder, message.Body)
		if err != nil {
			return nil, err
		}
		if h.bindCorrelationID {
			if err := bindCorrelationID(&value, message.Headers); err != nil {
				return nil, &StageError{Stage: StageMetadata, Cause: err}
			}
		}
		return callback(ctx, delivery, value)
	}
	for _, option := range options {
		option(&h)
	}
	return h
}

func (h Handler) Handle(ctx context.Context, delivery Delivery) (Disposition, error) {
	return h.callback(ctx, delivery)
}

// Requirements returns a copy of the descriptor's declarations.
func (h Handler) Requirements() []Requirement { return append([]Requirement(nil), h.requirements...) }

// WithRequirements snapshots the list. Third-party values remain immutable.
func WithRequirements(requirements ...Requirement) HandlerOption {
	snapshot := append([]Requirement(nil), requirements...)
	return func(h *Handler) { h.requirements = append(h.requirements, snapshot...) }
}

// WithCorrelationIDBinding enables one optional CorrelationIDSetter call after
// decoding. Ambiguous or non-UTF-8 metadata fails before business execution.
func WithCorrelationIDBinding() HandlerOption { return func(h *Handler) { h.bindCorrelationID = true } }
