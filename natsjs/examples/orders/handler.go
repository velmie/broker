package orders

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/velmie/broker"
	"github.com/velmie/broker/natsjs/v3"
)

const (
	retryDelay      = 250 * time.Millisecond
	renewalInterval = 100 * time.Millisecond
)

// CreateOrderDTO is the external JSON payload. It is mapped explicitly into an
// application command so wire changes do not define the application's signature.
type CreateOrderDTO struct {
	OrderID     string `json:"order_id"`
	ProductCode string `json:"product_code"`
	Count       int    `json:"count"`
}

// OrderCreatedDTO is the reply payload after successful application processing.
type OrderCreatedDTO struct {
	OrderID string `json:"order_id"`
}

// NewHandler adapts a command-specific function. This transport deliberately
// terminates malformed payloads and retries only its chosen application error.
// No recovery loop, local callback replay or dead-letter publication is implied.
func NewHandler(create func(context.Context, CreateOrderCommand) error) broker.Handler {
	handler := broker.NewTypedHandler(broker.DecodeJSON[*CreateOrderDTO],
		func(ctx context.Context, dto *CreateOrderDTO) error {
			return create(ctx, orderCommand(dto))
		}, handlerOptions()...)
	return terminateMalformed(handler)
}

// NewReplyHandler publishes a receipt to the destination already approved by
// wiring. It ignores inbound routing headers and requires request identity before
// application work. Redelivery keeps the response ID but can repeat the effect.
func NewReplyHandler(create func(context.Context, CreateOrderCommand) error, replies broker.Publisher) broker.Handler {
	resolve := func(_ context.Context, request broker.Message) (broker.ReplyTarget, error) {
		if request.ID == "" {
			return broker.ReplyTarget{}, errors.New("Message.ID: required for reply identity")
		}
		return broker.ReplyTarget{Publisher: replies, Metadata: broker.MessageMetadata{
			ID: request.ID + ".created", Headers: []broker.Header{{Name: "Correlation-Id", Value: []byte(request.ID)}},
		}}, nil
	}
	handler := broker.NewReplyHandler(broker.DecodeJSON[*CreateOrderDTO], broker.EncoderFunc(json.Marshal), resolve,
		func(ctx context.Context, dto *CreateOrderDTO) (OrderCreatedDTO, error) {
			if err := create(ctx, orderCommand(dto)); err != nil {
				return OrderCreatedDTO{}, err
			}
			return OrderCreatedDTO{OrderID: dto.OrderID}, nil
		}, handlerOptions()...)
	return terminateMalformed(handler)
}

func orderCommand(dto *CreateOrderDTO) CreateOrderCommand {
	return CreateOrderCommand{ID: dto.OrderID, SKU: dto.ProductCode, Quantity: dto.Count}
}

func handlerOptions() []broker.HandlerOption {
	return []broker.HandlerOption{broker.WithKeepAlive(renewalInterval), broker.WithRedelivery(retryDelay, func(err error) bool {
		return errors.Is(err, ErrInventoryUnavailable)
	})}
}

func terminateMalformed(handler broker.Handler) broker.Handler {
	return handler.WithMiddleware(broker.HandlerMiddleware{
		Requirements: []broker.Requirement{natsjs.TerminationRequirement{}},
		Wrap: func(next broker.HandlerFunc) broker.HandlerFunc {
			return func(ctx context.Context, delivery broker.Delivery) (broker.Disposition, error) {
				result, err := next(ctx, delivery)
				// Only the helper's own outer decode failure is a poison payload.
				// A nested error from the application remains a business failure.
				if _, malformed := err.(*broker.DecodeError); malformed {
					return natsjs.Terminate{Cause: err}, nil
				}
				return result, err
			}
		},
	})
}
