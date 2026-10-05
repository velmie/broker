# Request and reply

`NewReplyHandler` decodes a request, resolves an approved reply destination,
invokes a typed callback and publishes its response. Only successful reply
publication requests `Handled` for the original delivery.

Bind a fixed reply publisher in application wiring:

```go
package orders

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/velmie/broker"
)

type Order struct {
	ID string `json:"id"`
}

type Receipt struct {
	OrderID string `json:"order_id"`
}

func NewHandler(replies broker.Publisher, create func(context.Context, Order) (Receipt, error)) broker.Handler {
	resolve := func(_ context.Context, request broker.Message) (broker.ReplyTarget, error) {
		if request.ID == "" {
			return broker.ReplyTarget{}, errors.New("request ID is required")
		}
		return broker.ReplyTarget{
			Publisher: replies,
			Metadata: broker.MessageMetadata{
				// Repeated processing of this request keeps the same response identity.
				ID: request.ID + ".reply",
				Headers: []broker.Header{
					{Name: "Correlation-Id", Value: []byte(request.ID)},
				},
			},
		}, nil
	}
	return broker.NewReplyHandler(broker.DecodeJSON[Order], broker.EncoderFunc(json.Marshal), resolve, create)
}
```

The resolver runs before business work and must return a usable publisher and a
nonempty, stable response ID. This example correlates replies with the request
ID. Preserve your application's existing correlation convention when migrating.
Ensure the derived ID satisfies the selected adapter's limits.

For dynamic routing, map trusted application policy to already approved
publishers. Never publish blindly to a destination supplied in an input header.
Use `NewTypedHandler` for commands that do not require a reply.

## Failure boundaries

Business effects, reply publication and request acknowledgment are separate
steps. If publication fails after an effect, the request remains unsettled. If
acknowledgment fails after publication, the response may already be available.
Redelivery can repeat both effects and replies. Stable identities and
[idempotent application effects](message-identity.md#duplicate-processing) remain
necessary where duplicates matter.

`WithRedelivery` classifies business callback failures only. It does not classify
resolution, encoding or publication errors. Do not infer retry safety from the
absence of a successful return.

The requester owns its response subscription, correlation, timeout and duplicate
handling. The helper implements the responder's processing path and inherits
the reply publisher's documented acceptance guarantee.
