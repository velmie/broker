// Package orders demonstrates an application command and its message boundary.
package orders

import "errors"

// ErrInventoryUnavailable is an application error selected for delayed retry.
var ErrInventoryUnavailable = errors.New("inventory unavailable")

// CreateOrderCommand is an application input, independent of broker metadata
// and the external payload representation.
type CreateOrderCommand struct {
	ID       string
	SKU      string
	Quantity int
}
