package broker

import "time"

// Disposition is a data-only delivery decision. An adapter validates and executes
// supported concrete types. Implementing this marker does not imply support.
type Disposition interface{ DeliveryDisposition() }

// Handled requests successful transport progress after application work.
// The callback result alone does not establish transport confirmation.
type Handled struct{}

// RetryAfter requests native delayed redelivery. Cause is optional diagnostic
// evidence and does not make the callback fail. Delay is validated by the adapter.
type RetryAfter struct {
	Delay time.Duration
	Cause error
}

func (Handled) DeliveryDisposition()    {}
func (RetryAfter) DeliveryDisposition() {}
