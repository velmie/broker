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

// Reject requests terminal rejection for this subscription. It does not promise
// publication to a dead-letter destination. Cause is optional diagnostic evidence.
type Reject struct {
	Cause error
}

func (Handled) DeliveryDisposition()    {}
func (RetryAfter) DeliveryDisposition() {}
func (Reject) DeliveryDisposition()     {}
