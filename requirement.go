package broker

import "time"

// Requirement declares a capability needed by a handler. Third-party values must
// be immutable. Adapters validate the complete combination before processing.
type Requirement interface{ DeliveryRequirement() }

// RedeliveryRequirement requests native dynamic delayed redelivery.
type RedeliveryRequirement struct{}

// KeepAliveRequirement requests renewal while the callback runs. Interval is
// cadence, not an operation timeout. The adapter owns renewal and its budget.
type KeepAliveRequirement struct{ Interval time.Duration }

// AttemptsRequirement requests an AttemptReader, which can still report unknown.
type AttemptsRequirement struct{}

func (RedeliveryRequirement) DeliveryRequirement() {}
func (KeepAliveRequirement) DeliveryRequirement()  {}
func (AttemptsRequirement) DeliveryRequirement()   {}
