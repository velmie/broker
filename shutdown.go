package broker

import "time"

const (
	ShutdownGraceful ShutdownMode = iota
	ShutdownCancel
)

// ShutdownMode chooses whether admitted callbacks drain or receive cancellation.
type ShutdownMode uint8

// ShutdownPolicy belongs to each consumer. Zero is graceful with no implicit
// limit. A positive GracePeriod requests cancellation after that duration; it
// cannot terminate an uncooperative callback. Cancel mode requires a zero period.
type ShutdownPolicy struct {
	Mode        ShutdownMode
	GracePeriod time.Duration
}
