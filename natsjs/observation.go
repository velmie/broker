package natsjs

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/velmie/broker"
)

const (
	OperationDisposition = "disposition"
	OperationStartup     = "startup"
	OperationFetch       = "fetch"
	OperationProcess     = "process"
	OperationRenew       = "renew"
	OperationAck         = "ack"
	OperationNak         = "nak"
	OperationTerm        = "term"
	OperationStop        = "stop"
	OperationCleanup     = "cleanup"
	OperationRun         = "run"
	OutcomeStarted       = "started"
	OutcomeSucceeded     = "succeeded"
	OutcomeConfirmed     = "confirmed"
	OutcomeFailed        = "failed"
	OutcomeUnknown       = "unknown"
	OutcomeCanceled      = "canceled"
	OutcomeSuppressed    = "suppressed"
	OutcomeIdle          = "idle"
)

// Observer connects instrumentation at its owning adapter boundary. Both hooks
// are optional, synchronous and may run concurrently (renewal, shutdown and
// processing). They must return promptly and never wait for this Run to join.
// A panic disables subsequent hook calls for this Run. Failures of already
// entered hooks are retained as ObserverPanicError in Run's result without
// changing delivery decisions or replaying business code. No exporter or worker
// is owned. The application always owns cleanup of its instrumentation.
type Observer struct {
	// Begin derives correlation before decoding. The returned context must be
	// non-nil and preserve the input's cancellation ancestry. Processing and
	// later operation records use it. Terminal requests detach cancellation
	// only after admission and add their own finite operation deadline.
	Begin func(context.Context, DeliveryInfo) context.Context
	// Record receives original errors. A logging adapter must inspect/redact
	// application-specific causes and bound identifiers before serialization.
	// Messages, headers, credentials and raw ACK subjects are never supplied.
	Record func(context.Context, Observation)
}

// DeliveryInfo is detached native metadata, safe to retain. Sequence identifies
// the transport delivery for correlation; it is not a default metrics label.
type DeliveryInfo struct {
	Source   string
	Metadata nats.MsgMetadata
}

// Observation separates processing intent from transport results. Cause retains
// RetryAfter evidence even when Err is nil. Confirmed means a protocol response,
// not exactly-once effects or an enforceable processing lease. Unknown may have
// changed server state. Disposition is application-owned and must not be mutated.
type Observation struct {
	Source      string
	Metadata    nats.MsgMetadata
	Operation   string
	Outcome     string
	Duration    time.Duration
	Disposition broker.Disposition
	Cause       error
	Err         error
}

// ObserverPanicError preserves instrumentation panic evidence without formatting
// arbitrary values into logs. Value requires application-aware redaction.
type ObserverPanicError struct {
	Operation string
	Value     any
}

func (e *ObserverPanicError) Error() string {
	return fmt.Sprintf("observer %s panic (%T)", e.Operation, e.Value)
}
func (e *ObserverPanicError) Unwrap() error { cause, _ := e.Value.(error); return cause }

func outcome(err error) string {
	if err != nil {
		return OutcomeFailed
	}
	return OutcomeSucceeded
}
