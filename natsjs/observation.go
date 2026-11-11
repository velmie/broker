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
	// OutcomeInterrupted marks a pull receive or recovery lookup attempt selected for retry.
	// Err retains the cause. It does not indicate that Run has failed.
	OutcomeInterrupted = "interrupted"
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
	// Record receives original errors, including NATS server and SDK diagnostics.
	// Callbacks can record these unchanged. Observation fields do not include
	// message bodies, headers, credentials or raw ACK subjects.
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

// ObserverPanicError preserves an instrumentation panic. Error includes the
// panic value and its type. Error-valued panics remain available through Unwrap.
type ObserverPanicError struct {
	Operation string
	Value     any
}

func (e *ObserverPanicError) Error() string {
	return fmt.Sprintf("observer %s panic (%T): %v", e.Operation, e.Value, e.Value)
}
func (e *ObserverPanicError) Unwrap() error { cause, _ := e.Value.(error); return cause }

func outcome(err error) string {
	if err != nil {
		return OutcomeFailed
	}
	return OutcomeSucceeded
}
