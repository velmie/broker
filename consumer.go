package broker

import "context"

// Consumer validates a full requirement combination locally. Run repeats local
// validation, checks its remote binding and joins all owned work before returning.
// Its context requests shutdown according to its adapter's ShutdownPolicy.
type Consumer interface {
	Validate(...Requirement) error
	Run(context.Context, Handler) error
}

// ConsumerFactory binds a source locally without acquiring resources or starting
// workers. A nil error requires a non-nil consumer, which owns acquisition and
// cleanup inside Run.
type ConsumerFactory func() (Consumer, error)
