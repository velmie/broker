package subscriber

import (
	"errors"

	"github.com/nats-io/nats.go"
	"github.com/velmie/broker"
)

const defaultPullMsgBatch = 1

type subscriptor struct {
	subj      string
	doubleAck bool
	subOpts   []nats.SubOpt
	ackOpts   []nats.AckOpt
}

type pullSubscription struct {
	subscriptor
	durable  string
	batch    int
	pullOpts []nats.PullOpt
}

// PullSubscription allows to build pull subscription
func PullSubscription() *pullSubscription {
	return &pullSubscription{
		batch: defaultPullMsgBatch,
	}
}

// Durable sets durable
func (s *pullSubscription) Durable(drb string) *pullSubscription {
	s.durable = drb
	return s
}

// Batch sets batch for pull fetch
func (s *pullSubscription) Batch(b int) *pullSubscription {
	s.batch = b
	return s
}

// PullOptions specifies pull options
func (s *pullSubscription) PullOptions(opts ...nats.PullOpt) *pullSubscription {
	s.pullOpts = append(s.pullOpts, opts...)
	return s
}

// Subject sets the subject
func (s *pullSubscription) Subject(subj string) *pullSubscription {
	s.subj = subj
	return s
}

// SubOptions sets nats.SubOpt
func (s *pullSubscription) SubOptions(opts ...nats.SubOpt) *pullSubscription {
	s.subOpts = append(s.subOpts, opts...)
	return s
}

// AckOptions sets nats.AckOpt
func (s *pullSubscription) AckOptions(opts ...nats.AckOpt) *pullSubscription {
	s.ackOpts = append(s.ackOpts, opts...)
	return s
}

// DoubleAck specifies if double ack is required
func (s *pullSubscription) DoubleAck(da bool) *pullSubscription {
	s.doubleAck = da
	return s
}

func (s *pullSubscription) subscribe(js nats.JetStreamContext, h broker.Handler, opts ...broker.SubscribeOption) (*subscription, error) {
	if s.batch <= 0 {
		return nil, errors.New("pull subscriber batch must be greater than zero")
	}

	subOpts := append(s.subOpts, nats.ManualAck())

	nsub, err := js.PullSubscribe(s.subj, s.durable, subOpts...)
	if err != nil {
		return nil, err
	}

	ns := &subscription{
		subj:                s.subj,
		doubleAck:           s.doubleAck,
		nsub:                nsub,
		DefaultSubscription: brokerSubscription(s.subj, h, opts...),
		ackOpts:             s.ackOpts,
	}

	// start goroutine for handling messages pull
	go ns.handlePull(s.batch, s.pullOpts...)

	return ns, nil
}

type syncQueueSubscription struct {
	subscriptor
	queue string
}

// SyncQueueSubscription allows to sync queue subscription
func SyncQueueSubscription() *syncQueueSubscription {
	return &syncQueueSubscription{}
}

// Queue sets queue name
func (s *syncQueueSubscription) Queue(q string) *syncQueueSubscription {
	s.queue = q
	return s
}

// Subject sets subject
func (s *syncQueueSubscription) Subject(subj string) *syncQueueSubscription {
	s.subj = subj
	return s
}

// SubOptions sets nats.SubOpt
func (s *syncQueueSubscription) SubOptions(opts ...nats.SubOpt) *syncQueueSubscription {
	s.subOpts = append(s.subOpts, opts...)
	return s
}

// AckOptions sets nats.AckOpt
func (s *syncQueueSubscription) AckOptions(opts ...nats.AckOpt) *syncQueueSubscription {
	s.ackOpts = append(s.ackOpts, opts...)
	return s
}

// DoubleAck specifies if double ack is required
func (s *syncQueueSubscription) DoubleAck(da bool) *syncQueueSubscription {
	s.doubleAck = da
	return s
}

func (s *syncQueueSubscription) subscribe(js nats.JetStreamContext, h broker.Handler, opts ...broker.SubscribeOption) (*subscription, error) {
	subOpts := append(s.subOpts, nats.ManualAck())

	nsub, err := js.QueueSubscribeSync(s.subj, s.queue, subOpts...)
	if err != nil {
		return nil, err
	}

	ns := &subscription{
		subj:                s.subj,
		doubleAck:           s.doubleAck,
		nsub:                nsub,
		DefaultSubscription: brokerSubscription(s.subj, h, opts...),
		ackOpts:             s.ackOpts,
	}

	return ns, nil
}

type asyncQueueSubscription struct {
	subscriptor
	queue string
}

// AsyncQueueSubscription allows to build async queue subscription
func AsyncQueueSubscription() *asyncQueueSubscription {
	return &asyncQueueSubscription{}
}

// Subject sets subject
func (s *asyncQueueSubscription) Subject(subj string) *asyncQueueSubscription {
	s.subj = subj
	return s
}

// Queue sets queue name
func (s *asyncQueueSubscription) Queue(q string) *asyncQueueSubscription {
	s.queue = q
	return s
}

// SubOptions sets nats.SubOpt
func (s *asyncQueueSubscription) SubOptions(opts ...nats.SubOpt) *asyncQueueSubscription {
	s.subOpts = append(s.subOpts, opts...)
	return s
}

// AckOptions sets nats.AckOpt
func (s *asyncQueueSubscription) AckOptions(opts ...nats.AckOpt) *asyncQueueSubscription {
	s.ackOpts = append(s.ackOpts, opts...)
	return s
}

// DoubleAck specifies if double ack is required
func (s *asyncQueueSubscription) DoubleAck(da bool) *asyncQueueSubscription {
	s.doubleAck = da
	return s
}

func (s *asyncQueueSubscription) subscribe(js nats.JetStreamContext, h broker.Handler, opts ...broker.SubscribeOption) (*subscription, error) {
	subOpts := append(s.subOpts, nats.ManualAck())

	ns := &subscription{
		subj:                s.subj,
		doubleAck:           s.doubleAck,
		DefaultSubscription: brokerSubscription(s.subj, h, opts...),
		ackOpts:             s.ackOpts,
	}

	nsub, err := js.QueueSubscribe(s.subj, s.queue, ns.msgHandler, subOpts...)
	if err != nil {
		return nil, err
	}
	ns.nsub = nsub

	return ns, nil
}

type syncSubscription struct {
	subscriptor
}

// SyncSubscription builds new sync subscription
func SyncSubscription() *syncSubscription {
	return &syncSubscription{}
}

// Subject sets subject
func (s *syncSubscription) Subject(subj string) *syncSubscription {
	s.subj = subj
	return s
}

// SubOptions sets nats.SubOpt
func (s *syncSubscription) SubOptions(opts ...nats.SubOpt) *syncSubscription {
	s.subOpts = append(s.subOpts, opts...)
	return s
}

// AckOptions sets nats.AckOpt
func (s *syncSubscription) AckOptions(opts ...nats.AckOpt) *syncSubscription {
	s.ackOpts = append(s.ackOpts, opts...)
	return s
}

// DoubleAck specifies if double ack is required
func (s *syncSubscription) DoubleAck(da bool) *syncSubscription {
	s.doubleAck = da
	return s
}

func (s *syncSubscription) subscribe(js nats.JetStreamContext, h broker.Handler, opts ...broker.SubscribeOption) (*subscription, error) {
	subOpts := append(s.subOpts, nats.ManualAck())

	nsub, err := js.SubscribeSync(s.subj, subOpts...)
	if err != nil {
		return nil, err
	}

	ns := &subscription{
		subj:                s.subj,
		doubleAck:           s.doubleAck,
		nsub:                nsub,
		DefaultSubscription: brokerSubscription(s.subj, h, opts...),
		ackOpts:             s.ackOpts,
	}

	return ns, nil
}

type asyncSubscription struct {
	subscriptor
}

// AsyncSubscription builds new async subscription
func AsyncSubscription() *asyncSubscription {
	return &asyncSubscription{}
}

// Subject sets subject
func (s *asyncSubscription) Subject(subj string) *asyncSubscription {
	s.subj = subj
	return s
}

// SubOptions sets nats.SubOpt
func (s *asyncSubscription) SubOptions(opts ...nats.SubOpt) *asyncSubscription {
	s.subOpts = append(s.subOpts, opts...)
	return s
}

// AckOptions sets nats.AckOpt
func (s *asyncSubscription) AckOptions(opts ...nats.AckOpt) *asyncSubscription {
	s.ackOpts = append(s.ackOpts, opts...)
	return s
}

// DoubleAck specifies if double ack is required
func (s *asyncSubscription) DoubleAck(da bool) *asyncSubscription {
	s.doubleAck = da
	return s
}

func (s *asyncSubscription) subscribe(js nats.JetStreamContext, h broker.Handler, opts ...broker.SubscribeOption) (*subscription, error) {
	subOpts := append(s.subOpts, nats.ManualAck())

	ns := &subscription{
		subj:                s.subj,
		doubleAck:           s.doubleAck,
		DefaultSubscription: brokerSubscription(s.subj, h, opts...),
		ackOpts:             s.ackOpts,
	}

	nsub, err := js.Subscribe(s.subj, ns.msgHandler, subOpts...)
	if err != nil {
		return nil, err
	}
	ns.nsub = nsub

	return ns, nil
}

func brokerSubscription(subj string, h broker.Handler, opts ...broker.SubscribeOption) *broker.DefaultSubscription {
	// apply options to build default subscription
	o := broker.DefaultSubscribeOptions()
	for _, opt := range opts {
		if opt != nil {
			opt(o)
		}
	}
	return broker.NewDefaultSubscription(subj, o, opts, h)
}
