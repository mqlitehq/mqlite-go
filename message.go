package mqlite

import (
	"context"
	"time"
)

// Message is a delivered message handle. The lock token stays attached to the
// handle so settlement cannot accidentally target another delivery attempt.
type Message struct {
	SequenceNumber int64
	Body           []byte
	MessageID      string
	GroupID        string
	CorrelationID  string
	ReplyTo        string
	Subject        string
	ContentType    string
	Properties     map[string]string
	DeliveryCount  int
	EnqueuedAt     time.Time
	VisibleAt      time.Time
	ExpiresAt      time.Time
	LockedUntil    time.Time

	queue     string
	lockToken string
	s         settler

	// localLeaseDeadline is derived from broker response time and uses Go's
	// monotonic clock. It is intentionally private: LockedUntil preserves the exact
	// wire value while the managed receiver gets a skew-resistant schedule.
	localLeaseDeadline    time.Time
	leaseResponseRTT      time.Duration
	leaseClockResolution  time.Duration
	leaseClockUnavailable bool
}

// AbandonOpts configures an Abandon operation.
type AbandonOpts struct {
	Delay time.Duration
}

// RejectOpts configures an explicit dead-letter operation.
type RejectOpts struct {
	Reason string
	Detail string
}

// LockToken returns this delivery attempt's fencing token. Most callers should
// settle through the Message methods and never need to handle it directly.
func (m *Message) LockToken() string { return m.lockToken }

// Complete permanently acknowledges a successfully processed message.
func (m *Message) Complete(ctx context.Context) error {
	return m.s.complete(ctx, m.queue, m.SequenceNumber, m.lockToken)
}

// Abandon ends this delivery attempt. Below the broker's delivery limit the
// message becomes available immediately or after the optional delay.
func (m *Message) Abandon(ctx context.Context, opts ...AbandonOpts) error {
	var opt AbandonOpts
	if len(opts) > 0 {
		opt = opts[0]
	}
	return m.s.abandon(ctx, m.queue, m.SequenceNumber, m.lockToken, durationMillis(opt.Delay))
}

// Reject moves the message directly to the dead-letter state.
func (m *Message) Reject(ctx context.Context, opts ...RejectOpts) error {
	var opt RejectOpts
	if len(opts) > 0 {
		opt = opts[0]
	}
	return m.s.reject(ctx, m.queue, m.SequenceNumber, m.lockToken, opt.Reason, opt.Detail)
}

// Defer sets the message aside for a later receive by sequence number.
func (m *Message) Defer(ctx context.Context) error {
	return m.s.deferMsg(ctx, m.queue, m.SequenceNumber, m.lockToken)
}

// Renew extends this delivery's live lock without increasing DeliveryCount. It
// cannot revive an expired lock.
func (m *Message) Renew(ctx context.Context) error {
	return m.s.renew(ctx, m.queue, m.SequenceNumber, m.lockToken)
}

type settler interface {
	complete(context.Context, string, int64, string) error
	abandon(context.Context, string, int64, string, int64) error
	reject(context.Context, string, int64, string, string, string) error
	deferMsg(context.Context, string, int64, string) error
	renew(context.Context, string, int64, string) error
}

func durationMillis(d time.Duration) int64 {
	return int64(d / time.Millisecond)
}
