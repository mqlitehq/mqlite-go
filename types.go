package mqlite

import (
	"errors"
	"fmt"
	"time"
)

// queueNotFoundError is the queue-specific form of ErrNotFound. Keeping the
// hierarchy in the sentinel itself makes errors.Is portable between embedded
// MQLite and this remote-only SDK.
type queueNotFoundError struct{}

func (queueNotFoundError) Error() string { return "mqlite: queue not found" }

func (queueNotFoundError) Is(target error) bool { return target == ErrNotFound }

// Sentinel errors are stable categories returned by the remote broker client.
var (
	ErrLockLost             = errors.New("mqlite: lock lost")
	ErrUnauthenticated      = errors.New("mqlite: unauthenticated")
	ErrPermissionDenied     = errors.New("mqlite: permission denied")
	ErrKeyConflict          = errors.New("mqlite: access key conflict")
	ErrOutcomeUnknown       = errors.New("mqlite: outcome unknown")
	ErrInvalidArgument      = errors.New("mqlite: invalid argument")
	ErrNotFound             = errors.New("mqlite: not found")
	ErrUnsupported          = errors.New("mqlite: operation unsupported by broker")
	ErrQueueNotFound        = error(queueNotFoundError{})
	ErrDedupConflict        = errors.New("mqlite: message ID conflicts with existing body")
	ErrMessageTooLarge      = errors.New("mqlite: message too large")
	ErrNameConflict         = errors.New("mqlite: resource name conflict")
	ErrQueueNotEmpty        = errors.New("mqlite: queue is not empty")
	ErrSubscriptionNotFound = errors.New("mqlite: subscription not found")
	ErrSubscriptionNotEmpty = errors.New("mqlite: subscription is not empty")
	ErrSubscriptionLimit    = errors.New("mqlite: subscription limit reached")
	ErrGroupRequired        = errors.New("mqlite: message group ID required")
)

// OrderingMode controls queue ordering.
type OrderingMode string

const (
	OrderStandard   OrderingMode = "standard"
	OrderGroupFIFO  OrderingMode = "group_fifo"
	OrderStrictFIFO OrderingMode = "strict_fifo"
)

// State is a message lifecycle state accepted by Peek.
type State string

const (
	Active       State = "active"
	Locked       State = "locked"
	Deferred     State = "deferred"
	Scheduled    State = "scheduled"
	DeadLettered State = "dead_lettered"
)

// Filter is an expr-lang subscription predicate. An empty expression matches all.
type Filter struct {
	Expr string `json:"expr,omitempty"`
}

// OutMessage is a message to send. Body is opaque to the broker.
type OutMessage struct {
	Body          []byte
	MessageID     string
	GroupID       string
	CorrelationID string
	ReplyTo       string
	Subject       string
	ContentType   string
	Properties    map[string]string
	TTL           time.Duration
}

func validateOutMessageTTL(m OutMessage) error {
	if m.TTL < 0 {
		return fmt.Errorf("%w: message TTL cannot be negative", ErrInvalidArgument)
	}
	if m.TTL > 0 && m.TTL < time.Millisecond {
		return fmt.Errorf("%w: message TTL must be at least 1ms", ErrInvalidArgument)
	}
	return nil
}

// QueueConfig configures one queue.
type QueueConfig struct {
	LockDuration       time.Duration
	MaxDeliveryCount   int
	DefaultTTL         time.Duration
	DeadLetterOnExpire *bool
	DedupWindow        time.Duration
	Ordering           OrderingMode
	DLQMaxAge          time.Duration
	DLQMaxCount        int
	DLQMaxBytes        int64
}

// QueueInfo describes a queue or subscription backing queue.
type QueueInfo struct {
	State            string `json:"state"`
	Name             string `json:"name"`
	Kind             string `json:"kind"`
	LockDurationMs   int64  `json:"lock_duration_ms"`
	MaxDeliveryCount int    `json:"max_delivery_count"`
	DefaultTTLMs     int64  `json:"default_ttl_ms"`
	DedupWindowMs    int64  `json:"dedup_window_ms"`
}

// Metrics contains queue counters.
type Metrics struct {
	Queue              string `json:"queue"`
	Active             int64  `json:"active"`
	Locked             int64  `json:"locked"`
	Deferred           int64  `json:"deferred"`
	Scheduled          int64  `json:"scheduled"`
	DeadLettered       int64  `json:"dead_lettered"`
	Total              int64  `json:"total"`
	OldestMessageAgeMs int64  `json:"oldest_message_age_ms"`
}

// StatusInfo is a desensitized broker/backend snapshot.
type StatusInfo struct {
	Version       string `json:"version,omitempty"`
	Backend       string `json:"backend"`
	Remote        bool   `json:"remote"`
	Location      string `json:"location"`
	SchemaVersion string `json:"schema_version"`
	PingMs        int64  `json:"ping_ms"`
	DBSizeBytes   int64  `json:"db_size_bytes"`
	Queues        int    `json:"queues"`
	Subscriptions int    `json:"subscriptions"`
	ServerTimeMs  int64  `json:"server_time_ms,omitempty"`
	UptimeMs      int64  `json:"uptime_ms"`
	Auth          bool   `json:"auth"`
}

// DeleteQueueOpts configures queue deletion.
type DeleteQueueOpts struct{ Force bool }

// DeleteSubscriptionOpts configures subscription deletion.
type DeleteSubscriptionOpts struct{ Force bool }

// DeleteResult reports whether bounded physical cleanup is complete.
type DeleteResult struct {
	Deleted bool   `json:"deleted"`
	State   string `json:"state"`
}

// SubscribeOptions sets a fixed lifetime only at first creation.
type SubscribeOptions struct{ TTL time.Duration }

func (o SubscribeOptions) ttlMilliseconds() (int64, error) {
	if o.TTL < 0 {
		return 0, fmt.Errorf("%w: subscription TTL cannot be negative", ErrInvalidArgument)
	}
	if o.TTL > 0 && o.TTL < time.Millisecond {
		return 0, fmt.Errorf("%w: subscription TTL must be at least 1ms", ErrInvalidArgument)
	}
	return o.TTL.Milliseconds(), nil
}

// SubscriptionInfo is one active topic-to-subscription mapping.
type SubscriptionInfo struct {
	Topic string `json:"topic"`
	Name  string `json:"name"`
	Expr  string `json:"expr,omitempty"`
}

// SubscriptionPageItem is one lifecycle-aware inventory row.
type SubscriptionPageItem struct {
	Topic       string `json:"topic"`
	Name        string `json:"name"`
	Expr        string `json:"expr,omitempty"`
	State       string `json:"state"`
	CreatedAtMs int64  `json:"created_at_ms"`
	ExpiresAtMs int64  `json:"expires_at_ms"`
}

// ListSubscriptionsOptions selects one bounded inventory page.
type ListSubscriptionsOptions struct {
	Topic           string
	After           string
	Limit           int
	IncludeDeleting bool
}

// SubscriptionPage is one current-snapshot page.
type SubscriptionPage struct {
	Subscriptions []SubscriptionPageItem `json:"subscriptions"`
	Total         int                    `json:"total"`
	NextAfter     string                 `json:"next_after,omitempty"`
}

// FilterTestResult is the result of compiling and optionally evaluating a filter.
type FilterTestResult struct {
	Valid   bool   `json:"valid"`
	Error   string `json:"error,omitempty"`
	Ran     bool   `json:"ran"`
	Matched bool   `json:"matched"`
}

// SendOpts configures SendOne.
type SendOpts struct{ At time.Time }

// RecvOpts configures Receive.
type RecvOpts struct {
	Max        int
	Wait       time.Duration
	AtMostOnce bool
	Attempt    string
	Pick       []int64
}

func (o RecvOpts) mode() receiveMode {
	if o.AtMostOnce {
		return receiveAndDelete
	}
	return receivePeekLock
}

// PeekOpts configures Peek.
type PeekOpts struct {
	From  int64
	State State
	Max   int
}

// RedriveOpts configures moving dead letters back to active.
type RedriveOpts struct {
	To        string
	Max       int
	OlderThan time.Duration
	Rate      int
}

// PurgeOpts configures permanent dead-letter deletion.
type PurgeOpts struct {
	Max       int
	OlderThan time.Duration
}

func olderThanMilliseconds(d time.Duration) int64 {
	if d == 0 {
		return 0
	}
	if d < 0 {
		return -1
	}
	if ms := d.Milliseconds(); ms > 0 {
		return ms
	}
	return 1
}

func sendOption(opts []SendOpts) SendOpts {
	if len(opts) > 0 {
		return opts[0]
	}
	return SendOpts{}
}

func receiveOption(opts []RecvOpts) RecvOpts {
	if len(opts) > 0 {
		return opts[0]
	}
	return RecvOpts{}
}

func peekOption(opts []PeekOpts) PeekOpts {
	if len(opts) > 0 {
		return opts[0]
	}
	return PeekOpts{}
}

func redriveOption(opts []RedriveOpts) RedriveOpts {
	if len(opts) > 0 {
		return opts[0]
	}
	return RedriveOpts{}
}

func purgeOption(opts []PurgeOpts) PurgeOpts {
	if len(opts) > 0 {
		return opts[0]
	}
	return PurgeOpts{}
}

func deleteQueueOption(opts []DeleteQueueOpts) DeleteQueueOpts {
	if len(opts) > 0 {
		return opts[0]
	}
	return DeleteQueueOpts{}
}

func deleteSubscriptionOption(opts []DeleteSubscriptionOpts) DeleteSubscriptionOpts {
	if len(opts) > 0 {
		return opts[0]
	}
	return DeleteSubscriptionOpts{}
}

func epochMilliseconds(t time.Time) (int64, error) {
	if t.IsZero() {
		return 0, nil
	}
	// UnixNano is undefined outside roughly 1678..2262 and truncates negative
	// sub-milliseconds toward zero. Build the value from floor seconds plus the
	// normalized nanosecond field so the result matches time.Time.UnixMilli.
	const (
		maxInt64       = int64(^uint64(0) >> 1)
		minInt64       = -maxInt64 - 1
		maxMillisSec   = maxInt64 / 1000
		minMillisSec   = minInt64 / 1000
		maxMillisRema  = maxInt64 - maxMillisSec*1000
		minBoundaryAdd = 1000 + minInt64%1000
	)
	seconds := t.Unix()
	millis := int64(t.Nanosecond()) / int64(time.Millisecond)
	if seconds > maxMillisSec || seconds < minMillisSec-1 ||
		(seconds == maxMillisSec && millis > maxMillisRema) ||
		(seconds == minMillisSec-1 && millis < minBoundaryAdd) {
		return 0, fmt.Errorf("%w: time is outside the epoch-millisecond range", ErrInvalidArgument)
	}
	return seconds*1000 + millis, nil
}

func timeFromEpochMilliseconds(ms int64) time.Time {
	if ms == 0 {
		return time.Time{}
	}
	return time.Unix(ms/1000, (ms%1000)*int64(time.Millisecond))
}

// MaxRenewBatch is the broker's one-statement renewal cap.
const MaxRenewBatch = 512
