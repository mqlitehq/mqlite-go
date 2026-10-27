// Package wire defines the private JSON-over-HTTP contract used by the remote
// SDK. It intentionally contains no broker engine or storage dependencies.
package wire

// Unary RPC paths. Every operation is one JSON HTTP POST.
const (
	PathSend            = "/mqlite.v1.QueueService/Send"
	PathReceive         = "/mqlite.v1.QueueService/Receive"
	PathComplete        = "/mqlite.v1.QueueService/Complete"
	PathCompleteBatch   = "/mqlite.v1.QueueService/CompleteBatch"
	PathAbandon         = "/mqlite.v1.QueueService/Abandon"
	PathReject          = "/mqlite.v1.QueueService/Reject"
	PathDefer           = "/mqlite.v1.QueueService/Defer"
	PathReceiveDeferred = "/mqlite.v1.QueueService/ReceiveDeferred"
	PathRenew           = "/mqlite.v1.QueueService/Renew"
	PathRenewBatch      = "/mqlite.v1.QueueService/RenewBatch"
	PathSchedule        = "/mqlite.v1.QueueService/Schedule"
	PathCancel          = "/mqlite.v1.QueueService/Cancel"
	PathPeek            = "/mqlite.v1.QueueService/Peek"
	PathStats           = "/mqlite.v1.QueueService/Stats"

	PathDeleteQueue           = "/mqlite.v1.AdminService/DeleteQueue"
	PathCreateQueue           = "/mqlite.v1.AdminService/CreateQueue"
	PathSubscribe             = "/mqlite.v1.AdminService/Subscribe"
	PathListQueues            = "/mqlite.v1.AdminService/ListQueues"
	PathListSubscriptions     = "/mqlite.v1.AdminService/ListSubscriptions"
	PathListSubscriptionsPage = "/mqlite.v1.AdminService/ListSubscriptionsPage"
	PathDeleteSubscription    = "/mqlite.v1.AdminService/DeleteSubscription"
	PathTestFilter            = "/mqlite.v1.AdminService/TestFilter"
	PathRedrive               = "/mqlite.v1.AdminService/Redrive"
	PathPurge                 = "/mqlite.v1.AdminService/Purge"
	PathStatus                = "/mqlite.v1.AdminService/Status"
	PathObserve               = "/mqlite.v1.AdminService/Observe"

	PathCreateKey = "/mqlite.v1.AuthService/CreateKey"
	PathListKeys  = "/mqlite.v1.AuthService/ListKeys"
	PathRevokeKey = "/mqlite.v1.AuthService/RevokeKey"
)

// MaxResponseBytes bounds ordinary successful JSON responses. The broker's
// 32 MiB response budget counts decoded body bytes; JSON base64 encoding expands
// those bytes by roughly one third. A 64 MiB client bound therefore accepts the
// largest normal Receive/Peek response while still bounding an untrusted peer.
const MaxResponseBytes = 64 << 20

// MaxErrorResponseBytes bounds the error envelope retained from an untrusted
// broker or intermediary.
const MaxErrorResponseBytes = 64 << 10

// Message is the transport form used for send, receive, and peek. []byte uses
// encoding/json's standard base64 representation.
type Message struct {
	SeqNumber             int64             `json:"seq_number,omitempty"`
	EnqueuedAtMs          int64             `json:"enqueued_at_ms,omitempty"`
	ExpiresAtMs           int64             `json:"expires_at_ms,omitempty"`
	VisibleAtMs           int64             `json:"visible_at_ms,omitempty"`
	LockedUntilMs         int64             `json:"locked_until_ms,omitempty"`
	DeliveryCount         int               `json:"delivery_count,omitempty"`
	LockToken             string            `json:"lock_token,omitempty"`
	State                 string            `json:"state,omitempty"`
	DeadLetterReason      string            `json:"dead_letter_reason,omitempty"`
	DeadLetterDescription string            `json:"dead_letter_description,omitempty"`
	MessageID             string            `json:"message_id,omitempty"`
	CorrelationID         string            `json:"correlation_id,omitempty"`
	ReplyTo               string            `json:"reply_to,omitempty"`
	GroupID               string            `json:"group_id,omitempty"`
	ContentType           string            `json:"content_type,omitempty"`
	Subject               string            `json:"subject,omitempty"`
	Properties            map[string]string `json:"properties,omitempty"`
	Body                  []byte            `json:"body,omitempty"`
}

type SendRequest struct {
	Queue                  string    `json:"queue"`
	Messages               []Message `json:"messages"`
	ScheduledEnqueueTimeMs int64     `json:"scheduled_enqueue_time_ms,omitempty"`
	TTLMs                  int64     `json:"ttl_ms,omitempty"`
}

type SendMessage struct {
	Message
	TTLMs int64 `json:"ttl_ms,omitempty"`
}

type SendWithMessageTTLsRequest struct {
	Queue                  string        `json:"queue"`
	Messages               []SendMessage `json:"messages"`
	ScheduledEnqueueTimeMs int64         `json:"scheduled_enqueue_time_ms,omitempty"`
	TTLMs                  int64         `json:"ttl_ms,omitempty"`
}

type SendResponse struct {
	SeqNumbers []int64 `json:"seq_numbers"`
}

type ReceiveRequest struct {
	Queue       string `json:"queue"`
	MaxMessages int    `json:"max_messages,omitempty"`
	WaitTimeMs  int64  `json:"wait_time_ms,omitempty"`
	ReceiveMode int    `json:"receive_mode,omitempty"`
	AttemptID   string `json:"receive_attempt_id,omitempty"`
}

type ReceiveResponse struct {
	Messages []Message `json:"messages"`
}

type ReceiveDeferredRequest struct {
	Queue      string  `json:"queue"`
	SeqNumbers []int64 `json:"seq_numbers"`
}

type SettleRequest struct {
	Queue                 string `json:"queue"`
	SeqNumber             int64  `json:"seq_number"`
	LockToken             string `json:"lock_token"`
	DeadLetterReason      string `json:"dead_letter_reason,omitempty"`
	DeadLetterDescription string `json:"dead_letter_description,omitempty"`
	DelayMs               int64  `json:"delay_ms,omitempty"`
}

type SettleResponse struct {
	Ok bool `json:"ok"`
}

type SettleItem struct {
	SeqNumber int64  `json:"seq_number"`
	LockToken string `json:"lock_token"`
}

type CompleteBatchRequest struct {
	Queue    string       `json:"queue"`
	Messages []SettleItem `json:"messages"`
}

type SettleItemResult struct {
	SeqNumber     int64 `json:"seq_number"`
	Ok            bool  `json:"ok"`
	LockedUntilMs int64 `json:"locked_until_ms,omitempty"`
}

type CompleteBatchResponse struct {
	Results []SettleItemResult `json:"results"`
}

type RenewBatchRequest struct {
	Queue    string       `json:"queue"`
	Messages []SettleItem `json:"messages"`
}

type RenewBatchResponse struct {
	Results []SettleItemResult `json:"results"`
}

type CancelRequest struct {
	Queue     string `json:"queue"`
	SeqNumber int64  `json:"seq_number"`
}

type PeekRequest struct {
	Queue   string `json:"queue"`
	FromSeq int64  `json:"from_seq,omitempty"`
	State   string `json:"state,omitempty"`
	Max     int    `json:"max,omitempty"`
}

type PeekResponse struct {
	Messages []Message `json:"messages"`
}

type MetricsRequest struct {
	Queue string `json:"queue"`
}

type MetricsResponse struct {
	Queue              string `json:"queue"`
	Active             int64  `json:"active"`
	Locked             int64  `json:"locked"`
	Deferred           int64  `json:"deferred"`
	Scheduled          int64  `json:"scheduled"`
	DeadLettered       int64  `json:"dead_lettered"`
	Total              int64  `json:"total"`
	OldestMessageAgeMs int64  `json:"oldest_message_age_ms"`
}

type QueueConfigJSON struct {
	Kind               string `json:"kind,omitempty"`
	LockDurationMs     int64  `json:"lock_duration_ms,omitempty"`
	MaxDeliveryCount   int    `json:"max_delivery_count,omitempty"`
	DefaultTTLMs       int64  `json:"default_ttl_ms,omitempty"`
	DeadLetterOnExpire *bool  `json:"dead_letter_on_expire,omitempty"`
	DedupWindowMs      int64  `json:"dedup_window_ms,omitempty"`
	OrderingMode       string `json:"ordering_mode,omitempty"`
	DLQMaxAgeMs        int64  `json:"dlq_max_age_ms,omitempty"`
	DLQMaxCount        int    `json:"dlq_max_count,omitempty"`
	DLQMaxBytes        int64  `json:"dlq_max_bytes,omitempty"`
}

type CreateQueueRequest struct {
	Name   string          `json:"name"`
	Config QueueConfigJSON `json:"config"`
}

type DeleteQueueRequest struct {
	Name  string `json:"name"`
	Force bool   `json:"force,omitempty"`
}

type DeleteResponse struct {
	Deleted bool   `json:"deleted"`
	State   string `json:"state"`
}

type DeleteQueueResponse = DeleteResponse
type DeleteSubscriptionResponse = DeleteResponse

// Filter is one expr-lang subscription predicate. An empty expression matches
// every message.
type Filter struct {
	Expr string `json:"expr,omitempty"`
}

type SubscribeRequest struct {
	Topic  string  `json:"topic"`
	Name   string  `json:"name"`
	Filter *Filter `json:"filter,omitempty"`
}

type SubscribeWithOptionsRequest struct {
	Topic  string  `json:"topic"`
	Name   string  `json:"name"`
	Filter *Filter `json:"filter,omitempty"`
	TTLMs  int64   `json:"ttl_ms,omitempty"`
}

type DeleteSubscriptionRequest struct {
	Topic string `json:"topic"`
	Name  string `json:"name"`
	Force bool   `json:"force,omitempty"`
}

type ListQueuesResponse struct {
	Queues []QueueInfoJSON `json:"queues"`
}

type QueueInfoJSON struct {
	State            string `json:"state"`
	Name             string `json:"name"`
	Kind             string `json:"kind"`
	LockDurationMs   int64  `json:"lock_duration_ms"`
	MaxDeliveryCount int    `json:"max_delivery_count"`
	DefaultTTLMs     int64  `json:"default_ttl_ms"`
	DedupWindowMs    int64  `json:"dedup_window_ms"`
}

type RedriveRequest struct {
	Queue       string `json:"queue"`
	Target      string `json:"target,omitempty"`
	Max         int    `json:"max,omitempty"`
	OlderThanMs int64  `json:"older_than_ms,omitempty"`
	RatePerSec  int    `json:"rate_per_sec,omitempty"`
}

type RedriveResponse struct {
	Moved int `json:"moved"`
}

type PurgeRequest struct {
	Queue       string `json:"queue"`
	Max         int    `json:"max,omitempty"`
	OlderThanMs int64  `json:"older_than_ms,omitempty"`
}

type PurgeResponse struct {
	Purged int `json:"purged"`
}

type SubscriptionJSON struct {
	Topic string `json:"topic"`
	Name  string `json:"name"`
	Expr  string `json:"expr,omitempty"`
}

type ListSubscriptionsResponse struct {
	Subscriptions []SubscriptionJSON `json:"subscriptions"`
}

type SubscriptionPageJSON struct {
	Topic       string `json:"topic"`
	Name        string `json:"name"`
	Expr        string `json:"expr,omitempty"`
	State       string `json:"state"`
	CreatedAtMs int64  `json:"created_at_ms"`
	ExpiresAtMs int64  `json:"expires_at_ms"`
}

type ListSubscriptionsPageRequest struct {
	Topic           string `json:"topic,omitempty"`
	After           string `json:"after,omitempty"`
	Limit           int    `json:"limit,omitempty"`
	IncludeDeleting bool   `json:"include_deleting,omitempty"`
}

type ListSubscriptionsPageResponse struct {
	Subscriptions []SubscriptionPageJSON `json:"subscriptions"`
	Total         int                    `json:"total"`
	NextAfter     string                 `json:"next_after,omitempty"`
}

type TestFilterRequest struct {
	Expr    string   `json:"expr"`
	Message *Message `json:"message,omitempty"`
}

type TestFilterResponse struct {
	Valid   bool   `json:"valid"`
	Error   string `json:"error,omitempty"`
	Ran     bool   `json:"ran"`
	Matched bool   `json:"matched"`
}

type StatusResponse struct {
	Version       string `json:"version"`
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

type Empty struct{}

type ErrorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}
