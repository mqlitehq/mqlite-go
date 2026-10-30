package mqlite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/mqlitehq/mqlite-go/internal/wire"
)

func outToWire(message OutMessage) wire.Message {
	return wire.Message{
		Body:          message.Body,
		MessageID:     message.MessageID,
		GroupID:       message.GroupID,
		CorrelationID: message.CorrelationID,
		ReplyTo:       message.ReplyTo,
		Subject:       message.Subject,
		ContentType:   message.ContentType,
		Properties:    message.Properties,
	}
}

func (c *Client) wireToMessage(queue string, message wire.Message, observation serverClockObservation) *Message {
	lockedUntil := timeFromEpochMilliseconds(message.LockedUntilMs)
	localLeaseDeadline, observedClock := observation.localLeaseDeadline(lockedUntil)
	return &Message{
		SequenceNumber:        message.SeqNumber,
		Body:                  message.Body,
		MessageID:             message.MessageID,
		GroupID:               message.GroupID,
		CorrelationID:         message.CorrelationID,
		ReplyTo:               message.ReplyTo,
		Subject:               message.Subject,
		ContentType:           message.ContentType,
		Properties:            message.Properties,
		DeliveryCount:         message.DeliveryCount,
		EnqueuedAt:            timeFromEpochMilliseconds(message.EnqueuedAtMs),
		VisibleAt:             timeFromEpochMilliseconds(message.VisibleAtMs),
		ExpiresAt:             timeFromEpochMilliseconds(message.ExpiresAtMs),
		LockedUntil:           lockedUntil,
		queue:                 queue,
		lockToken:             message.LockToken,
		s:                     c,
		localLeaseDeadline:    localLeaseDeadline,
		leaseResponseRTT:      observation.responseRTT,
		leaseClockResolution:  observation.resolution,
		leaseClockUnavailable: !lockedUntil.IsZero() && !observedClock,
	}
}

func filterToWire(filter *Filter) *wire.Filter {
	if filter == nil {
		return nil
	}
	return &wire.Filter{Expr: filter.Expr}
}

// SendOne sends or schedules one message and returns its sequence number. When
// queue names a topic, only active matching subscriptions retain a copy; a
// successful publish with no match returns sequence zero and stores no history.
// An invalid successful acknowledgement returns ErrOutcomeUnknown because the
// message may already have committed; reconcile before retrying.
func (c *Client) SendOne(ctx context.Context, queue string, message OutMessage, opts ...SendOpts) (int64, error) {
	if err := validateOutMessageTTL(message); err != nil {
		return 0, err
	}
	option := sendOption(opts)
	path := wire.PathSend
	request := wire.SendRequest{
		Queue:    queue,
		Messages: []wire.Message{outToWire(message)},
		TTLMs:    message.TTL.Milliseconds(),
	}
	if !option.At.IsZero() {
		path = wire.PathSchedule
		at, err := epochMilliseconds(option.At)
		if err != nil {
			return 0, err
		}
		request.ScheduledEnqueueTimeMs = at
	}
	response := wire.SendResponse{}
	if err := c.post(ctx, path, request, &response); err != nil {
		var invalid *invalidSuccessResponseError
		if errors.As(err, &invalid) {
			return 0, &ambiguousOutcomeError{operation: "Send", cause: err}
		}
		return 0, err
	}
	if len(response.SeqNumbers) != 1 {
		return 0, &ambiguousOutcomeError{
			operation: "Send",
			cause:     fmt.Errorf("received %d sequence numbers for 1 message", len(response.SeqNumbers)),
		}
	}
	return response.SeqNumbers[0], nil
}

// Send atomically enqueues one or more messages. An empty batch is a no-op. An
// invalid successful acknowledgement returns ErrOutcomeUnknown because the
// batch may already have committed; reconcile before retrying.
func (c *Client) Send(ctx context.Context, queue string, messages ...OutMessage) ([]int64, error) {
	if len(messages) == 0 {
		return nil, nil
	}
	for i, message := range messages {
		if err := validateOutMessageTTL(message); err != nil {
			return nil, fmt.Errorf("message %d: %w", i, err)
		}
	}
	ttl := messages[0].TTL.Milliseconds()
	uniformTTL := true
	for _, message := range messages[1:] {
		if message.TTL.Milliseconds() != ttl {
			uniformTTL = false
			break
		}
	}
	var request interface{}
	if uniformTTL {
		items := make([]wire.Message, len(messages))
		for i, message := range messages {
			items[i] = outToWire(message)
		}
		request = wire.SendRequest{Queue: queue, Messages: items, TTLMs: ttl}
	} else {
		items := make([]wire.SendMessage, len(messages))
		for i, message := range messages {
			items[i] = wire.SendMessage{Message: outToWire(message), TTLMs: message.TTL.Milliseconds()}
		}
		request = wire.SendWithMessageTTLsRequest{Queue: queue, Messages: items}
	}
	response := wire.SendResponse{}
	if err := c.post(ctx, wire.PathSend, request, &response); err != nil {
		var invalid *invalidSuccessResponseError
		if errors.As(err, &invalid) {
			return nil, &ambiguousOutcomeError{operation: "Send", cause: err}
		}
		return nil, err
	}
	if len(response.SeqNumbers) != len(messages) {
		return nil, &ambiguousOutcomeError{
			operation: "Send",
			cause:     fmt.Errorf("received %d sequence numbers for %d messages", len(response.SeqNumbers), len(messages)),
		}
	}
	return response.SeqNumbers, nil
}

// Cancel removes one never-delivered scheduled message. An invalid successful
// acknowledgement returns ErrOutcomeUnknown because cancellation may already
// have committed.
func (c *Client) Cancel(ctx context.Context, queue string, sequenceNumber int64) error {
	response := wire.SettleResponse{}
	if err := c.post(ctx, wire.PathCancel, wire.CancelRequest{Queue: queue, SeqNumber: sequenceNumber}, &response); err != nil {
		var invalid *invalidSuccessResponseError
		if errors.As(err, &invalid) {
			return &ambiguousOutcomeError{operation: "Cancel", cause: err}
		}
		return err
	}
	if !response.Ok {
		return &ambiguousOutcomeError{
			operation: "Cancel",
			cause:     errors.New("successful response did not confirm cancellation"),
		}
	}
	return nil
}

// SettleResult is one result from CompleteBatch or RenewBatch.
type SettleResult struct {
	SequenceNumber int64
	Ok             bool
	LockedUntil    time.Time
}

func settleResults(results []wire.SettleItemResult) []SettleResult {
	out := make([]SettleResult, len(results))
	for i, result := range results {
		out[i] = SettleResult{
			SequenceNumber: result.SeqNumber,
			Ok:             result.Ok,
			LockedUntil:    timeFromEpochMilliseconds(result.LockedUntilMs),
		}
	}
	return out
}

func settleItems(messages []*Message) ([]wire.SettleItem, error) {
	items := make([]wire.SettleItem, len(messages))
	for i, message := range messages {
		if message == nil {
			return nil, fmt.Errorf("%w: message %d is nil", ErrInvalidArgument, i)
		}
		items[i] = wire.SettleItem{SeqNumber: message.SequenceNumber, LockToken: message.lockToken}
	}
	return items, nil
}

func validateBatchResults(messages []*Message, results []wire.SettleItemResult, renew bool) error {
	if len(results) != len(messages) {
		return fmt.Errorf("mqlite: batch response has %d results for %d messages", len(results), len(messages))
	}
	for i, result := range results {
		if result.SeqNumber != messages[i].SequenceNumber || result.LockedUntilMs < 0 ||
			(!renew && result.LockedUntilMs != 0) ||
			(renew && ((result.Ok && result.LockedUntilMs == 0) || (!result.Ok && result.LockedUntilMs != 0))) {
			return fmt.Errorf("mqlite: invalid batch result at index %d", i)
		}
	}
	return nil
}

// Message creates a settleable handle for a previously received message.
func (c *Client) Message(queue string, sequenceNumber int64, lockToken string) *Message {
	return &Message{queue: queue, SequenceNumber: sequenceNumber, lockToken: lockToken, s: c}
}

// CompleteBatch completes many messages in one round trip.
func (c *Client) CompleteBatch(ctx context.Context, queue string, messages ...*Message) ([]SettleResult, error) {
	items, err := settleItems(messages)
	if err != nil {
		return nil, err
	}
	response := wire.CompleteBatchResponse{}
	if err := c.post(ctx, wire.PathCompleteBatch, wire.CompleteBatchRequest{Queue: queue, Messages: items}, &response); err != nil {
		var invalid *invalidSuccessResponseError
		if errors.As(err, &invalid) {
			return nil, &ambiguousOutcomeError{operation: "CompleteBatch", cause: err}
		}
		return nil, err
	}
	if err := validateBatchResults(messages, response.Results, false); err != nil {
		return nil, &ambiguousOutcomeError{operation: "CompleteBatch", cause: err}
	}
	return settleResults(response.Results), nil
}

// RenewBatch renews many live locks in one statement and round trip.
func (c *Client) RenewBatch(ctx context.Context, queue string, messages ...*Message) ([]SettleResult, error) {
	results, _, err := c.renewBatchObserved(ctx, queue, messages...)
	return results, err
}

func (c *Client) renewBatchObserved(ctx context.Context, queue string, messages ...*Message) ([]SettleResult, serverClockObservation, error) {
	if len(messages) > MaxRenewBatch {
		return nil, serverClockObservation{}, fmt.Errorf("%w: renew batch exceeds %d messages", ErrInvalidArgument, MaxRenewBatch)
	}
	items, err := settleItems(messages)
	if err != nil {
		return nil, serverClockObservation{}, err
	}
	response := wire.RenewBatchResponse{}
	observation, err := c.postObserved(ctx, wire.PathRenewBatch, wire.RenewBatchRequest{Queue: queue, Messages: items}, &response)
	if err != nil {
		var invalid *invalidSuccessResponseError
		if errors.As(err, &invalid) {
			return nil, serverClockObservation{}, &ambiguousOutcomeError{operation: "RenewBatch", cause: err}
		}
		return nil, serverClockObservation{}, err
	}
	if err := validateBatchResults(messages, response.Results, true); err != nil {
		return nil, serverClockObservation{}, &ambiguousOutcomeError{operation: "RenewBatch", cause: err}
	}
	return settleResults(response.Results), observation, nil
}

// renewLease is the managed receiver's deadline-bearing renewal path. The
// public single-message Renew route predates deadline responses, while
// RenewBatch reports the committed lease deadline even for one item.
func (c *Client) renewLease(ctx context.Context, message *Message) (renewalLease, error) {
	results, observation, err := c.renewBatchObserved(ctx, message.queue, message)
	if err != nil {
		return renewalLease{}, err
	}
	if len(results) != 1 || !results[0].Ok {
		return renewalLease{}, ErrLockLost
	}
	lease := renewalLease{
		responseRTT:      observation.responseRTT,
		clockResolution:  observation.resolution,
		clockUnavailable: observation.serverUpper.IsZero(),
	}
	if deadline, ok := observation.localLeaseDeadline(results[0].LockedUntil); ok {
		lease.deadline = deadline
		return lease, nil
	}
	lease.deadline = results[0].LockedUntil
	return lease, nil
}

// Receive claims messages with Peek-Lock by default. Pick retrieves deferred
// sequence numbers, and AtMostOnce uses receive-and-delete.
func (c *Client) Receive(ctx context.Context, queue string, opts ...RecvOpts) ([]*Message, error) {
	option := receiveOption(opts)
	if option.Wait < 0 {
		return nil, fmt.Errorf("%w: receive wait cannot be negative", ErrInvalidArgument)
	}
	response := wire.ReceiveResponse{}
	var observation serverClockObservation
	var err error
	if len(option.Pick) > 0 {
		observation, err = c.postObserved(ctx, wire.PathReceiveDeferred, wire.ReceiveDeferredRequest{
			Queue: queue, SeqNumbers: option.Pick,
		}, &response)
		if err != nil {
			return nil, err
		}
	} else {
		max := option.Max
		if max <= 0 {
			max = 1
		}
		observation, err = c.postObserved(ctx, wire.PathReceive, wire.ReceiveRequest{
			Queue: queue, MaxMessages: max, WaitTimeMs: option.Wait.Milliseconds(),
			ReceiveMode: int(option.mode()), AttemptID: option.Attempt,
		}, &response)
		if err != nil {
			var invalid *invalidSuccessResponseError
			if option.mode() == receiveAndDelete && errors.As(err, &invalid) {
				return nil, &ambiguousOutcomeError{operation: "ReceiveAtMostOnce", cause: err}
			}
			return nil, err
		}
	}
	return c.messagesFromWire(queue, response.Messages, observation), nil
}

func (c *Client) receiveOne(ctx context.Context, queue string, max int, waitMs int64, mode receiveMode, attemptID string) ([]*Message, error) {
	response := wire.ReceiveResponse{}
	observation, err := c.postObserved(ctx, wire.PathReceive, wire.ReceiveRequest{
		Queue: queue, MaxMessages: max, WaitTimeMs: waitMs,
		ReceiveMode: int(mode), AttemptID: attemptID,
	}, &response)
	if err != nil {
		return nil, err
	}
	return c.messagesFromWire(queue, response.Messages, observation), nil
}

func (c *Client) messagesFromWire(queue string, messages []wire.Message, observation serverClockObservation) []*Message {
	out := make([]*Message, len(messages))
	for i, message := range messages {
		out[i] = c.wireToMessage(queue, message, observation)
	}
	return out
}

// PeekedMessage is a read-only browse result.
type PeekedMessage struct {
	SequenceNumber        int64
	State                 State
	Body                  []byte
	MessageID             string
	GroupID               string
	CorrelationID         string
	ReplyTo               string
	Subject               string
	ContentType           string
	Properties            map[string]string
	DeliveryCount         int
	EnqueuedAt            time.Time
	VisibleAt             time.Time
	ExpiresAt             time.Time
	LockedUntil           time.Time
	DeadLetterReason      string
	DeadLetterDescription string
}

// Peek browses messages without claiming them.
func (c *Client) Peek(ctx context.Context, queue string, opts ...PeekOpts) ([]*PeekedMessage, error) {
	option := peekOption(opts)
	response := wire.PeekResponse{}
	if err := c.post(ctx, wire.PathPeek, wire.PeekRequest{
		Queue: queue, FromSeq: option.From, State: string(option.State), Max: option.Max,
	}, &response); err != nil {
		return nil, err
	}
	out := make([]*PeekedMessage, len(response.Messages))
	for i, message := range response.Messages {
		out[i] = &PeekedMessage{
			SequenceNumber: message.SeqNumber, State: State(message.State), Body: message.Body,
			MessageID: message.MessageID, GroupID: message.GroupID, CorrelationID: message.CorrelationID,
			ReplyTo: message.ReplyTo, Subject: message.Subject, ContentType: message.ContentType,
			Properties: message.Properties, DeliveryCount: message.DeliveryCount,
			EnqueuedAt:       timeFromEpochMilliseconds(message.EnqueuedAtMs),
			VisibleAt:        timeFromEpochMilliseconds(message.VisibleAtMs),
			ExpiresAt:        timeFromEpochMilliseconds(message.ExpiresAtMs),
			LockedUntil:      timeFromEpochMilliseconds(message.LockedUntilMs),
			DeadLetterReason: message.DeadLetterReason, DeadLetterDescription: message.DeadLetterDescription,
		}
	}
	return out, nil
}

// CreateQueue creates or updates one queue.
func (c *Client) CreateQueue(ctx context.Context, name string, config QueueConfig) error {
	if err := config.validate(); err != nil {
		return err
	}
	return c.post(ctx, wire.PathCreateQueue, wire.CreateQueueRequest{
		Name: name,
		Config: wire.QueueConfigJSON{
			LockDurationMs: config.LockDuration.Milliseconds(), MaxDeliveryCount: config.MaxDeliveryCount,
			DefaultTTLMs: config.DefaultTTL.Milliseconds(), DeadLetterOnExpire: config.DeadLetterOnExpire,
			DedupWindowMs: config.DedupWindow.Milliseconds(), OrderingMode: string(config.Ordering),
			DLQMaxAgeMs: config.DLQMaxAge.Milliseconds(), DLQMaxCount: config.DLQMaxCount,
			DLQMaxBytes: config.DLQMaxBytes,
		},
	}, &wire.Empty{})
}

// DeleteQueue deletes an empty plain queue.
func (c *Client) DeleteQueue(ctx context.Context, name string) error {
	_, err := c.DeleteQueueWithOptions(ctx, name)
	return err
}

// DeleteQueueWithOptions deletes a queue and optionally discards retained work.
func (c *Client) DeleteQueueWithOptions(ctx context.Context, name string, opts ...DeleteQueueOpts) (DeleteResult, error) {
	option := deleteQueueOption(opts)
	var raw json.RawMessage
	if err := c.post(ctx, wire.PathDeleteQueue, wire.DeleteQueueRequest{Name: name, Force: option.Force}, &raw); err != nil {
		return DeleteResult{}, err
	}
	response, err := wire.DecodeDeleteResponse(raw)
	if err != nil {
		return DeleteResult{}, fmt.Errorf("%w: invalid deletion acknowledgement; inspect ListQueues to reconcile", ErrOutcomeUnknown)
	}
	return DeleteResult{Deleted: response.Deleted, State: response.State}, nil
}

// Subscribe creates a topic subscription without a fixed TTL, or updates an
// existing subscription without changing its original lifetime. Receive from
// the subscription name, not the topic name.
func (c *Client) Subscribe(ctx context.Context, topic, name string, filter *Filter) error {
	return c.post(ctx, wire.PathSubscribe, wire.SubscribeRequest{
		Topic: topic, Name: name, Filter: filterToWire(filter),
	}, &wire.Empty{})
}

// SubscribeWithOptions creates or updates a subscription and sets a fixed TTL
// only when the subscription is first created.
func (c *Client) SubscribeWithOptions(ctx context.Context, topic, name string, filter *Filter, opts SubscribeOptions) error {
	ttl, err := opts.ttlMilliseconds()
	if err != nil {
		return err
	}
	return c.post(ctx, wire.PathSubscribe, wire.SubscribeWithOptionsRequest{
		Topic: topic, Name: name, Filter: filterToWire(filter), TTLMs: ttl,
	}, &wire.Empty{})
}

// DeleteSubscription deletes an empty subscription. It always requires manage
// permission on brokers that support subscription deletion.
func (c *Client) DeleteSubscription(ctx context.Context, topic, name string) error {
	_, err := c.DeleteSubscriptionWithOptions(ctx, topic, name)
	return err
}

// DeleteSubscriptionWithOptions optionally authorizes discarding retained work.
func (c *Client) DeleteSubscriptionWithOptions(ctx context.Context, topic, name string, opts ...DeleteSubscriptionOpts) (DeleteResult, error) {
	option := deleteSubscriptionOption(opts)
	var raw json.RawMessage
	if err := c.post(ctx, wire.PathDeleteSubscription, wire.DeleteSubscriptionRequest{
		Topic: topic, Name: name, Force: option.Force,
	}, &raw); err != nil {
		return DeleteResult{}, err
	}
	response, err := wire.DecodeDeleteResponse(raw)
	if err != nil {
		return DeleteResult{}, fmt.Errorf("%w: invalid deletion acknowledgement; inspect ListSubscriptions with IncludeDeleting to reconcile", ErrOutcomeUnknown)
	}
	return DeleteResult{Deleted: response.Deleted, State: response.State}, nil
}

// ListQueues returns broker-wide queue inventory and requires manage permission.
func (c *Client) ListQueues(ctx context.Context) ([]QueueInfo, error) {
	response := wire.ListQueuesResponse{}
	if err := c.post(ctx, wire.PathListQueues, wire.Empty{}, &response); err != nil {
		return nil, err
	}
	out := make([]QueueInfo, len(response.Queues))
	for i, queue := range response.Queues {
		state := queue.State
		if state == "" {
			state = "active"
		}
		out[i] = QueueInfo{
			State: state, Name: queue.Name, Kind: queue.Kind, LockDurationMs: queue.LockDurationMs,
			MaxDeliveryCount: queue.MaxDeliveryCount, DefaultTTLMs: queue.DefaultTTLMs,
			DedupWindowMs: queue.DedupWindowMs,
		}
	}
	return out, nil
}

// Stats returns current counters for one queue or subscription.
func (c *Client) Stats(ctx context.Context, queue string) (Metrics, error) {
	response := wire.MetricsResponse{}
	if err := c.post(ctx, wire.PathStats, wire.MetricsRequest{Queue: queue}, &response); err != nil {
		return Metrics{}, err
	}
	return Metrics{
		Queue: response.Queue, Active: response.Active, Locked: response.Locked,
		Deferred: response.Deferred, Scheduled: response.Scheduled,
		DeadLettered: response.DeadLettered, Total: response.Total,
		OldestMessageAgeMs: response.OldestMessageAgeMs,
	}, nil
}

// Redrive moves dead-lettered messages back to active.
func (c *Client) Redrive(ctx context.Context, queue string, opts ...RedriveOpts) (int, error) {
	option := redriveOption(opts)
	response := wire.RedriveResponse{}
	if err := c.post(ctx, wire.PathRedrive, wire.RedriveRequest{
		Queue: queue, Target: option.To, Max: option.Max,
		OlderThanMs: olderThanMilliseconds(option.OlderThan), RatePerSec: option.Rate,
	}, &response); err != nil {
		return 0, err
	}
	return response.Moved, nil
}

// Purge permanently removes dead letters.
func (c *Client) Purge(ctx context.Context, queue string, opts ...PurgeOpts) (int, error) {
	option := purgeOption(opts)
	response := wire.PurgeResponse{}
	if err := c.post(ctx, wire.PathPurge, wire.PurgeRequest{
		Queue: queue, Max: option.Max, OlderThanMs: olderThanMilliseconds(option.OlderThan),
	}, &response); err != nil {
		return 0, err
	}
	return response.Purged, nil
}

// Status returns a desensitized broker/backend snapshot.
func (c *Client) Status(ctx context.Context) (StatusInfo, error) {
	response := wire.StatusResponse{}
	if err := c.post(ctx, wire.PathStatus, wire.Empty{}, &response); err != nil {
		return StatusInfo{}, err
	}
	return StatusInfo{
		Version: response.Version, Backend: response.Backend, Remote: response.Remote,
		Location: response.Location, SchemaVersion: response.SchemaVersion,
		PingMs: response.PingMs, DBSizeBytes: response.DBSizeBytes,
		Queues: response.Queues, Subscriptions: response.Subscriptions,
		ServerTimeMs: response.ServerTimeMs, UptimeMs: response.UptimeMs, Auth: response.Auth,
	}, nil
}

// ListSubscriptions returns active subscriptions using the legacy complete route.
func (c *Client) ListSubscriptions(ctx context.Context) ([]SubscriptionInfo, error) {
	response := wire.ListSubscriptionsResponse{}
	if err := c.post(ctx, wire.PathListSubscriptions, wire.Empty{}, &response); err != nil {
		return nil, err
	}
	out := make([]SubscriptionInfo, len(response.Subscriptions))
	for i, subscription := range response.Subscriptions {
		out[i] = SubscriptionInfo{Topic: subscription.Topic, Name: subscription.Name, Expr: subscription.Expr}
	}
	return out, nil
}

// ListSubscriptionsWithOptions returns one validated lifecycle-aware page.
func (c *Client) ListSubscriptionsWithOptions(ctx context.Context, opts ListSubscriptionsOptions) (SubscriptionPage, error) {
	request := wire.ListSubscriptionsPageRequest{
		Topic: opts.Topic, After: opts.After, Limit: opts.Limit, IncludeDeleting: opts.IncludeDeleting,
	}
	var raw json.RawMessage
	if err := c.post(ctx, wire.PathListSubscriptionsPage, request, &raw); err != nil {
		return SubscriptionPage{}, err
	}
	response, err := wire.DecodeListSubscriptionsPageResponse(raw, request)
	if err != nil {
		return SubscriptionPage{}, err
	}
	out := SubscriptionPage{
		Subscriptions: make([]SubscriptionPageItem, len(response.Subscriptions)),
		Total:         response.Total, NextAfter: response.NextAfter,
	}
	for i, subscription := range response.Subscriptions {
		out.Subscriptions[i] = SubscriptionPageItem{
			Topic: subscription.Topic, Name: subscription.Name, Expr: subscription.Expr,
			State: subscription.State, CreatedAtMs: subscription.CreatedAtMs,
			ExpiresAtMs: subscription.ExpiresAtMs,
		}
	}
	return out, nil
}

// TestFilter compiles and optionally evaluates a subscription expression.
func (c *Client) TestFilter(ctx context.Context, expression string, sample *OutMessage, enqueuedAtMs, visibleAtMs int64) (FilterTestResult, error) {
	request := wire.TestFilterRequest{Expr: expression}
	if sample != nil {
		message := outToWire(*sample)
		message.EnqueuedAtMs = enqueuedAtMs
		message.VisibleAtMs = visibleAtMs
		request.Message = &message
	}
	response := wire.TestFilterResponse{}
	if err := c.post(ctx, wire.PathTestFilter, request, &response); err != nil {
		return FilterTestResult{}, err
	}
	return FilterTestResult{
		Valid: response.Valid, Error: response.Error, Ran: response.Ran, Matched: response.Matched,
	}, nil
}

// Receiver returns a managed receive loop bound to queue. To consume a topic,
// bind it to the subscription name rather than the topic name.
func (c *Client) Receiver(queue string, opts ...ReceiverOption) *Receiver {
	return newReceiver(c, queue, opts)
}

func (c *Client) settle(ctx context.Context, path string, request wire.SettleRequest) error {
	response := wire.SettleResponse{}
	if err := c.post(ctx, path, request, &response); err != nil {
		var invalid *invalidSuccessResponseError
		if operation := ambiguousSettlementOperation(path); operation != "" && errors.As(err, &invalid) {
			return &ambiguousOutcomeError{operation: operation, cause: err}
		}
		return err
	}
	if !response.Ok {
		return ErrLockLost
	}
	return nil
}

func ambiguousSettlementOperation(path string) string {
	switch path {
	case wire.PathComplete:
		return "Complete"
	case wire.PathAbandon:
		return "Abandon"
	case wire.PathReject:
		return "Reject"
	case wire.PathDefer:
		return "Defer"
	case wire.PathRenew:
		return "Renew"
	default:
		return ""
	}
}

func (c *Client) complete(ctx context.Context, queue string, sequenceNumber int64, token string) error {
	return c.settle(ctx, wire.PathComplete, wire.SettleRequest{
		Queue: queue, SeqNumber: sequenceNumber, LockToken: token,
	})
}

func (c *Client) abandon(ctx context.Context, queue string, sequenceNumber int64, token string, delayMs int64) error {
	return c.settle(ctx, wire.PathAbandon, wire.SettleRequest{
		Queue: queue, SeqNumber: sequenceNumber, LockToken: token, DelayMs: delayMs,
	})
}

func (c *Client) reject(ctx context.Context, queue string, sequenceNumber int64, token, reason, detail string) error {
	return c.settle(ctx, wire.PathReject, wire.SettleRequest{
		Queue: queue, SeqNumber: sequenceNumber, LockToken: token,
		DeadLetterReason: reason, DeadLetterDescription: detail,
	})
}

func (c *Client) deferMsg(ctx context.Context, queue string, sequenceNumber int64, token string) error {
	return c.settle(ctx, wire.PathDefer, wire.SettleRequest{
		Queue: queue, SeqNumber: sequenceNumber, LockToken: token,
	})
}

func (c *Client) renew(ctx context.Context, queue string, sequenceNumber int64, token string) error {
	return c.settle(ctx, wire.PathRenew, wire.SettleRequest{
		Queue: queue, SeqNumber: sequenceNumber, LockToken: token,
	})
}
