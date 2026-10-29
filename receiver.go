package mqlite

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"
)

type receiveMode int

const (
	receivePeekLock receiveMode = iota
	receiveAndDelete
)

const (
	leaseDeadlineSafety           = 10 * time.Millisecond
	minimumAutoRenewInterval      = 50 * time.Millisecond
	renewRTTMargin                = 10 * time.Millisecond
	uncertainLeaseRenewPeriod     = 250 * time.Millisecond
	defaultRenewAttemptTimeout    = time.Second
	maxInitialRenewAttemptTimeout = 5 * time.Second
)

type receiveSource interface {
	settler
	receiveOne(context.Context, string, int, int64, receiveMode, string) ([]*Message, error)
}

type renewalLease struct {
	deadline          time.Time
	responseRTT       time.Duration
	clockResolution   time.Duration
	clockUnavailable  bool
	deadlineUntrusted bool
}

type leaseRenewSource interface {
	renewLease(context.Context, *Message) (renewalLease, error)
}

type receiverConfig struct {
	autoRenew   bool
	concurrency int
	prefetch    int
	onError     func(error)
}

// ReceiverOption configures a managed Receiver.
type ReceiverOption func(*receiverConfig)

// WithAutoRenew keeps live locks renewed while handlers are running.
func WithAutoRenew() ReceiverOption {
	return func(c *receiverConfig) { c.autoRenew = true }
}

// WithConcurrency sets the maximum number of concurrent handlers. Settlement
// may remain in flight after a handler returns, within the Receiver's separate
// bounded total-in-flight capacity.
func WithConcurrency(n int) ReceiverOption {
	return func(c *receiverConfig) { c.concurrency = n }
}

// WithPrefetch caps each claim. It is always clamped to the number of idle
// workers, so a message never waits behind another handler while already locked.
func WithPrefetch(n int) ReceiverOption {
	return func(c *receiverConfig) { c.prefetch = n }
}

// WithErrorHandler receives transient receive failures, one degraded clock-mode
// diagnostic when broker time headers are unavailable, plus terminal renewal
// and settlement failures. Recovered renewal and settlement retries are not
// reported. Calls are serialized and the callback must return promptly.
func WithErrorHandler(fn func(error)) ReceiverOption {
	return func(c *receiverConfig) { c.onError = fn }
}

// Receiver repeatedly claims messages and dispatches them to a handler. A nil
// handler result completes the message; a non-nil result abandons it.
type Receiver struct {
	src   receiveSource
	queue string
	cfg   receiverConfig

	errMu                 sync.Mutex
	transientBackoff      time.Duration
	maxReceiveBackoff     time.Duration
	renewRetryBackoff     time.Duration
	maxRenewBackoff       time.Duration
	settleBackoff         time.Duration
	maxSettleBackoff      time.Duration
	settleTimeout         time.Duration
	renewAttemptTimeout   time.Duration
	jitter                func(time.Duration) time.Duration
	renewCapabilityMu     sync.Mutex
	renewBatchUnsupported bool
	clockWarningOnce      sync.Once
}

func newReceiver(src receiveSource, queue string, opts []ReceiverOption) *Receiver {
	cfg := receiverConfig{concurrency: 1}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	if cfg.concurrency < 1 {
		cfg.concurrency = 1
	}
	return &Receiver{
		src:                 src,
		queue:               queue,
		cfg:                 cfg,
		transientBackoff:    500 * time.Millisecond,
		maxReceiveBackoff:   10 * time.Second,
		renewRetryBackoff:   100 * time.Millisecond,
		maxRenewBackoff:     time.Second,
		settleBackoff:       100 * time.Millisecond,
		maxSettleBackoff:    time.Second,
		settleTimeout:       5 * time.Second,
		renewAttemptTimeout: defaultRenewAttemptTimeout,
		jitter:              jitterBackoff,
	}
}

func newAttemptID() string {
	var data [16]byte
	_, _ = rand.Read(data[:])
	return hex.EncodeToString(data[:])
}

func (r *Receiver) notify(err error) {
	if err == nil || r.cfg.onError == nil {
		return
	}
	r.errMu.Lock()
	defer r.errMu.Unlock()
	r.cfg.onError(err)
}

func (r *Receiver) notifyDegradedClock(unavailable bool) {
	if !unavailable {
		return
	}
	r.clockWarningOnce.Do(func() {
		r.notify(errors.New("mqlite: broker time headers unavailable; lease scheduling is using degraded clock mode"))
	})
}

func permanentReceiverError(err error) bool {
	return errors.Is(err, ErrUnauthenticated) ||
		errors.Is(err, ErrPermissionDenied) ||
		errors.Is(err, ErrNotFound) ||
		errors.Is(err, ErrQueueNotFound) ||
		errors.Is(err, ErrUnsupported)
}

func exponentialBackoff(initial, maximum time.Duration, failures int) time.Duration {
	if initial <= 0 {
		return 0
	}
	delay := initial
	for i := 0; i < failures && delay < maximum; i++ {
		if delay > maximum/2 {
			delay = maximum
			break
		}
		delay *= 2
	}
	if maximum > 0 && delay > maximum {
		return maximum
	}
	return delay
}

// jitterBackoff returns a delay in [delay/2, delay]. crypto/rand avoids a
// process-wide deterministic retry rhythm without adding a dependency or shared
// PRNG state. A failed entropy read merely falls back to the upper bound.
func jitterBackoff(delay time.Duration) time.Duration {
	if delay <= time.Nanosecond {
		return delay
	}
	var value [1]byte
	if _, err := rand.Read(value[:]); err != nil {
		return delay
	}
	lower := delay / 2
	span := delay - lower
	return lower + time.Duration(int64(span)*int64(value[0])/255)
}

func waitContext(ctx context.Context, delay time.Duration) bool {
	if delay <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func retryableOperationError(err error) bool {
	if err == nil {
		return false
	}
	var responseErr *HTTPError
	if errors.As(err, &responseErr) {
		// HTTP 499 is a response from the broker or intermediary, not proof that
		// the operation's controlling context was canceled. Exact managed
		// operations may retry it while their own context remains live. The
		// surrounding loops always check that context before replaying.
		return responseErr.StatusCode == 408 || responseErr.StatusCode == 429 || responseErr.StatusCode == 499 || responseErr.StatusCode/100 == 5
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, ErrLockLost) || errors.Is(err, ErrInvalidArgument) || permanentReceiverError(err) {
		return false
	}
	// No HTTP response means a transport failure or an invalid successful
	// acknowledgement. Exact renewal and settlement requests are safe to replay.
	// This includes DeadlineExceeded from a custom http.Client.Timeout while the
	// operation's controlling context is still live; each loop checks ctx.Err().
	return true
}

func reserveWorkerPairs(ctx context.Context, handlerSlots, totalSlots chan struct{}, max int) int {
	select {
	case handlerSlots <- struct{}{}:
	case <-ctx.Done():
		return 0
	}
	select {
	case totalSlots <- struct{}{}:
	case <-ctx.Done():
		<-handlerSlots
		return 0
	}

	reserved := 1
	for reserved < max {
		select {
		case handlerSlots <- struct{}{}:
			select {
			case totalSlots <- struct{}{}:
				reserved++
			case <-ctx.Done():
				<-handlerSlots
				return reserved
			default:
				<-handlerSlots
				return reserved
			}
		case <-ctx.Done():
			return reserved
		default:
			return reserved
		}
	}
	return reserved
}

func releaseWorkerPairs(handlerSlots, totalSlots chan struct{}, count int) {
	for i := 0; i < count; i++ {
		<-handlerSlots
		<-totalSlots
	}
}

// Run processes messages until ctx is canceled or a permanent broker error is
// observed. It returns ctx.Err() for normal cancellation. Run waits for active
// handlers, lock renewals, and terminal settlements to exit, so handlers must
// honor ctx and return.
func (r *Receiver) Run(ctx context.Context, handler func(context.Context, *Message) error) error {
	batch := r.cfg.prefetch
	if batch <= 0 || batch > r.cfg.concurrency {
		batch = r.cfg.concurrency
	}

	handlerSlots := make(chan struct{}, r.cfg.concurrency)
	totalSlots := make(chan struct{}, r.cfg.concurrency+batch)
	fatal := make(chan error, 1)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var workers sync.WaitGroup

	fail := func(err error) {
		select {
		case fatal <- err:
		default:
		}
		cancel()
	}
	finish := func() error {
		workers.Wait()
		select {
		case err := <-fatal:
			return err
		default:
			return ctx.Err()
		}
	}

	for {
		if runCtx.Err() != nil {
			return finish()
		}
		reserved := reserveWorkerPairs(runCtx, handlerSlots, totalSlots, batch)
		if reserved == 0 {
			return finish()
		}
		if runCtx.Err() != nil {
			releaseWorkerPairs(handlerSlots, totalSlots, reserved)
			return finish()
		}

		attemptID := newAttemptID()
		var messages []*Message
		var err error
		hadTransientFailure := false
		failures := 0
		for {
			messages, err = r.src.receiveOne(runCtx, r.queue, reserved, 20000, receivePeekLock, attemptID)
			if err == nil {
				break
			}
			if !retryableOperationError(err) {
				break
			}
			if runCtx.Err() != nil {
				break
			}
			hadTransientFailure = true
			failures++
			if failures == 1 {
				// A single lost response is commonly recovered by an immediate replay
				// of the same attempt. Preserve the existing fast path and keep it
				// silent when that replay succeeds.
				continue
			}
			r.notify(err)
			delay := r.jitter(exponentialBackoff(r.transientBackoff, r.maxReceiveBackoff, failures-2))
			if !waitContext(runCtx, delay) {
				break
			}
			// Keep the exact request, including receive_attempt_id, across the
			// entire retry window. The broker may already have claimed a batch.
		}
		if err != nil {
			releaseWorkerPairs(handlerSlots, totalSlots, reserved)
			if !retryableOperationError(err) {
				if runCtx.Err() != nil && errors.Is(err, context.Canceled) {
					return finish()
				}
				r.notify(err)
				fail(err)
				return finish()
			}
			if runCtx.Err() != nil {
				return finish()
			}
			// Transient failures were already observed before each backoff. If
			// cancellation ended the retry window there is nothing more to report.
			return finish()
		}
		if runCtx.Err() != nil {
			releaseWorkerPairs(handlerSlots, totalSlots, reserved)
			return finish()
		}
		if len(messages) > reserved {
			releaseWorkerPairs(handlerSlots, totalSlots, reserved)
			err = fmt.Errorf("mqlite: broker returned %d messages for a receive capped at %d", len(messages), reserved)
			r.notify(err)
			fail(err)
			return finish()
		}

		valid := messages[:0]
		unusable := 0
		for _, message := range messages {
			if message == nil || message.LockedUntil.IsZero() {
				unusable++
				continue
			}
			valid = append(valid, message)
		}
		messages = valid
		if unusable > 0 {
			r.notify(fmt.Errorf("%w: broker returned %d unusable message leases", ErrLockLost, unusable))
		}
		if runCtx.Err() != nil {
			releaseWorkerPairs(handlerSlots, totalSlots, reserved)
			return finish()
		}

		releaseWorkerPairs(handlerSlots, totalSlots, reserved-len(messages))
		forceInitialRenewal := hadTransientFailure
		for _, message := range messages {
			workers.Add(1)
			go func(m *Message, forceInitialRenewal bool) {
				defer workers.Done()
				defer func() { <-totalSlots }()
				r.processWithHandlerRelease(runCtx, m, handler, func() { <-handlerSlots }, fail, forceInitialRenewal)
			}(message, forceInitialRenewal)
		}
	}
}

func (r *Receiver) process(ctx context.Context, message *Message, handler func(context.Context, *Message) error, fail func(error)) {
	r.processWithHandlerRelease(ctx, message, handler, func() {}, fail, false)
}

func (r *Receiver) processWithHandlerRelease(ctx context.Context, message *Message, handler func(context.Context, *Message) error, releaseHandler func(), fail func(error), forceInitialRenewal bool) {
	handlerCtx, cancelHandler := context.WithCancel(ctx)
	defer cancelHandler()
	handlerReleased := false
	defer func() {
		if !handlerReleased {
			releaseHandler()
		}
	}()

	var cancelRenew context.CancelFunc
	var renewDone chan error
	var renewErr error
	deadline, leaseLength := messageLeaseDeadline(message)
	lease := renewalLease{
		deadline:         deadline,
		responseRTT:      message.leaseResponseRTT,
		clockResolution:  message.leaseClockResolution,
		clockUnavailable: message.leaseClockUnavailable,
	}
	r.notifyDegradedClock(lease.clockUnavailable)
	if ctx.Err() != nil {
		return
	}
	initialAttemptDeadline := time.Time{}
	initialRemaining := time.Until(message.localLeaseDeadline)
	if !message.localLeaseDeadline.IsZero() && initialRemaining > leaseDeadlineSafety &&
		!leaseNeedsUntrustedSchedule(lease, initialRemaining) {
		// A still-usable mapped horizon is a hard ownership boundary even when
		// measured RTT makes us renew immediately. A horizon already inside the
		// safety window may only be a conservative long-poll lower bound: probe it
		// under the bounded untrusted watchdog before rejecting a freshly claimed
		// token.
		initialAttemptDeadline = message.localLeaseDeadline
	}
	if forceInitialRenewal {
		// A response replay still needs a broker fence even when the response
		// carries a usable server-time-derived local deadline. Keep that deadline
		// only as the watchdog for this first RPC: a valid lease may allow a remote
		// backend more than the generic 1s fallback to answer.
		// A replayed attempt may be stale even when client clock skew makes its
		// wire deadline look far in the future. Do not let that untrusted value
		// become the lease estimate for a legacy Renew fallback. A successful
		// deadline-bearing renewal below can establish a new trusted horizon.
		deadline = time.Time{}
		leaseLength = 0
	}
	if leaseLength <= 0 {
		leaseLength = 0
	}
	if forceInitialRenewal || (r.cfg.autoRenew && needsInitialLeaseConfirmation(lease, leaseLength)) {
		if ctx.Err() != nil {
			return
		}
		// A replayed or locally unsafe lease cannot authorize user work. Confirm
		// it synchronously before dispatch. Without an exact mapped horizon, the
		// request watchdog is independent of the untrusted renewal cadence so
		// ordinary cross-region RTT does not look like immediate lock loss.
		attemptCtx, cancelAttempt := r.initialRenewalAttemptContext(ctx, initialAttemptDeadline)
		renewed, err := r.renewMessage(attemptCtx, message, leaseLength, lease)
		attemptErr := attemptCtx.Err()
		cancelAttempt()
		if ctx.Err() != nil {
			return
		}
		if attemptErr != nil {
			r.loseLease(cancelHandler, attemptErr)
			return
		}
		if err != nil {
			if !retryableRenewError(err) {
				if permanentReceiverError(err) {
					cancelHandler()
					r.settleError(err, fail)
					return
				}
				r.loseLease(cancelHandler, err)
				return
			}
			// There is no trustworthy lease window in which to back off. Match
			// renewLoop's fail-closed classification rather than dispatching work
			// under a token the broker may already have reassigned.
			r.loseLease(cancelHandler, ErrLockLost)
			return
		}
		lease = renewed
		r.notifyDegradedClock(lease.clockUnavailable)
		if ctx.Err() != nil {
			return
		}
		confirmedDeadline, remaining := anchorLeaseDeadline(lease.deadline)
		if pathologicalExactLease(lease, remaining) {
			r.loseLease(cancelHandler, fmt.Errorf("%w: renewed lease is too short for the minimum renewal interval", ErrLockLost))
			return
		}
		if remaining > leaseDeadlineSafety {
			deadline = confirmedDeadline
			leaseLength = remaining
			if leaseNeedsUntrustedSchedule(lease, remaining) {
				deadline = time.Time{}
				leaseLength = 0
			}
		} else {
			// A successful renewal is authoritative proof that the broker still
			// owns this token even when clock skew makes its absolute deadline
			// unusable locally, or a legacy Renew reports no deadline. Keep that
			// proof separate from the local schedule.
			deadline = time.Time{}
			leaseLength = 0
		}
	}
	if r.cfg.autoRenew {
		if ctx.Err() != nil {
			return
		}
		renewCtx, cancel := context.WithCancel(ctx)
		cancelRenew = cancel
		renewDone = make(chan error, 1)
		go func() {
			renewDone <- r.renewLoop(renewCtx, message, cancelHandler, fail, deadline, leaseLength, lease)
		}()
	}
	var stopRenewOnce sync.Once
	stopRenew := func() {
		stopRenewOnce.Do(func() {
			if cancelRenew != nil {
				cancelRenew()
				renewErr = <-renewDone
			}
		})
	}
	// Goexit and panics still run defers. Do not let either path release the
	// total-inflight slot while a renewal remains alive outside the bound.
	defer stopRenew()

	handlerErr := handler(handlerCtx, message)
	releaseHandler()
	handlerReleased = true
	// A lock token must never be renewed concurrently with its terminal
	// settlement. Stop and join the renew loop before Complete or Abandon.
	stopRenew()
	if renewErr != nil {
		// The renew loop already canceled the message-scoped handler and reported
		// the terminal renewal failure. Settling after a known renewal failure can
		// only race a lost token or repeat the same permanent authorization error.
		return
	}
	settleCtx, cancelSettle := context.WithTimeout(context.Background(), r.settleTimeout)
	defer cancelSettle()
	if handlerErr != nil {
		r.settleWithRetry(settleCtx, func(ctx context.Context) error {
			return message.Abandon(ctx)
		}, fail)
		return
	}
	r.settleWithRetry(settleCtx, func(ctx context.Context) error {
		return message.Complete(ctx)
	}, fail)
}

func (r *Receiver) settleWithRetry(ctx context.Context, settle func(context.Context) error, fail func(error)) {
	failures := 0
	var lastErr error
	for {
		err := settle(ctx)
		if err == nil {
			return
		}
		lastErr = err
		if !retryableOperationError(err) || ctx.Err() != nil {
			r.settleError(err, fail)
			return
		}
		delay := r.jitter(exponentialBackoff(r.settleBackoff, r.maxSettleBackoff, failures))
		failures++
		if !waitContext(ctx, delay) {
			r.settleError(lastErr, fail)
			return
		}
	}
}

func (r *Receiver) settleError(err error, fail func(error)) {
	if err == nil {
		return
	}
	r.notify(err)
	// Keep HTTP status authoritative after the retry budget is exhausted. A
	// gateway 5xx body can contain a misleading permanent broker code.
	if permanentReceiverError(err) && !retryableOperationError(err) {
		fail(err)
	}
}

func renewalResponseHeadroom(responseRTT time.Duration) time.Duration {
	if responseRTT <= 0 {
		return leaseDeadlineSafety + renewRTTMargin
	}
	return 2*responseRTT + leaseDeadlineSafety + renewRTTMargin
}

func needsInitialLeaseConfirmation(lease renewalLease, remaining time.Duration) bool {
	if lease.clockUnavailable || lease.deadlineUntrusted {
		return true
	}
	if remaining <= minimumAutoRenewInterval+leaseDeadlineSafety {
		return true
	}
	headroom := renewalResponseHeadroom(lease.responseRTT)
	if lease.clockResolution >= time.Second {
		headroom += lease.clockResolution
	}
	return (lease.responseRTT > 0 || lease.clockResolution >= time.Second) && remaining <= headroom
}

func coarseLeaseNeedsUntrustedSchedule(lease renewalLease, remaining time.Duration) bool {
	if lease.clockResolution < time.Second {
		return false
	}
	return remaining <= lease.clockResolution+renewalResponseHeadroom(lease.responseRTT)
}

func leaseNeedsUntrustedSchedule(lease renewalLease, remaining time.Duration) bool {
	if lease.clockUnavailable || lease.deadlineUntrusted {
		return true
	}
	return coarseLeaseNeedsUntrustedSchedule(lease, remaining)
}

func pathologicalExactLease(lease renewalLease, remaining time.Duration) bool {
	return !lease.deadlineUntrusted && !lease.clockUnavailable && lease.clockResolution < time.Second && remaining <= minimumAutoRenewInterval+leaseDeadlineSafety
}

func autoRenewInterval(remaining, responseRTT time.Duration) time.Duration {
	if remaining <= leaseDeadlineSafety {
		// A fresh response can carry a non-zero deadline that elapsed in transit.
		// Probe it immediately so a handler is not left running for another fixed
		// interval without a confirmed lock. Success installs a new deadline;
		// failures either back off or terminate, so this does not form a hot loop.
		return 0
	}
	usable := remaining - leaseDeadlineSafety
	half := usable / 2
	if half <= 0 {
		return 0
	}
	interval := usable / 3
	// Renew near one-third of the way through a normal lease, but avoid a
	// needlessly hot steady-state cadence. The half-lease cap keeps the 50ms
	// guard from ever scheduling at or after a short deadline.
	if interval < minimumAutoRenewInterval {
		interval = minimumAutoRenewInterval
	}
	if interval > half {
		interval = half
	}
	latest := remaining - renewalResponseHeadroom(responseRTT)
	if latest <= 0 {
		return 0
	}
	if interval > latest {
		interval = latest
	}
	return interval
}

func anchorLeaseDeadline(remote time.Time) (time.Time, time.Duration) {
	if remote.IsZero() {
		return time.Time{}, 0
	}
	now := time.Now()
	remaining := remote.Sub(now)
	// Wire timestamps carry wall time only. Rebuild the deadline from time.Now
	// so later time.Until calls use Go's monotonic clock and ignore wall-clock
	// corrections after this response was received.
	return now.Add(remaining), remaining
}

func messageLeaseDeadline(message *Message) (time.Time, time.Duration) {
	if !message.localLeaseDeadline.IsZero() {
		return message.localLeaseDeadline, time.Until(message.localLeaseDeadline)
	}
	return anchorLeaseDeadline(message.LockedUntil)
}

func (r *Receiver) renewalAttemptContext(parent context.Context, deadline time.Time) (context.Context, context.CancelFunc) {
	if deadline.IsZero() {
		// Scheduling cadence and request timeout serve different purposes. Keep
		// probes sparse enough to protect the broker, but allow ordinary public or
		// cross-region RTT before deciding that the lease can no longer be proven.
		return context.WithTimeout(parent, r.renewAttemptTimeout)
	}
	// Stop the RPC before the last safety margin is spent. deadline was
	// re-anchored onto Go's monotonic clock when it entered the renewal state.
	return context.WithDeadline(parent, deadline.Add(-leaseDeadlineSafety))
}

func (r *Receiver) initialRenewalAttemptContext(parent context.Context, trustedDeadline time.Time) (context.Context, context.CancelFunc) {
	if trustedDeadline.IsZero() {
		return context.WithTimeout(parent, r.renewAttemptTimeout)
	}
	deadline := trustedDeadline.Add(-leaseDeadlineSafety)
	maximum := time.Now().Add(maxInitialRenewAttemptTimeout)
	if deadline.After(maximum) {
		deadline = maximum
	}
	// A non-zero mapped deadline is a hard ownership boundary. If scheduling or
	// callback work already consumed it, WithDeadline returns an immediately
	// canceled context instead of silently widening the attempt to one second.
	return context.WithDeadline(parent, deadline)
}

const minimumRenewRetryHeadroom = 5 * time.Millisecond

func retryableRenewError(err error) bool {
	return retryableOperationError(err)
}

func (r *Receiver) renewMessage(ctx context.Context, message *Message, leaseLength time.Duration, current renewalLease) (renewalLease, error) {
	if source, ok := r.src.(leaseRenewSource); ok {
		r.renewCapabilityMu.Lock()
		unsupported := r.renewBatchUnsupported
		r.renewCapabilityMu.Unlock()
		if !unsupported {
			lease, err := source.renewLease(ctx, message)
			if err == nil {
				return lease, nil
			}
			if !errors.Is(err, ErrUnsupported) {
				return renewalLease{}, err
			}
			// Capability absence is stable for one Receiver endpoint. Cache only
			// the result; never hold the mutex across the network request. Concurrent
			// first renewals may probe in parallel rather than blocking one another's
			// leases behind a slow request.
			r.renewCapabilityMu.Lock()
			r.renewBatchUnsupported = true
			r.renewCapabilityMu.Unlock()
		}
	}

	started := time.Now()
	if err := message.Renew(ctx); err != nil {
		return renewalLease{}, err
	}
	if leaseLength <= 0 {
		current.deadline = time.Time{}
		current.deadlineUntrusted = true
		return current, nil
	}
	// Legacy single Renew does not return a deadline. The broker extends by the
	// queue lock duration; the delivery's initial remaining lease is the safest
	// estimate available without a management-permission metadata call.
	current.deadline = started.Add(leaseLength)
	return current, nil
}

func (r *Receiver) loseLease(cancelHandler context.CancelFunc, cause error) error {
	err := cause
	if !errors.Is(err, ErrLockLost) {
		err = fmt.Errorf("%w: automatic renewal failed before the lease could be confirmed: %v", ErrLockLost, cause)
	}
	cancelHandler()
	r.notify(err)
	return err
}

func (r *Receiver) renewLoop(ctx context.Context, message *Message, cancelHandler context.CancelFunc, fail func(error), deadline time.Time, leaseLength time.Duration, lease renewalLease) error {
	failures := 0

	for {
		var delay time.Duration
		if failures == 0 {
			if deadline.IsZero() {
				delay = uncertainLeaseRenewPeriod
			} else {
				remaining := time.Until(deadline)
				if pathologicalExactLease(lease, remaining) {
					return r.loseLease(cancelHandler, fmt.Errorf("%w: renewed lease is too short for the minimum renewal interval", ErrLockLost))
				}
				delay = autoRenewInterval(remaining, lease.responseRTT)
			}
		} else {
			remaining := time.Until(deadline) - leaseDeadlineSafety
			if !deadline.IsZero() && remaining <= 2*minimumRenewRetryHeadroom {
				return r.loseLease(cancelHandler, ErrLockLost)
			}
			delay = r.jitter(exponentialBackoff(r.renewRetryBackoff, r.maxRenewBackoff, failures-1))
			if !deadline.IsZero() {
				latest := remaining - minimumRenewRetryHeadroom
				if delay > latest {
					delay = latest
				}
				if delay < minimumRenewRetryHeadroom {
					delay = minimumRenewRetryHeadroom
				}
			}
		}
		if !waitContext(ctx, delay) {
			return nil
		}

		renewCtx, cancelRenew := r.renewalAttemptContext(ctx, deadline)
		renewed, err := r.renewMessage(renewCtx, message, leaseLength, lease)
		attemptErr := renewCtx.Err()
		cancelRenew()
		if ctx.Err() != nil {
			return nil
		}
		if attemptErr != nil {
			// The attempt consumed every locally defensible millisecond. Even a
			// late transport result cannot restore a safe settlement window.
			return r.loseLease(cancelHandler, attemptErr)
		}
		if err == nil {
			failures = 0
			lease = renewed
			r.notifyDegradedClock(lease.clockUnavailable)
			if ctx.Err() != nil {
				return nil
			}
			confirmedDeadline, remaining := anchorLeaseDeadline(lease.deadline)
			if pathologicalExactLease(lease, remaining) {
				return r.loseLease(cancelHandler, fmt.Errorf("%w: renewed lease is too short for the minimum renewal interval", ErrLockLost))
			}
			if remaining > leaseDeadlineSafety {
				deadline = confirmedDeadline
				leaseLength = remaining
				if leaseNeedsUntrustedSchedule(lease, remaining) {
					deadline = time.Time{}
					leaseLength = 0
				}
			} else {
				deadline = time.Time{}
				leaseLength = 0
			}
			continue
		}
		if !retryableRenewError(err) {
			if permanentReceiverError(err) {
				cancelHandler()
				r.settleError(err, fail)
				return err
			}
			return r.loseLease(cancelHandler, err)
		}
		if deadline.IsZero() {
			// The last acknowledgement proved ownership, but an unusable remote
			// deadline gives no local retry budget. Cancel rather than allowing a
			// second handler to overlap after an unbounded transient outage.
			return r.loseLease(cancelHandler, err)
		}
		failures++
	}
}

// Close releases Receiver resources. A Receiver owns no background work outside
// Run, so Close is currently a no-op kept for parity with the main MQLite SDK.
func (r *Receiver) Close() error { return nil }
