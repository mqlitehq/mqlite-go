package mqlite

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
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
	receiveBackoffBase            = 500 * time.Millisecond
	receiveBackoffMax             = 3 * time.Second
	// Bound one server-paced Receive wait so stale intermediary metadata cannot
	// silently park a long-lived consumer.
	receiveRetryAfterMax = 60 * time.Second
	settleTimeout        = 30 * time.Second
	settleAttemptTimeout = 15 * time.Second
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
	// Legacy Renew returns no deadline. Keep its fixed conservative duration
	// estimate separate from the current RPC's time-depleted lease window.
	legacyLeaseEstimate time.Duration
}

type leaseRenewSource interface {
	renewLease(context.Context, *Message) (renewalLease, error)
}

type initialRenewalResult struct {
	lease           renewalLease
	err, attemptErr error
}

type initialBatchRenewer interface {
	renewInitialBatch(context.Context, string, []*Message) ([]initialRenewalResult, error)
}

// A legacy endpoint confirms the oldest lease first. This barrier ends at the
// confirmation boundary, so it never serializes user handlers or settlement.
type initialRenewalPlan struct {
	result          *initialRenewalResult
	previous        <-chan struct{}
	done            chan struct{}
	once            sync.Once
	attemptDeadline time.Time
}

func (p *initialRenewalPlan) release() {
	if p != nil && p.done != nil {
		p.once.Do(func() { close(p.done) })
	}
}

func (p *initialRenewalPlan) wait(ctx context.Context) bool {
	if p == nil || p.previous == nil {
		return ctx.Err() == nil
	}
	select {
	case <-p.previous:
		return ctx.Err() == nil
	case <-ctx.Done():
		return false
	}
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

	errMu               sync.Mutex
	renewRetryBackoff   time.Duration
	maxRenewBackoff     time.Duration
	settleBackoff       time.Duration
	maxSettleBackoff    time.Duration
	renewAttemptTimeout time.Duration
	jitter              func(time.Duration) time.Duration
	// Receive has separate timing seams so its reconnect policy can be tested
	// deterministically without changing renewal or settlement scheduling.
	receiveJitter         func(time.Duration) time.Duration
	receiveWait           func(context.Context, time.Duration) bool
	settlement            settlementTiming
	renewCapabilityMu     sync.Mutex
	renewBatchUnsupported bool
	clockWarningOnce      sync.Once
}

// settlementTiming keeps confirmation budgets private and lets tests advance
// request and retry deadlines without sleeping through remote round trips.
type settlementTiming struct {
	totalTimeout   time.Duration
	attemptTimeout time.Duration
	withTimeout    func(context.Context, time.Duration) (context.Context, context.CancelFunc)
	wait           func(context.Context, time.Duration) bool
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
		renewRetryBackoff:   100 * time.Millisecond,
		maxRenewBackoff:     time.Second,
		settleBackoff:       100 * time.Millisecond,
		maxSettleBackoff:    time.Second,
		renewAttemptTimeout: defaultRenewAttemptTimeout,
		jitter:              jitterBackoff,
		receiveJitter:       jitterBackoff,
		receiveWait:         waitContext,
		settlement:          settlementTiming{totalTimeout: settleTimeout, attemptTimeout: settleAttemptTimeout, withTimeout: context.WithTimeout, wait: waitContext},
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

// waitRenewal stops an idle heartbeat at an RPC boundary without canceling
// an already issued request. The request keeps its original ownership watchdog.
func waitRenewal(ctx context.Context, stop <-chan struct{}, delay time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-stop:
		return false
	default:
	}
	if delay <= 0 {
		return true
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-stop:
		return false
	case <-timer.C:
		return true
	}
}

// receiveRetryDelay returns the wait before replaying the current Receive
// attempt. The first transient failure adds no client-generated delay so a
// response lost after a committed claim is replayed promptly. Later generated
// delays use equal-jitter exponential backoff capped at three seconds.
// A valid Retry-After from HTTP 429/503 can raise that generated delay, up to
// receiveRetryAfterMax. The boolean reports whether the server hint raised the
// delay, so even a first failure is observable before the Receiver waits.
func (r *Receiver) receiveRetryDelay(failures int, err error) (time.Duration, bool) {
	delay := time.Duration(0)
	if failures > 1 {
		nominal := exponentialBackoff(receiveBackoffBase, receiveBackoffMax, failures-2)
		delay = r.receiveJitter(nominal)
		if delay < 0 {
			delay = 0
		}
		if delay > receiveBackoffMax {
			delay = receiveBackoffMax
		}
	}
	if hint, ok := managedReceiveRetryAfter(err); ok {
		if hint > receiveRetryAfterMax {
			hint = receiveRetryAfterMax
		}
		if hint > delay {
			return hint, true
		}
	}
	return delay, false
}

// managedReceiveRetryAfter returns a private server pacing hint only for the
// statuses whose HTTP semantics permit it here. Renew and settlement retry
// paths intentionally do not consult this metadata.
func managedReceiveRetryAfter(err error) (time.Duration, bool) {
	var responseErr *HTTPError
	if !errors.As(err, &responseErr) || responseErr == nil || !responseErr.retryAfterOK {
		return 0, false
	}
	if responseErr.StatusCode != 429 && responseErr.StatusCode != 503 {
		return 0, false
	}
	return responseErr.retryAfter, true
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
// honor ctx and return. When settlement remains safe, a returning handler gets a
// fresh 30-second confirmation budget, with at most fifteen seconds per attempt,
// even during shutdown. These cooperative contexts cannot bound a handler or
// custom HTTP transport that ignores cancellation.
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
			delay, serverPaced := r.receiveRetryDelay(failures, err)
			if failures > 1 || serverPaced {
				r.notify(err)
			}
			if !r.receiveWait(runCtx, delay) {
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
		plans, preparationErr := r.prepareInitialRenewals(runCtx, messages, forceInitialRenewal)
		if runCtx.Err() != nil {
			releaseWorkerPairs(handlerSlots, totalSlots, len(messages))
			return finish()
		}
		if preparationErr != nil {
			// A whole-batch permanent failure must fence Run before any handler
			// can run or release capacity for a new claim (MQLITE-183).
			fail(preparationErr)
			r.notify(preparationErr)
			releaseWorkerPairs(handlerSlots, totalSlots, len(messages))
			return finish()
		}
		for i, message := range messages {
			workers.Add(1)
			go func(m *Message, forceInitialRenewal bool, plan *initialRenewalPlan) {
				defer workers.Done()
				defer func() { <-totalSlots }()
				r.processWithHandlerRelease(runCtx, m, handler, func() { <-handlerSlots }, fail, forceInitialRenewal, plan)
			}(message, forceInitialRenewal, plans[i])
		}
	}
}

func initialMessageLease(message *Message) (time.Time, time.Duration, renewalLease, time.Time) {
	deadline, length := messageLeaseDeadline(message)
	lease := renewalLease{deadline: deadline, responseRTT: message.leaseResponseRTT,
		clockResolution: message.leaseClockResolution, clockUnavailable: message.leaseClockUnavailable}
	initialDeadline := time.Time{}
	remaining := time.Until(message.localLeaseDeadline)
	if !message.localLeaseDeadline.IsZero() && remaining > leaseDeadlineSafety && !leaseNeedsUntrustedSchedule(lease, remaining) {
		initialDeadline = message.localLeaseDeadline
	}
	return deadline, length, lease, initialDeadline
}

// prepareInitialRenewals collapses only the confirmations needed before user
// dispatch. Background renewals and the handling/settlement handoff are unchanged.
func (r *Receiver) prepareInitialRenewals(ctx context.Context, messages []*Message, force bool) ([]*initialRenewalPlan, error) {
	plans := make([]*initialRenewalPlan, len(messages))
	var selected []*Message
	var indexes []int
	pointers := make(map[*Message]bool)
	sequences := make(map[int64]bool)
	for i, message := range messages {
		_, remaining, lease, trustedDeadline := initialMessageLease(message)
		if !force && !(r.cfg.autoRenew && needsInitialLeaseConfirmation(lease, remaining)) {
			continue
		}
		if pointers[message] || message.SequenceNumber > 0 && sequences[message.SequenceNumber] {
			return nil, fmt.Errorf("%w: duplicate message in initial confirmation batch", ErrInvalidArgument)
		}
		pointers[message] = true
		sequences[message.SequenceNumber] = true
		attemptCtx, cancel := r.initialRenewalAttemptContext(ctx, trustedDeadline)
		deadline, _ := attemptCtx.Deadline()
		cancel()
		plans[i] = &initialRenewalPlan{attemptDeadline: deadline}
		selected = append(selected, message)
		indexes = append(indexes, i)
	}
	if len(selected) < 2 {
		// Preserve the existing single-message path and its watchdog timing.
		for _, i := range indexes {
			plans[i] = nil
		}
		return plans, nil
	}
	r.renewCapabilityMu.Lock()
	unsupported := r.renewBatchUnsupported
	r.renewCapabilityMu.Unlock()
	if source, ok := r.src.(initialBatchRenewer); ok && !unsupported && len(selected) <= MaxRenewBatch {
		deadline := plans[indexes[0]].attemptDeadline
		for _, i := range indexes[1:] {
			if plans[i].attemptDeadline.Before(deadline) {
				deadline = plans[i].attemptDeadline
			}
		}
		attemptCtx, cancel := context.WithDeadline(ctx, deadline)
		results, err := callInitialBatch(attemptCtx, source, r.queue, selected)
		attemptErr := attemptCtx.Err()
		cancel()
		if ctx.Err() != nil {
			return plans, nil
		}
		if errors.Is(err, ErrUnsupported) {
			r.renewCapabilityMu.Lock()
			r.renewBatchUnsupported = true
			r.renewCapabilityMu.Unlock()
		} else {
			if err != nil && !retryableRenewError(err) && permanentReceiverError(err) {
				return plans, err
			}
			if err == nil && len(results) != len(selected) {
				err = fmt.Errorf("mqlite: initial renewal returned %d results for %d messages", len(results), len(selected))
			}
			for n, i := range indexes {
				result := initialRenewalResult{err: err, attemptErr: attemptErr}
				if err == nil {
					result = results[n]
					result.attemptErr = attemptErr
				}
				if result.err != nil && !retryableRenewError(result.err) && permanentReceiverError(result.err) {
					return plans, result.err
				}
				plans[i].result = &result
			}
			return plans, nil
		}
	}
	// Unsupported/custom sources retain the old per-item protocol. Stable
	// deadline ordering spends each original watchdog while waiting its turn.
	sort.SliceStable(indexes, func(i, j int) bool {
		return plans[indexes[i]].attemptDeadline.Before(plans[indexes[j]].attemptDeadline)
	})
	var previous <-chan struct{}
	for _, i := range indexes {
		plans[i].previous = previous
		plans[i].done = make(chan struct{})
		previous = plans[i].done
	}
	return plans, nil
}

func callInitialBatch(ctx context.Context, source initialBatchRenewer, queue string, messages []*Message) ([]initialRenewalResult, error) {
	type outcome struct {
		results []initialRenewalResult
		err     error
	}
	done := make(chan outcome, 1)
	go func() {
		result := outcome{err: fmt.Errorf("%w: initial batch confirmation terminated without a result", ErrLockLost)}
		// A transport's Goexit must not strand Run. Panics retain their existing
		// behavior; cooperative cancellation still joins the actual invocation.
		defer func() { done <- result }()
		result.results, result.err = source.renewInitialBatch(ctx, queue, messages)
	}()
	result := <-done
	return result.results, result.err
}

func (r *Receiver) process(ctx context.Context, message *Message, handler func(context.Context, *Message) error, fail func(error)) {
	r.processWithHandlerRelease(ctx, message, handler, func() {}, fail, false)
}

func (r *Receiver) processWithHandlerRelease(ctx context.Context, message *Message, handler func(context.Context, *Message) error, releaseHandler func(), fail func(error), forceInitialRenewal bool, initial ...*initialRenewalPlan) {
	var plan *initialRenewalPlan
	if len(initial) != 0 {
		plan = initial[0]
	}
	defer plan.release()
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
	var renewExited, stopHandlingRenew chan struct{}
	var renewErr error
	var renewAborted bool
	var progress renewalProgress
	deadline, leaseLength, lease, initialAttemptDeadline := initialMessageLease(message)
	r.notifyDegradedClock(lease.clockUnavailable)
	if ctx.Err() != nil {
		return
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
		var renewed renewalLease
		var err, attemptErr error
		if plan != nil && plan.result != nil {
			renewed, err, attemptErr = plan.result.lease, plan.result.err, plan.result.attemptErr
		} else {
			var attemptCtx context.Context
			var cancelAttempt context.CancelFunc
			if plan != nil {
				attemptCtx, cancelAttempt = context.WithDeadline(ctx, plan.attemptDeadline)
			} else {
				attemptCtx, cancelAttempt = r.initialRenewalAttemptContext(ctx, initialAttemptDeadline)
			}
			defer cancelAttempt()
			if plan.wait(attemptCtx) {
				renewed, err = r.renewMessage(attemptCtx, message, leaseLength, lease)
			}
			attemptErr = attemptCtx.Err()
			cancelAttempt()
		}
		if attemptErr != nil || err == nil || retryableRenewError(err) || !permanentReceiverError(err) {
			plan.release()
		}
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
					r.stopPermanentRenewal(cancelHandler, err, fail)
					return
				}
				// A definitive protocol/configuration rejection keeps its cause;
				// it does not prove that the broker rejected this lock token.
				cancelHandler()
				r.notify(err)
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
			if lease.legacyLeaseEstimate > 0 {
				leaseLength = lease.legacyLeaseEstimate
			}
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
		renewExited, stopHandlingRenew = make(chan struct{}), make(chan struct{})
		progress = renewalProgress{deadline: deadline, leaseLength: leaseLength, lease: lease}
		go func() {
			err, returned := ErrLockLost, false
			defer func() {
				if !returned {
					renewAborted = true
					cancelHandler()
				}
				renewDone <- err
				close(renewExited)
			}()
			err = r.renewLoop(renewCtx, message, cancelHandler, fail, &progress, false, stopHandlingRenew)
			returned = true
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
	// Start the existing confirmation budget at handler return. Let an issued
	// renewal finish inside its ownership watchdog and adopt its confirmed lease,
	// instead of canceling and reissuing the same broker write during handoff.
	settleCtx, cancelSettle := r.settlement.withTimeout(context.Background(), r.settlement.totalTimeout)
	defer cancelSettle()
	if stopHandlingRenew != nil {
		close(stopHandlingRenew)
		select {
		case <-renewExited:
		case <-ctx.Done():
		case <-settleCtx.Done():
		}
	}
	// Run cancellation, budget expiry and abnormal exits still cancel and join.
	stopRenew()
	if renewErr != nil {
		if renewAborted {
			r.notify(fmt.Errorf("%w: handling heartbeat terminated without a result", ErrLockLost))
		}
		// The renew loop already canceled the message-scoped handler and reported
		// the terminal renewal failure. Settling after a known renewal failure can
		// only race a lost token or repeat the same permanent authorization error.
		return
	}
	// Match broker settlement receipts by replaying this exact fenced request;
	// the shared confirmation budget also bounds cleanup during shutdown.
	stopSettlingRenew := func() {}
	if r.cfg.autoRenew {
		renewCtx, cancel := context.WithCancel(settleCtx)
		var settlingWG sync.WaitGroup
		settlingWG.Add(1)
		go func() {
			defer settlingWG.Done()
			r.renewLoop(renewCtx, message, cancelHandler, fail, &progress, true, nil)
		}()
		var once sync.Once
		stopSettlingRenew = func() { once.Do(func() { cancel(); settlingWG.Wait() }) }
	}
	// Run cancellation still cancels a running handler. Once it returned, its
	// heartbeat shares only the existing settlement confirmation budget.
	defer stopSettlingRenew()
	var err error
	if handlerErr != nil {
		err = r.retrySettlement(settleCtx, func(ctx context.Context) error {
			return message.Abandon(ctx)
		})
	} else {
		err = r.retrySettlement(settleCtx, func(ctx context.Context) error {
			return message.Complete(ctx)
		})
	}
	stopSettlingRenew()
	r.settleError(err, fail)
}

func (r *Receiver) settleWithRetry(ctx context.Context, settle func(context.Context) error, fail func(error)) {
	r.settleError(r.retrySettlement(ctx, settle), fail)
}

// settlementConfirmationError preserves the last transport or HTTP cause while
// marking an exhausted confirmation budget as an unconfirmed settlement outcome.
type settlementConfirmationError struct{ cause error }

func (e *settlementConfirmationError) Error() string {
	return fmt.Sprintf("mqlite: settlement outcome unknown after confirmation budget expired: %v", e.cause)
}
func (e *settlementConfirmationError) Unwrap() error        { return e.cause }
func (e *settlementConfirmationError) Is(target error) bool { return target == ErrOutcomeUnknown }

func (r *Receiver) retrySettlement(ctx context.Context, settle func(context.Context) error) error {
	failures := 0
	var lastErr error
	for {
		if ctx.Err() != nil {
			if lastErr == nil {
				return ctx.Err() // no request was attempted
			}
			return &settlementConfirmationError{cause: lastErr}
		}
		callCtx, cancel := r.settlement.withTimeout(ctx, r.settlement.attemptTimeout)
		lastErr = settle(callCtx)
		cancel()
		// Confirmed acknowledgements and definitive errors win a deadline race.
		if lastErr == nil || !retryableOperationError(lastErr) {
			return lastErr
		}
		if ctx.Err() != nil {
			return &settlementConfirmationError{cause: lastErr}
		}
		delay := r.jitter(exponentialBackoff(r.settleBackoff, r.maxSettleBackoff, failures))
		failures++
		if !r.settlement.wait(ctx, delay) {
			return &settlementConfirmationError{cause: lastErr}
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

func (r *Receiver) stopPermanentRenewal(cancelHandler context.CancelFunc, err error, fail func(error)) {
	// Stop the receive loop before releasing handler capacity. Otherwise the
	// handler can observe cancellation and free its slot while a synchronous
	// error callback is still running, allowing one more claim with credentials
	// the broker has already rejected.
	fail(err)
	cancelHandler()
	r.notify(err)
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
		current.legacyLeaseEstimate = 0
		return current, nil
	}
	// Legacy single Renew does not return a deadline. The broker extends by the
	// queue lock duration; the delivery's initial remaining lease is the safest
	// estimate available without a management-permission metadata call.
	current.deadline = started.Add(leaseLength)
	current.legacyLeaseEstimate = leaseLength
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

// renewalProgress is owned by one heartbeat at a time and transferred only
// after joining it. Public Message metadata remains immutable during renewal.
type renewalProgress struct {
	deadline    time.Time
	leaseLength time.Duration
	lease       renewalLease
}

func (r *Receiver) renewLoop(ctx context.Context, message *Message, cancelHandler context.CancelFunc, fail func(error), progress *renewalProgress, settling bool, stop <-chan struct{}) error {
	deadline, leaseLength, lease := progress.deadline, progress.leaseLength, progress.lease
	defer func() { *progress = renewalProgress{deadline: deadline, leaseLength: leaseLength, lease: lease} }()
	lose := func(cause error) error {
		if settling {
			// An already committed settlement makes Renew lose its token before
			// the acknowledgement or exact receipt replay arrives. The final
			// settlement alone resolves this: never replace Unknown with loss,
			// or emit a second loss after an unsuccessful settlement.
			return nil
		}
		return r.loseLease(cancelHandler, cause)
	}
	failures := 0

	for {
		var delay time.Duration
		if failures == 0 {
			if deadline.IsZero() {
				delay = uncertainLeaseRenewPeriod
			} else {
				remaining := time.Until(deadline)
				if pathologicalExactLease(lease, remaining) {
					return lose(fmt.Errorf("%w: renewed lease is too short for the minimum renewal interval", ErrLockLost))
				}
				delay = autoRenewInterval(remaining, lease.responseRTT)
			}
		} else {
			remaining := time.Until(deadline) - leaseDeadlineSafety
			if !deadline.IsZero() && remaining <= 2*minimumRenewRetryHeadroom {
				return lose(ErrLockLost)
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
		if !waitRenewal(ctx, stop, delay) {
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
			return lose(attemptErr)
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
				return lose(fmt.Errorf("%w: renewed lease is too short for the minimum renewal interval", ErrLockLost))
			}
			if remaining > leaseDeadlineSafety {
				deadline = confirmedDeadline
				leaseLength = remaining
				if lease.legacyLeaseEstimate > 0 {
					leaseLength = lease.legacyLeaseEstimate
				}
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
				r.stopPermanentRenewal(cancelHandler, err, fail)
				return err
			}
			if errors.Is(err, ErrLockLost) {
				return lose(err) // Its own settlement may already have released the token.
			}
			cancelHandler()
			r.notify(err)
			return err
		}
		if deadline.IsZero() {
			// The last acknowledgement proved ownership, but an unusable remote
			// deadline gives no local retry budget. Cancel rather than allowing a
			// second handler to overlap after an unbounded transient outage.
			return lose(err)
		}
		failures++
	}
}

// Close releases Receiver resources. A Receiver owns no background work outside
// Run, so Close is currently a no-op kept for parity with the main MQLite SDK.
func (r *Receiver) Close() error { return nil }
