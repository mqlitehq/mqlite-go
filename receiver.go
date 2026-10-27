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

type receiveSource interface {
	settler
	receiveOne(context.Context, string, int, int64, receiveMode, string) ([]*Message, error)
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

// WithConcurrency sets the maximum number of concurrent handlers.
func WithConcurrency(n int) ReceiverOption {
	return func(c *receiverConfig) { c.concurrency = n }
}

// WithPrefetch caps each claim. It is always clamped to the number of idle
// workers, so a message never waits behind another handler while already locked.
func WithPrefetch(n int) ReceiverOption {
	return func(c *receiverConfig) { c.prefetch = n }
}

// WithErrorHandler receives transient receive failures and settlement failures.
// Calls are serialized and the callback must return promptly.
func WithErrorHandler(fn func(error)) ReceiverOption {
	return func(c *receiverConfig) { c.onError = fn }
}

// Receiver repeatedly claims messages and dispatches them to a handler. A nil
// handler result completes the message; a non-nil result abandons it.
type Receiver struct {
	src   receiveSource
	queue string
	cfg   receiverConfig

	errMu            sync.Mutex
	transientBackoff time.Duration
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
		src:              src,
		queue:            queue,
		cfg:              cfg,
		transientBackoff: 500 * time.Millisecond,
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

func permanentReceiverError(err error) bool {
	return errors.Is(err, ErrUnauthenticated) ||
		errors.Is(err, ErrPermissionDenied) ||
		errors.Is(err, ErrNotFound) ||
		errors.Is(err, ErrQueueNotFound) ||
		errors.Is(err, ErrUnsupported)
}

func reserveWorkers(ctx context.Context, sem chan struct{}, max int) int {
	select {
	case sem <- struct{}{}:
	case <-ctx.Done():
		return 0
	}

	reserved := 1
	for reserved < max {
		select {
		case sem <- struct{}{}:
			reserved++
		default:
			return reserved
		}
	}
	return reserved
}

// Run processes messages until ctx is canceled or a permanent broker error is
// observed. It returns ctx.Err() for normal cancellation. Run waits for active
// handlers and lock renewals to exit, so handlers must honor ctx and return.
func (r *Receiver) Run(ctx context.Context, handler func(context.Context, *Message) error) error {
	batch := r.cfg.prefetch
	if batch <= 0 || batch > r.cfg.concurrency {
		batch = r.cfg.concurrency
	}

	sem := make(chan struct{}, r.cfg.concurrency)
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
		reserved := reserveWorkers(runCtx, sem, batch)
		if reserved == 0 {
			return finish()
		}
		if runCtx.Err() != nil {
			for i := 0; i < reserved; i++ {
				<-sem
			}
			return finish()
		}

		attemptID := newAttemptID()
		messages, err := r.src.receiveOne(runCtx, r.queue, reserved, 20000, receivePeekLock, attemptID)
		if err != nil && !permanentReceiverError(err) && runCtx.Err() == nil {
			messages, err = r.src.receiveOne(runCtx, r.queue, reserved, 20000, receivePeekLock, attemptID)
		}
		if err != nil {
			for i := 0; i < reserved; i++ {
				<-sem
			}
			if permanentReceiverError(err) {
				r.notify(err)
				fail(err)
				return finish()
			}
			if runCtx.Err() != nil {
				return finish()
			}
			r.notify(err)
			select {
			case <-runCtx.Done():
				return finish()
			case <-time.After(r.transientBackoff):
			}
			continue
		}
		if len(messages) > reserved {
			for i := 0; i < reserved; i++ {
				<-sem
			}
			err = fmt.Errorf("mqlite: broker returned %d messages for a receive capped at %d", len(messages), reserved)
			r.notify(err)
			fail(err)
			return finish()
		}

		for i := len(messages); i < reserved; i++ {
			<-sem
		}
		for _, message := range messages {
			workers.Add(1)
			go func(m *Message) {
				defer workers.Done()
				defer func() { <-sem }()
				r.process(runCtx, m, handler, fail)
			}(message)
		}
	}
}

func (r *Receiver) process(ctx context.Context, message *Message, handler func(context.Context, *Message) error, fail func(error)) {
	var cancelRenew context.CancelFunc
	var renewWG *sync.WaitGroup
	if r.cfg.autoRenew {
		renewCtx, cancel := context.WithCancel(ctx)
		cancelRenew = cancel
		renewWG = new(sync.WaitGroup)
		renewWG.Add(1)
		go func() {
			defer renewWG.Done()
			r.renewLoop(renewCtx, message, fail)
		}()
	}

	handlerErr := handler(ctx, message)
	if cancelRenew != nil {
		// A lock token must never be renewed concurrently with its terminal
		// settlement. Stop and join the renew loop before Complete or Abandon.
		cancelRenew()
		renewWG.Wait()
	}
	settleCtx, cancelSettle := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelSettle()
	if handlerErr != nil {
		r.settleError(message.Abandon(settleCtx), fail)
		return
	}
	r.settleError(message.Complete(settleCtx), fail)
}

func (r *Receiver) settleError(err error, fail func(error)) {
	if err == nil {
		return
	}
	r.notify(err)
	if permanentReceiverError(err) {
		fail(err)
	}
}

func autoRenewInterval(remaining time.Duration) time.Duration {
	if remaining <= 0 {
		// A missing deadline should not create a tight loop. The broker will
		// either accept this conservative fallback renewal or return lock_lost.
		return 10 * time.Second
	}
	interval := remaining / 2
	if interval <= 0 {
		return time.Nanosecond
	}
	return interval
}

func retryableRenewError(err error) bool {
	return err != nil &&
		!errors.Is(err, ErrLockLost) &&
		!errors.Is(err, ErrInvalidArgument) &&
		!permanentReceiverError(err)
}

func (r *Receiver) renewLoop(ctx context.Context, message *Message, fail func(error)) {
	interval := autoRenewInterval(time.Until(message.LockedUntil))

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			err := message.Renew(ctx)
			if err != nil && ctx.Err() == nil && retryableRenewError(err) {
				// Renew is fenced and only extends a deadline, so one replay is
				// safe when the first response is lost or a transport fails. Retry
				// before invoking user observation code so a slow callback cannot
				// consume the remaining lease.
				firstErr := err
				err = message.Renew(ctx)
				r.notify(firstErr)
			}
			if err != nil {
				if ctx.Err() == nil {
					r.settleError(err, fail)
				}
				return
			}
		}
	}
}

// Close releases Receiver resources. A Receiver owns no background work outside
// Run, so Close is currently a no-op kept for parity with the main MQLite SDK.
func (r *Receiver) Close() error { return nil }
