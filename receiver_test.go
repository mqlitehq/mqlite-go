package mqlite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mqlitehq/mqlite-go/internal/wire"
)

type receiveRecord struct {
	max       int
	waitMs    int64
	mode      receiveMode
	attemptID string
}

type settleRecord struct {
	op     string
	queue  string
	seq    int64
	token  string
	delay  int64
	reason string
	detail string
}

type fakeReceiveSource struct {
	mu         sync.Mutex
	receives   []receiveRecord
	settles    []settleRecord
	receiveFn  func(context.Context, int, receiveRecord) ([]*Message, error)
	settleErr  error
	settleFn   func(settleRecord, int) error
	renewFn    func(int) error
	renewCalls int
	renewed    chan struct{}
}

type blockingRenewSettler struct {
	renewStarted     chan struct{}
	renewCanceled    chan struct{}
	allowRenewReturn chan struct{}
	completeCalled   chan struct{}

	mu         sync.Mutex
	operations []string
}

type concurrentLeaseSource struct {
	*fakeReceiveSource
	calls   int32
	both    chan struct{}
	release chan struct{}
}

type deadlineLeaseSource struct {
	*fakeReceiveSource
	renewLeaseFn func(context.Context, *Message) (time.Time, error)
}

func (s *deadlineLeaseSource) renewLease(ctx context.Context, message *Message) (time.Time, error) {
	return s.renewLeaseFn(ctx, message)
}

func (s *concurrentLeaseSource) renewLease(context.Context, *Message) (time.Time, error) {
	if atomic.AddInt32(&s.calls, 1) == 2 {
		close(s.both)
	}
	<-s.release
	return time.Now().Add(time.Second), nil
}

func newBlockingRenewSettler() *blockingRenewSettler {
	return &blockingRenewSettler{
		renewStarted:     make(chan struct{}),
		renewCanceled:    make(chan struct{}),
		allowRenewReturn: make(chan struct{}),
		completeCalled:   make(chan struct{}),
	}
}

func (s *blockingRenewSettler) record(operation string) {
	s.mu.Lock()
	s.operations = append(s.operations, operation)
	s.mu.Unlock()
}

func (s *blockingRenewSettler) snapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.operations...)
}

func (s *blockingRenewSettler) complete(context.Context, string, int64, string) error {
	s.record("complete")
	close(s.completeCalled)
	return nil
}

func (s *blockingRenewSettler) abandon(context.Context, string, int64, string, int64) error {
	s.record("abandon")
	return nil
}

func (s *blockingRenewSettler) reject(context.Context, string, int64, string, string, string) error {
	return nil
}

func (s *blockingRenewSettler) deferMsg(context.Context, string, int64, string) error {
	return nil
}

func (s *blockingRenewSettler) renew(ctx context.Context, _ string, _ int64, _ string) error {
	close(s.renewStarted)
	<-ctx.Done()
	close(s.renewCanceled)
	<-s.allowRenewReturn
	s.record("renew returned")
	return ctx.Err()
}

func (s *fakeReceiveSource) receiveOne(ctx context.Context, queue string, max int, waitMs int64, mode receiveMode, attemptID string) ([]*Message, error) {
	s.mu.Lock()
	record := receiveRecord{max: max, waitMs: waitMs, mode: mode, attemptID: attemptID}
	s.receives = append(s.receives, record)
	call := len(s.receives)
	fn := s.receiveFn
	s.mu.Unlock()
	if fn == nil {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return fn(ctx, call, record)
}

func (s *fakeReceiveSource) recordSettle(record settleRecord) error {
	s.mu.Lock()
	s.settles = append(s.settles, record)
	err := s.settleErr
	settleFn := s.settleFn
	settleCall := len(s.settles)
	renewed := s.renewed
	var renewFn func(int) error
	var renewCall int
	if record.op == "renew" {
		s.renewCalls++
		renewCall = s.renewCalls
		renewFn = s.renewFn
	}
	s.mu.Unlock()
	if settleFn != nil {
		return settleFn(record, settleCall)
	}
	if record.op == "renew" && renewed != nil {
		select {
		case renewed <- struct{}{}:
		default:
		}
	}
	if renewFn != nil {
		return renewFn(renewCall)
	}
	return err
}

func (s *fakeReceiveSource) complete(ctx context.Context, queue string, seq int64, token string) error {
	return s.recordSettle(settleRecord{op: "complete", queue: queue, seq: seq, token: token})
}

func (s *fakeReceiveSource) abandon(ctx context.Context, queue string, seq int64, token string, delay int64) error {
	return s.recordSettle(settleRecord{op: "abandon", queue: queue, seq: seq, token: token, delay: delay})
}

func (s *fakeReceiveSource) reject(ctx context.Context, queue string, seq int64, token, reason, detail string) error {
	return s.recordSettle(settleRecord{op: "reject", queue: queue, seq: seq, token: token, reason: reason, detail: detail})
}

func (s *fakeReceiveSource) deferMsg(ctx context.Context, queue string, seq int64, token string) error {
	return s.recordSettle(settleRecord{op: "defer", queue: queue, seq: seq, token: token})
}

func (s *fakeReceiveSource) renew(ctx context.Context, queue string, seq int64, token string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.recordSettle(settleRecord{op: "renew", queue: queue, seq: seq, token: token})
}

func (s *fakeReceiveSource) message(seq int64) *Message {
	return &Message{
		SequenceNumber: seq,
		LockedUntil:    time.Now().Add(time.Minute),
		queue:          "jobs",
		lockToken:      fmt.Sprintf("token-%d", seq),
		s:              s,
	}
}

func (s *fakeReceiveSource) receiveSnapshot() []receiveRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]receiveRecord(nil), s.receives...)
}

func (s *fakeReceiveSource) settleSnapshot() []settleRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]settleRecord(nil), s.settles...)
}

func TestMessageSettlementKeepsDeliveryIdentity(t *testing.T) {
	source := &fakeReceiveSource{}
	message := &Message{SequenceNumber: 42, queue: "orders", lockToken: "fence", s: source}
	ctx := context.Background()

	if message.LockToken() != "fence" {
		t.Fatalf("LockToken = %q", message.LockToken())
	}
	if err := message.Complete(ctx); err != nil {
		t.Fatal(err)
	}
	if err := message.Abandon(ctx, AbandonOpts{Delay: 1500 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	if err := message.Reject(ctx, RejectOpts{Reason: "invalid", Detail: "bad payload"}); err != nil {
		t.Fatal(err)
	}
	if err := message.Defer(ctx); err != nil {
		t.Fatal(err)
	}
	if err := message.Renew(ctx); err != nil {
		t.Fatal(err)
	}

	got := source.settleSnapshot()
	if len(got) != 5 {
		t.Fatalf("settlements = %+v", got)
	}
	for _, record := range got {
		if record.queue != "orders" || record.seq != 42 || record.token != "fence" {
			t.Fatalf("identity changed: %+v", record)
		}
	}
	if got[1].op != "abandon" || got[1].delay != 1500 {
		t.Fatalf("abandon = %+v", got[1])
	}
	if got[2].op != "reject" || got[2].reason != "invalid" || got[2].detail != "bad payload" {
		t.Fatalf("reject = %+v", got[2])
	}
}

func TestReceiverRetryReusesAttemptIDAndCompletes(t *testing.T) {
	source := &fakeReceiveSource{}
	source.receiveFn = func(ctx context.Context, call int, record receiveRecord) ([]*Message, error) {
		if call == 1 {
			return nil, errors.New("temporary transport failure")
		}
		if call == 2 {
			return []*Message{source.message(1)}, nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	var observed int32
	receiver := newReceiver(source, "jobs", []ReceiverOption{WithErrorHandler(func(error) {
		atomic.AddInt32(&observed, 1)
	})})
	receiver.transientBackoff = time.Millisecond
	receiver.maxReceiveBackoff = 4 * time.Millisecond
	receiver.jitter = func(delay time.Duration) time.Duration { return delay }
	err := receiver.Run(ctx, func(context.Context, *Message) error {
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v", err)
	}
	receives := source.receiveSnapshot()
	if len(receives) != 2 {
		t.Fatalf("receive calls = %+v", receives)
	}
	if receives[0].attemptID == "" || receives[0].attemptID != receives[1].attemptID {
		t.Fatalf("attempt IDs = %q / %q", receives[0].attemptID, receives[1].attemptID)
	}
	if got := atomic.LoadInt32(&observed); got != 0 {
		t.Fatalf("error notifications = %d, want 0 after one recovered receive failure", got)
	}
	settles := source.settleSnapshot()
	if len(settles) != 1 || settles[0].op != "complete" {
		t.Fatalf("settlements = %+v", settles)
	}
}

func TestReceiverRetryKeepsAttemptIDAcrossBackoff(t *testing.T) {
	source := &fakeReceiveSource{}
	source.receiveFn = func(ctx context.Context, call int, record receiveRecord) ([]*Message, error) {
		if call <= 2 {
			return nil, errors.New("temporary transport failure")
		}
		if call == 3 {
			return []*Message{source.message(1)}, nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	var observed int32
	receiver := newReceiver(source, "jobs", []ReceiverOption{WithErrorHandler(func(error) {
		atomic.AddInt32(&observed, 1)
	})})
	receiver.transientBackoff = time.Millisecond
	receiver.maxReceiveBackoff = 4 * time.Millisecond
	receiver.jitter = func(delay time.Duration) time.Duration { return delay }
	err := receiver.Run(ctx, func(context.Context, *Message) error {
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v", err)
	}
	receives := source.receiveSnapshot()
	if len(receives) != 3 {
		t.Fatalf("receive calls = %+v", receives)
	}
	for i := 1; i < len(receives); i++ {
		if receives[i].attemptID != receives[0].attemptID {
			t.Fatalf("attempt IDs changed across retry %d: %+v", i, receives)
		}
	}
	if got := atomic.LoadInt32(&observed); got != 1 {
		t.Fatalf("error notifications = %d, want 1 after two consecutive receive failures", got)
	}
}

func TestReceiverStopsOnNonRetryableReceiveError(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{
			name: "redirect",
			err:  &HTTPError{StatusCode: http.StatusTemporaryRedirect, cause: errors.New("unexpected redirect")},
		},
		{
			name: "invalid argument",
			err: &HTTPError{StatusCode: http.StatusBadRequest,
				cause: fmt.Errorf("%w: invalid receive request", ErrInvalidArgument)},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source := &fakeReceiveSource{}
			retried := make(chan struct{})
			source.receiveFn = func(context.Context, int, receiveRecord) ([]*Message, error) {
				if len(source.receiveSnapshot()) == 2 {
					close(retried)
				}
				return nil, test.err
			}
			observed := make(chan error, 1)
			receiver := newReceiver(source, "jobs", []ReceiverOption{WithErrorHandler(func(err error) {
				observed <- err
			})})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			var handled int32
			go func() {
				done <- receiver.Run(ctx, func(context.Context, *Message) error {
					atomic.AddInt32(&handled, 1)
					return nil
				})
			}()

			var runErr error
			select {
			case runErr = <-done:
			case <-retried:
				cancel()
				runErr = <-done
			case <-time.After(2 * time.Second):
				cancel()
				<-done
				t.Fatal("Run did not return or retry the non-retryable receive error")
			}
			if runErr != test.err {
				t.Fatalf("Run error = %v, want original error %v", runErr, test.err)
			}
			if receives := source.receiveSnapshot(); len(receives) != 1 {
				t.Fatalf("receive calls = %+v, want exactly one", receives)
			}
			if got := atomic.LoadInt32(&handled); got != 0 {
				t.Fatalf("handler calls = %d, want 0", got)
			}
			select {
			case got := <-observed:
				if got != test.err {
					t.Fatalf("error notification = %v, want %v", got, test.err)
				}
			default:
				t.Fatal("non-retryable receive error was not reported")
			}
			select {
			case got := <-observed:
				t.Fatalf("extra error notification = %v", got)
			default:
			}
		})
	}
}

func TestReceiverDoesNotDispatchSuccessfulReceiveAfterCancellation(t *testing.T) {
	source := &fakeReceiveSource{}
	receiveStarted := make(chan struct{})
	source.receiveFn = func(ctx context.Context, _ int, _ receiveRecord) ([]*Message, error) {
		close(receiveStarted)
		<-ctx.Done()
		return []*Message{source.message(1)}, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	receiver := newReceiver(source, "jobs", nil)
	var handled int32
	done := make(chan error, 1)
	go func() {
		done <- receiver.Run(ctx, func(context.Context, *Message) error {
			atomic.AddInt32(&handled, 1)
			return nil
		})
	}()
	select {
	case <-receiveStarted:
	case <-time.After(2 * time.Second):
		cancel()
		<-done
		t.Fatal("Receive did not start")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run error = %v, want context canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not release canceled Receive reservations")
	}
	if got := atomic.LoadInt32(&handled); got != 0 {
		t.Fatalf("handler calls after canceled Receive = %d, want 0", got)
	}
	if settles := source.settleSnapshot(); len(settles) != 0 {
		t.Fatalf("settlements after canceled Receive = %+v, want none", settles)
	}
}

func TestReceiverDoesNotApplyReplayExpiryFilterToFirstSuccess(t *testing.T) {
	source := &fakeReceiveSource{}
	source.receiveFn = func(ctx context.Context, call int, record receiveRecord) ([]*Message, error) {
		if call == 1 {
			message := source.message(1)
			message.LockedUntil = time.Now().Add(5 * time.Millisecond)
			return []*Message{message}, nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	var handled int32
	receiver := newReceiver(source, "jobs", nil)
	err := receiver.Run(ctx, func(context.Context, *Message) error {
		atomic.AddInt32(&handled, 1)
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v, want context canceled", err)
	}
	if got := atomic.LoadInt32(&handled); got != 1 {
		t.Fatalf("near-expiry first-success handler calls = %d, want 1", got)
	}
	if receives := source.receiveSnapshot(); len(receives) != 1 {
		t.Fatalf("receive calls = %+v, want one successful first attempt", receives)
	}
	settles := source.settleSnapshot()
	if len(settles) != 1 || settles[0].op != "complete" || settles[0].seq != 1 {
		t.Fatalf("settlements = %+v, want first message completed", settles)
	}
}

func TestReceiverDiscardsZeroDeadlineOnFirstSuccess(t *testing.T) {
	source := &fakeReceiveSource{}
	source.receiveFn = func(ctx context.Context, call int, record receiveRecord) ([]*Message, error) {
		switch call {
		case 1:
			message := source.message(1)
			message.LockedUntil = time.Time{}
			return []*Message{message}, nil
		case 2:
			return []*Message{source.message(2)}, nil
		default:
			<-ctx.Done()
			return nil, ctx.Err()
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	var handled int64
	var observed int32
	receiver := newReceiver(source, "jobs", []ReceiverOption{WithErrorHandler(func(err error) {
		if !errors.Is(err, ErrLockLost) {
			t.Errorf("error notification = %v, want ErrLockLost", err)
		}
		atomic.AddInt32(&observed, 1)
	})})
	err := receiver.Run(ctx, func(_ context.Context, message *Message) error {
		atomic.StoreInt64(&handled, message.SequenceNumber)
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v, want context canceled", err)
	}
	if got := atomic.LoadInt64(&handled); got != 2 {
		t.Fatalf("handled sequence = %d, want fresh message 2", got)
	}
	receives := source.receiveSnapshot()
	if len(receives) != 2 || receives[0].attemptID == receives[1].attemptID {
		t.Fatalf("receive attempts = %+v, want a fresh ID after unusable first response", receives)
	}
	if got := atomic.LoadInt32(&observed); got != 1 {
		t.Fatalf("error notifications = %d, want one ErrLockLost", got)
	}
	settles := source.settleSnapshot()
	if len(settles) != 1 || settles[0].seq != 2 || settles[0].op != "complete" {
		t.Fatalf("settlements = %+v, want only fresh message completed", settles)
	}
}

func TestReceiverDoesNotDispatchRemainderWhenLeaseNotificationCancels(t *testing.T) {
	source := &fakeReceiveSource{}
	source.receiveFn = func(ctx context.Context, call int, record receiveRecord) ([]*Message, error) {
		if call == 1 {
			zero := source.message(1)
			zero.LockedUntil = time.Time{}
			return []*Message{zero, source.message(2)}, nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	var observed int32
	var handled int32
	receiver := newReceiver(source, "jobs", []ReceiverOption{
		WithConcurrency(2),
		WithPrefetch(2),
		WithErrorHandler(func(err error) {
			if !errors.Is(err, ErrLockLost) {
				t.Errorf("error notification = %v, want ErrLockLost", err)
			}
			atomic.AddInt32(&observed, 1)
			cancel()
		}),
	})
	err := receiver.Run(ctx, func(context.Context, *Message) error {
		atomic.AddInt32(&handled, 1)
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v, want context canceled", err)
	}
	if got := atomic.LoadInt32(&observed); got != 1 {
		t.Fatalf("error notifications = %d, want 1", got)
	}
	if got := atomic.LoadInt32(&handled); got != 0 {
		t.Fatalf("handler calls after notification canceled Run = %d, want 0", got)
	}
	if settles := source.settleSnapshot(); len(settles) != 0 {
		t.Fatalf("settlements after notification canceled Run = %+v, want none", settles)
	}
}

func TestReceiverDiscardsStaleReceiveReplayAndStartsFreshAttempt(t *testing.T) {
	tests := []struct {
		name     string
		deadline func() time.Time
	}{
		{name: "zero deadline", deadline: func() time.Time { return time.Time{} }},
		{name: "expired deadline", deadline: func() time.Time { return time.Now().Add(-time.Second) }},
		{name: "inside safety window", deadline: func() time.Time { return time.Now().Add(5 * time.Millisecond) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source := &fakeReceiveSource{}
			source.receiveFn = func(ctx context.Context, call int, record receiveRecord) ([]*Message, error) {
				switch call {
				case 1:
					return nil, errors.New("lost receive response")
				case 2:
					message := source.message(1)
					message.LockedUntil = test.deadline()
					return []*Message{message}, nil
				case 3:
					return []*Message{source.message(2)}, nil
				default:
					<-ctx.Done()
					return nil, ctx.Err()
				}
			}
			ctx, cancel := context.WithCancel(context.Background())
			var handled int64
			var observed int32
			receiver := newReceiver(source, "jobs", []ReceiverOption{WithErrorHandler(func(err error) {
				if !errors.Is(err, ErrLockLost) {
					t.Errorf("error notification = %v, want ErrLockLost", err)
				}
				atomic.AddInt32(&observed, 1)
			})})
			err := receiver.Run(ctx, func(_ context.Context, message *Message) error {
				atomic.StoreInt64(&handled, message.SequenceNumber)
				cancel()
				return nil
			})
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Run error = %v, want context canceled", err)
			}
			if got := atomic.LoadInt64(&handled); got != 2 {
				t.Fatalf("handled sequence = %d, want only fresh message 2", got)
			}
			receives := source.receiveSnapshot()
			if len(receives) != 3 {
				t.Fatalf("receive calls = %+v, want failed attempt, replay, then fresh attempt", receives)
			}
			if receives[0].attemptID == "" || receives[0].attemptID != receives[1].attemptID {
				t.Fatalf("replay attempt IDs = %q / %q, want equal non-empty IDs", receives[0].attemptID, receives[1].attemptID)
			}
			if receives[2].attemptID == receives[0].attemptID {
				t.Fatalf("fresh receive reused stale attempt ID %q", receives[2].attemptID)
			}
			if got := atomic.LoadInt32(&observed); got != 1 {
				t.Fatalf("error notifications = %d, want one stale replay notification", got)
			}
			settles := source.settleSnapshot()
			if len(settles) != 1 || settles[0].op != "complete" || settles[0].seq != 2 {
				t.Fatalf("settlements = %+v, want only fresh message completed", settles)
			}
		})
	}
}

func TestReceiverRetriesExactTerminalSettlement(t *testing.T) {
	tests := []struct {
		name       string
		handlerErr error
		wantOp     string
		firstErr   error
	}{
		{name: "complete transport", wantOp: "complete", firstErr: errors.New("connection reset")},
		{name: "abandon transport", handlerErr: errors.New("handler failed"), wantOp: "abandon", firstErr: errors.New("connection reset")},
		{name: "complete request deadline", wantOp: "complete", firstErr: context.DeadlineExceeded},
		{name: "abandon request deadline", handlerErr: errors.New("handler failed"), wantOp: "abandon", firstErr: context.DeadlineExceeded},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source := &fakeReceiveSource{}
			source.receiveFn = func(ctx context.Context, call int, record receiveRecord) ([]*Message, error) {
				if call == 1 {
					return []*Message{source.message(7)}, nil
				}
				<-ctx.Done()
				return nil, ctx.Err()
			}
			source.settleFn = func(record settleRecord, call int) error {
				if call == 1 {
					return test.firstErr
				}
				return nil
			}
			var observed int32
			ctx, cancel := context.WithCancel(context.Background())
			receiver := newReceiver(source, "jobs", []ReceiverOption{WithErrorHandler(func(error) {
				atomic.AddInt32(&observed, 1)
			})})
			receiver.settleBackoff = time.Millisecond
			receiver.maxSettleBackoff = 4 * time.Millisecond
			receiver.jitter = func(delay time.Duration) time.Duration { return delay }
			err := receiver.Run(ctx, func(context.Context, *Message) error {
				cancel()
				return test.handlerErr
			})
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Run error = %v", err)
			}
			settles := source.settleSnapshot()
			if len(settles) != 2 || settles[0] != settles[1] || settles[0].op != test.wantOp {
				t.Fatalf("settlement attempts = %+v, want two identical %s requests", settles, test.wantOp)
			}
			if got := atomic.LoadInt32(&observed); got != 0 {
				t.Fatalf("error notifications = %d, want 0 after recovered settlement", got)
			}
		})
	}
}

func TestReceiverDoesNotRetryTerminalSettlementError(t *testing.T) {
	source := &fakeReceiveSource{settleErr: ErrLockLost}
	message := source.message(8)
	var observed int32
	receiver := newReceiver(source, "jobs", []ReceiverOption{WithErrorHandler(func(err error) {
		if !errors.Is(err, ErrLockLost) {
			t.Errorf("error notification = %v, want ErrLockLost", err)
		}
		atomic.AddInt32(&observed, 1)
	})})
	receiver.process(context.Background(), message, func(context.Context, *Message) error {
		return nil
	}, func(err error) {
		t.Errorf("unexpected fatal receiver error: %v", err)
	})
	settles := source.settleSnapshot()
	if len(settles) != 1 || settles[0].op != "complete" {
		t.Fatalf("settlement attempts = %+v, want one Complete", settles)
	}
	if got := atomic.LoadInt32(&observed); got != 1 {
		t.Fatalf("error notifications = %d, want 1", got)
	}
}

func TestReceiverStopsOnPermanentReceiveError(t *testing.T) {
	source := &fakeReceiveSource{}
	source.receiveFn = func(context.Context, int, receiveRecord) ([]*Message, error) {
		return nil, fmt.Errorf("broker rejected credential: %w", ErrPermissionDenied)
	}
	var observed int32
	receiver := newReceiver(source, "jobs", []ReceiverOption{WithErrorHandler(func(error) {
		atomic.AddInt32(&observed, 1)
	})})
	err := receiver.Run(context.Background(), func(context.Context, *Message) error { return nil })
	if !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("Run error = %v", err)
	}
	if calls := len(source.receiveSnapshot()); calls != 1 {
		t.Fatalf("receive calls = %d", calls)
	}
	if atomic.LoadInt32(&observed) != 1 {
		t.Fatalf("error notifications = %d", observed)
	}
}

func TestReceiverPreservesPermanentErrorObservedAfterCancellation(t *testing.T) {
	source := &fakeReceiveSource{}
	source.receiveFn = func(ctx context.Context, _ int, _ receiveRecord) ([]*Message, error) {
		<-ctx.Done()
		return nil, ErrPermissionDenied
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	receiver := newReceiver(source, "jobs", nil)
	err := receiver.Run(ctx, func(context.Context, *Message) error { return nil })
	if !errors.Is(err, context.Canceled) {
		// Cancellation before Run reserves a worker prevents the receive from
		// starting; this is the normal fast-cancel path.
		t.Fatalf("Run error = %v, want context.Canceled", err)
	}

	started := make(chan struct{})
	source.receiveFn = func(ctx context.Context, _ int, _ receiveRecord) ([]*Message, error) {
		close(started)
		<-ctx.Done()
		return nil, ErrPermissionDenied
	}
	ctx, cancel = context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- receiver.Run(ctx, func(context.Context, *Message) error { return nil })
	}()
	<-started
	cancel()
	if err = <-done; !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("Run error = %v, want ErrPermissionDenied", err)
	}
}

func TestReceiverBoundsClaimsToIdleConcurrency(t *testing.T) {
	source := &fakeReceiveSource{}
	var first int32
	source.receiveFn = func(ctx context.Context, call int, record receiveRecord) ([]*Message, error) {
		if atomic.CompareAndSwapInt32(&first, 0, 1) {
			messages := make([]*Message, record.max)
			for i := range messages {
				messages[i] = source.message(int64(i + 1))
			}
			return messages, nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	receiver := newReceiver(source, "jobs", []ReceiverOption{WithConcurrency(3), WithPrefetch(20)})
	started := make(chan struct{}, 3)
	release := make(chan struct{})
	var active int32
	var maximum int32
	var finished int32
	done := make(chan error, 1)
	go func() {
		done <- receiver.Run(ctx, func(context.Context, *Message) error {
			current := atomic.AddInt32(&active, 1)
			for {
				previous := atomic.LoadInt32(&maximum)
				if current <= previous || atomic.CompareAndSwapInt32(&maximum, previous, current) {
					break
				}
			}
			started <- struct{}{}
			<-release
			atomic.AddInt32(&active, -1)
			if atomic.AddInt32(&finished, 1) == 3 {
				cancel()
			}
			return nil
		})
	}()

	for i := 0; i < 3; i++ {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatal("handlers did not start")
		}
	}
	receives := source.receiveSnapshot()
	if len(receives) != 1 || receives[0].max != 3 {
		t.Fatalf("receive calls = %+v", receives)
	}
	if atomic.LoadInt32(&maximum) != 3 {
		t.Fatalf("maximum concurrency = %d", maximum)
	}
	close(release)
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop")
	}
}

func TestReserveWorkerPairsRollsBackPartialReservation(t *testing.T) {
	t.Run("canceled blocking pair", func(t *testing.T) {
		handlerSlots := make(chan struct{}, 1)
		totalSlots := make(chan struct{})
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan int, 1)
		go func() {
			done <- reserveWorkerPairs(ctx, handlerSlots, totalSlots, 1)
		}()

		deadline := time.NewTimer(2 * time.Second)
		defer deadline.Stop()
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for len(handlerSlots) == 0 {
			select {
			case <-ticker.C:
			case <-deadline.C:
				cancel()
				<-done
				t.Fatal("reservation did not acquire its first token")
			}
		}
		cancel()
		if got := <-done; got != 0 {
			t.Fatalf("reserved pairs = %d, want 0 after cancellation", got)
		}
		if len(handlerSlots) != 0 || len(totalSlots) != 0 {
			t.Fatalf("partial reservation leaked: handler=%d total=%d", len(handlerSlots), len(totalSlots))
		}
	})

	t.Run("greedy second token unavailable", func(t *testing.T) {
		handlerSlots := make(chan struct{}, 2)
		totalSlots := make(chan struct{}, 1)
		got := reserveWorkerPairs(context.Background(), handlerSlots, totalSlots, 2)
		if got != 1 {
			t.Fatalf("reserved pairs = %d, want 1", got)
		}
		if len(handlerSlots) != 1 || len(totalSlots) != 1 {
			t.Fatalf("greedy reservation did not roll back: handler=%d total=%d", len(handlerSlots), len(totalSlots))
		}
		releaseWorkerPairs(handlerSlots, totalSlots, got)
		if len(handlerSlots) != 0 || len(totalSlots) != 0 {
			t.Fatalf("released reservations leaked: handler=%d total=%d", len(handlerSlots), len(totalSlots))
		}
	})
}

func TestReceiverOverlapsHandlersWithBoundedSettlementInflight(t *testing.T) {
	source := &fakeReceiveSource{}
	firstSettleStarted := make(chan struct{})
	secondSettleStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	releaseSecond := make(chan struct{})
	thirdReceive := make(chan struct{})
	thirdReceiveCanceled := make(chan struct{})
	var firstReleaseOnce sync.Once
	var secondReleaseOnce sync.Once
	var thirdReceiveOnce sync.Once

	source.receiveFn = func(ctx context.Context, call int, record receiveRecord) ([]*Message, error) {
		switch call {
		case 1:
			return []*Message{source.message(1)}, nil
		case 2:
			<-firstSettleStarted
			return []*Message{source.message(2)}, nil
		default:
			thirdReceiveOnce.Do(func() { close(thirdReceive) })
			<-ctx.Done()
			close(thirdReceiveCanceled)
			return nil, ctx.Err()
		}
	}
	source.settleFn = func(record settleRecord, _ int) error {
		if record.op != "complete" {
			return nil
		}
		switch record.seq {
		case 1:
			close(firstSettleStarted)
			<-releaseFirst
		case 2:
			close(secondSettleStarted)
			<-releaseSecond
		}
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	handled := make(chan int64, 2)
	receiver := newReceiver(source, "jobs", []ReceiverOption{WithConcurrency(1)})
	var runErr error
	runDone := make(chan struct{})
	go func() {
		runErr = receiver.Run(ctx, func(_ context.Context, message *Message) error {
			handled <- message.SequenceNumber
			return nil
		})
		close(runDone)
	}()
	defer func() {
		cancel()
		firstReleaseOnce.Do(func() { close(releaseFirst) })
		secondReleaseOnce.Do(func() { close(releaseSecond) })
		select {
		case <-runDone:
		case <-time.After(2 * time.Second):
			t.Error("Receiver.Run did not exit during test cleanup")
		}
	}()

	waitHandled := func(want int64) {
		t.Helper()
		select {
		case got := <-handled:
			if got != want {
				t.Fatalf("handled sequence = %d, want %d", got, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("handler %d did not start", want)
		}
	}
	waitHandled(1)
	select {
	case <-firstSettleStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("first settlement did not start")
	}
	// The first Complete remains blocked, but its handler slot is already free.
	waitHandled(2)
	select {
	case <-secondSettleStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("second settlement did not start")
	}
	if receives := source.receiveSnapshot(); len(receives) != 2 {
		t.Fatalf("receive calls while two settlements are in flight = %+v, want 2", receives)
	}
	select {
	case <-thirdReceive:
		t.Fatal("third Receive started while total in-flight capacity was full")
	case <-time.After(150 * time.Millisecond):
	}

	// Releasing one total-in-flight slot permits exactly the next claim wave.
	firstReleaseOnce.Do(func() { close(releaseFirst) })
	select {
	case <-thirdReceive:
	case <-time.After(2 * time.Second):
		t.Fatal("third Receive did not start after settlement released capacity")
	}
	cancel()
	select {
	case <-thirdReceiveCanceled:
	case <-time.After(2 * time.Second):
		t.Fatal("in-flight Receive did not observe cancellation")
	}
	select {
	case <-runDone:
		t.Fatal("Run returned before the remaining settlement finished")
	case <-time.After(25 * time.Millisecond):
	}
	secondReleaseOnce.Do(func() { close(releaseSecond) })
	select {
	case <-runDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not join settlement after it was released")
	}
	if !errors.Is(runErr, context.Canceled) {
		t.Fatalf("Run error = %v, want context canceled", runErr)
	}
	settles := source.settleSnapshot()
	if len(settles) != 2 || settles[0].seq != 1 || settles[1].seq != 2 {
		t.Fatalf("settlements = %+v, want completed messages 1 and 2", settles)
	}
}

func TestReceiverReleasesHandlerSlotWhenHandlerGoexits(t *testing.T) {
	source := &fakeReceiveSource{}
	source.receiveFn = func(ctx context.Context, call int, record receiveRecord) ([]*Message, error) {
		if call <= 2 {
			return []*Message{source.message(int64(call))}, nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	receiver := newReceiver(source, "jobs", []ReceiverOption{WithConcurrency(1)})
	firstStarted := make(chan struct{})
	secondStarted := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- receiver.Run(ctx, func(_ context.Context, message *Message) error {
			if message.SequenceNumber == 1 {
				close(firstStarted)
				runtime.Goexit()
			}
			close(secondStarted)
			cancel()
			return nil
		})
	}()
	select {
	case <-firstStarted:
	case <-time.After(2 * time.Second):
		cancel()
		<-done
		t.Fatal("first handler did not start")
	}
	select {
	case <-secondStarted:
	case <-time.After(2 * time.Second):
		cancel()
		<-done
		t.Fatal("handler Goexit leaked its reserved slot")
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run error = %v, want context canceled", err)
		}
	case <-time.After(2 * time.Second):
		cancel()
		t.Fatal("Run did not join after handler Goexit")
	}
	settles := source.settleSnapshot()
	if len(settles) != 1 || settles[0].seq != 2 || settles[0].op != "complete" {
		t.Fatalf("settlements = %+v, want only second message completed", settles)
	}
}

func TestReceiverGoexitJoinsAutoRenewBeforeReleasingPipelineSlots(t *testing.T) {
	source := &fakeReceiveSource{}
	renewStarted := make(chan struct{})
	renewCanceled := make(chan struct{})
	allowRenewReturn := make(chan struct{})
	var renewOnce sync.Once
	leaseSource := &deadlineLeaseSource{
		fakeReceiveSource: source,
		renewLeaseFn: func(ctx context.Context, _ *Message) (time.Time, error) {
			renewOnce.Do(func() { close(renewStarted) })
			<-ctx.Done()
			close(renewCanceled)
			<-allowRenewReturn
			return time.Time{}, ctx.Err()
		},
	}
	source.receiveFn = func(ctx context.Context, call int, _ receiveRecord) ([]*Message, error) {
		switch call {
		case 1:
			message := source.message(1)
			message.LockedUntil = time.Now().Add(40 * time.Millisecond)
			return []*Message{message}, nil
		case 2:
			message := source.message(2)
			message.LockedUntil = time.Now().Add(time.Hour)
			return []*Message{message}, nil
		default:
			<-ctx.Done()
			return nil, ctx.Err()
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	receiver := newReceiver(leaseSource, "jobs", []ReceiverOption{WithConcurrency(1), WithAutoRenew()})
	secondStarted := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- receiver.Run(ctx, func(_ context.Context, message *Message) error {
			if message.SequenceNumber == 1 {
				select {
				case <-renewStarted:
				case <-time.After(2 * time.Second):
					cancel()
					return errors.New("renew did not start")
				}
				runtime.Goexit()
			}
			close(secondStarted)
			cancel()
			return nil
		})
	}()
	select {
	case <-renewCanceled:
	case <-time.After(2 * time.Second):
		cancel()
		close(allowRenewReturn)
		<-done
		t.Fatal("handler Goexit did not cancel auto-renew")
	}
	select {
	case <-secondStarted:
		cancel()
		close(allowRenewReturn)
		<-done
		t.Fatal("handler Goexit released pipeline capacity before auto-renew joined")
	case <-time.After(100 * time.Millisecond):
	}
	close(allowRenewReturn)
	select {
	case <-secondStarted:
	case <-time.After(2 * time.Second):
		cancel()
		<-done
		t.Fatal("pipeline did not continue after auto-renew exited")
	}
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v, want context.Canceled", err)
	}
	settles := source.settleSnapshot()
	if len(settles) != 1 || settles[0].seq != 2 || settles[0].op != "complete" {
		t.Fatalf("settlements = %+v, want only second message completed", settles)
	}
}

func TestReceiverAutoRenewsAndAbandonsHandlerFailure(t *testing.T) {
	source := &fakeReceiveSource{renewed: make(chan struct{}, 1)}
	var returned int32
	source.receiveFn = func(ctx context.Context, call int, record receiveRecord) ([]*Message, error) {
		if atomic.CompareAndSwapInt32(&returned, 0, 1) {
			message := source.message(9)
			message.LockedUntil = time.Now().Add(20 * time.Millisecond)
			return []*Message{message}, nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	receiver := newReceiver(source, "jobs", []ReceiverOption{WithAutoRenew()})
	err := receiver.Run(ctx, func(context.Context, *Message) error {
		select {
		case <-source.renewed:
		case <-time.After(time.Second):
			t.Error("message was not renewed")
		}
		cancel()
		return errors.New("handler failed")
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v", err)
	}
	settles := source.settleSnapshot()
	var renews, abandons, completes int
	for _, record := range settles {
		switch record.op {
		case "renew":
			renews++
		case "abandon":
			abandons++
		case "complete":
			completes++
		}
	}
	if renews == 0 || abandons != 1 || completes != 0 {
		t.Fatalf("settlements = %+v", settles)
	}
	if settles[len(settles)-1].op != "abandon" {
		t.Fatalf("renewal overlapped terminal settlement: %+v", settles)
	}
}

func TestReceiverGatesUnsafeInitialLeaseBeforeHandler(t *testing.T) {
	t.Run("confirmed renewal releases dispatch", func(t *testing.T) {
		base := &fakeReceiveSource{}
		base.receiveFn = func(ctx context.Context, call int, _ receiveRecord) ([]*Message, error) {
			if call == 1 {
				message := base.message(41)
				message.LockedUntil = time.Now().Add(leaseDeadlineSafety / 2)
				return []*Message{message}, nil
			}
			<-ctx.Done()
			return nil, ctx.Err()
		}
		renewStarted := make(chan struct{})
		allowRenew := make(chan struct{})
		var releaseOnce sync.Once
		releaseRenew := func() { releaseOnce.Do(func() { close(allowRenew) }) }
		defer releaseRenew()
		var renewCalls int32
		source := &deadlineLeaseSource{fakeReceiveSource: base}
		source.renewLeaseFn = func(context.Context, *Message) (time.Time, error) {
			if atomic.AddInt32(&renewCalls, 1) == 1 {
				close(renewStarted)
			}
			<-allowRenew
			return time.Now().Add(time.Minute), nil
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		handlerStarted := make(chan struct{})
		runDone := make(chan error, 1)
		go func() {
			runDone <- newReceiver(source, "jobs", []ReceiverOption{WithAutoRenew()}).Run(ctx, func(context.Context, *Message) error {
				close(handlerStarted)
				cancel()
				return nil
			})
		}()
		select {
		case <-renewStarted:
		case <-time.After(time.Second):
			releaseRenew()
			cancel()
			<-runDone
			t.Fatal("initial renewal did not start")
		}
		select {
		case <-handlerStarted:
			releaseRenew()
			cancel()
			<-runDone
			t.Fatal("handler started before the initial lease was confirmed")
		case <-time.After(100 * time.Millisecond):
		}
		releaseRenew()
		select {
		case <-handlerStarted:
		case <-time.After(time.Second):
			cancel()
			<-runDone
			t.Fatal("handler did not start after the initial lease was confirmed")
		}
		if err := <-runDone; !errors.Is(err, context.Canceled) {
			t.Fatalf("Run error = %v, want context.Canceled", err)
		}
		if got := atomic.LoadInt32(&renewCalls); got != 1 {
			t.Fatalf("renew calls = %d, want 1", got)
		}
		settles := base.settleSnapshot()
		if len(settles) != 1 || settles[0].op != "complete" || settles[0].seq != 41 {
			t.Fatalf("settlements = %+v, want confirmed message completed", settles)
		}
	})

	t.Run("failed renewal never dispatches", func(t *testing.T) {
		base := &fakeReceiveSource{}
		base.receiveFn = func(ctx context.Context, call int, _ receiveRecord) ([]*Message, error) {
			if call == 1 {
				message := base.message(42)
				message.LockedUntil = time.Now().Add(-time.Millisecond)
				return []*Message{message}, nil
			}
			<-ctx.Done()
			return nil, ctx.Err()
		}
		renewStarted := make(chan struct{})
		allowRenew := make(chan struct{})
		var releaseOnce sync.Once
		releaseRenew := func() { releaseOnce.Do(func() { close(allowRenew) }) }
		defer releaseRenew()
		var renewCalls int32
		source := &deadlineLeaseSource{fakeReceiveSource: base}
		source.renewLeaseFn = func(context.Context, *Message) (time.Time, error) {
			atomic.AddInt32(&renewCalls, 1)
			close(renewStarted)
			<-allowRenew
			return time.Time{}, ErrLockLost
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		var observed int32
		var handlerCalls int32
		runDone := make(chan error, 1)
		receiver := newReceiver(source, "jobs", []ReceiverOption{
			WithAutoRenew(),
			WithErrorHandler(func(err error) {
				if !errors.Is(err, ErrLockLost) {
					t.Errorf("error notification = %v, want ErrLockLost", err)
				}
				atomic.AddInt32(&observed, 1)
				cancel()
			}),
		})
		go func() {
			runDone <- receiver.Run(ctx, func(context.Context, *Message) error {
				atomic.AddInt32(&handlerCalls, 1)
				return nil
			})
		}()
		select {
		case <-renewStarted:
		case <-time.After(time.Second):
			releaseRenew()
			cancel()
			<-runDone
			t.Fatal("initial renewal did not start")
		}
		if got := atomic.LoadInt32(&handlerCalls); got != 0 {
			t.Fatalf("handler calls before renewal result = %d, want 0", got)
		}
		releaseRenew()
		if err := <-runDone; !errors.Is(err, context.Canceled) {
			t.Fatalf("Run error = %v, want context.Canceled", err)
		}
		if got := atomic.LoadInt32(&renewCalls); got != 1 {
			t.Fatalf("renew calls = %d, want 1", got)
		}
		if got := atomic.LoadInt32(&observed); got != 1 {
			t.Fatalf("error notifications = %d, want 1", got)
		}
		if got := atomic.LoadInt32(&handlerCalls); got != 0 {
			t.Fatalf("handler calls = %d, want 0 after lost lease", got)
		}
		if settles := base.settleSnapshot(); len(settles) != 0 {
			t.Fatalf("settlements = %+v, want none after lost lease", settles)
		}
	})

	t.Run("cancellation while confirming never dispatches", func(t *testing.T) {
		base := &fakeReceiveSource{}
		base.receiveFn = func(ctx context.Context, call int, _ receiveRecord) ([]*Message, error) {
			if call == 1 {
				message := base.message(44)
				message.LockedUntil = time.Now().Add(-time.Millisecond)
				return []*Message{message}, nil
			}
			<-ctx.Done()
			return nil, ctx.Err()
		}
		renewStarted := make(chan struct{})
		allowRenew := make(chan struct{})
		source := &deadlineLeaseSource{fakeReceiveSource: base}
		source.renewLeaseFn = func(context.Context, *Message) (time.Time, error) {
			close(renewStarted)
			<-allowRenew
			// Model a response that raced cancellation and still decoded as success.
			return time.Now().Add(time.Minute), nil
		}
		ctx, cancel := context.WithCancel(context.Background())
		var handlerCalls int32
		runDone := make(chan error, 1)
		go func() {
			runDone <- newReceiver(source, "jobs", []ReceiverOption{WithAutoRenew()}).Run(ctx, func(context.Context, *Message) error {
				atomic.AddInt32(&handlerCalls, 1)
				return nil
			})
		}()
		select {
		case <-renewStarted:
		case <-time.After(time.Second):
			cancel()
			close(allowRenew)
			<-runDone
			t.Fatal("initial renewal did not start")
		}
		cancel()
		close(allowRenew)
		if err := <-runDone; !errors.Is(err, context.Canceled) {
			t.Fatalf("Run error = %v, want context.Canceled", err)
		}
		if got := atomic.LoadInt32(&handlerCalls); got != 0 {
			t.Fatalf("handler calls = %d, want 0 after cancellation", got)
		}
		if settles := base.settleSnapshot(); len(settles) != 0 {
			t.Fatalf("settlements = %+v, want none after cancellation", settles)
		}
	})

	t.Run("legacy renewal without a deadline releases dispatch", func(t *testing.T) {
		source := &fakeReceiveSource{}
		source.receiveFn = func(ctx context.Context, call int, _ receiveRecord) ([]*Message, error) {
			if call == 1 {
				message := source.message(45)
				message.LockedUntil = time.Now().Add(-time.Millisecond)
				return []*Message{message}, nil
			}
			<-ctx.Done()
			return nil, ctx.Err()
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		var observed int32
		var handlerCalls int32
		err := newReceiver(source, "jobs", []ReceiverOption{
			WithAutoRenew(),
			WithErrorHandler(func(error) { atomic.AddInt32(&observed, 1) }),
		}).Run(ctx, func(context.Context, *Message) error {
			atomic.AddInt32(&handlerCalls, 1)
			cancel()
			return nil
		})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run error = %v, want context.Canceled", err)
		}
		if got := atomic.LoadInt32(&observed); got != 0 {
			t.Fatalf("error notifications = %d, want 0", got)
		}
		if got := atomic.LoadInt32(&handlerCalls); got != 1 {
			t.Fatalf("handler calls = %d, want 1 after confirmed legacy Renew", got)
		}
		settles := source.settleSnapshot()
		if len(settles) != 2 || settles[0].op != "renew" || settles[1].op != "complete" {
			t.Fatalf("operations = %+v, want legacy Renew then Complete", settles)
		}
	})

	t.Run("usable lease starts without renewal", func(t *testing.T) {
		base := &fakeReceiveSource{}
		base.receiveFn = func(ctx context.Context, call int, _ receiveRecord) ([]*Message, error) {
			if call == 1 {
				message := base.message(43)
				message.LockedUntil = time.Now().Add(time.Hour)
				return []*Message{message}, nil
			}
			<-ctx.Done()
			return nil, ctx.Err()
		}
		var renewCalls int32
		source := &deadlineLeaseSource{fakeReceiveSource: base}
		source.renewLeaseFn = func(context.Context, *Message) (time.Time, error) {
			atomic.AddInt32(&renewCalls, 1)
			return time.Now().Add(time.Hour), nil
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		err := newReceiver(source, "jobs", []ReceiverOption{WithAutoRenew()}).Run(ctx, func(context.Context, *Message) error {
			cancel()
			return nil
		})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run error = %v, want context.Canceled", err)
		}
		if got := atomic.LoadInt32(&renewCalls); got != 0 {
			t.Fatalf("renew calls = %d, want no dispatch-gate request for a usable lease", got)
		}
	})
}

func TestReceiverAutoRenewUsesConfirmedLeaseWhenClientClockIsAhead(t *testing.T) {
	tests := []struct {
		name         string
		lostResponse bool
		wantReceives int
	}{
		{name: "first response", wantReceives: 1},
		{name: "replayed response", lostResponse: true, wantReceives: 2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			base := &fakeReceiveSource{}
			base.receiveFn = func(ctx context.Context, call int, _ receiveRecord) ([]*Message, error) {
				if test.lostResponse && call == 1 {
					return nil, errors.New("lost receive response")
				}
				if call == test.wantReceives {
					message := base.message(46)
					// Model a client clock two seconds ahead of a broker whose
					// lock duration is one second.
					message.LockedUntil = time.Now().Add(-time.Second)
					return []*Message{message}, nil
				}
				<-ctx.Done()
				return nil, ctx.Err()
			}
			var renewCalls int32
			renewedAfterDispatch := make(chan struct{})
			source := &deadlineLeaseSource{fakeReceiveSource: base}
			source.renewLeaseFn = func(context.Context, *Message) (time.Time, error) {
				if atomic.AddInt32(&renewCalls, 1) == 2 {
					close(renewedAfterDispatch)
				}
				// Renewal committed at the broker, but its absolute deadline
				// remains in the past according to the client's fast clock.
				return time.Now().Add(-time.Second), nil
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			var handlerCalls int32
			var observed int32
			receiver := newReceiver(source, "jobs", []ReceiverOption{
				WithAutoRenew(),
				WithErrorHandler(func(error) {
					atomic.AddInt32(&observed, 1)
					cancel()
				}),
			})
			err := receiver.Run(ctx, func(context.Context, *Message) error {
				atomic.AddInt32(&handlerCalls, 1)
				select {
				case <-renewedAfterDispatch:
				case <-time.After(time.Second):
					t.Error("confirmed skewed lease was not renewed again")
				}
				cancel()
				return nil
			})
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Run error = %v, want context.Canceled", err)
			}
			if got := atomic.LoadInt32(&handlerCalls); got != 1 {
				t.Fatalf("handler calls = %d, want 1", got)
			}
			if got := atomic.LoadInt32(&renewCalls); got < 2 {
				t.Fatalf("renew calls = %d, want initial confirmation plus one managed renewal", got)
			}
			if got := atomic.LoadInt32(&observed); got != 0 {
				t.Fatalf("error notifications = %d, want 0", got)
			}
			if got := len(base.receiveSnapshot()); got != test.wantReceives {
				t.Fatalf("receive calls = %d, want %d", got, test.wantReceives)
			}
			settles := base.settleSnapshot()
			if len(settles) != 1 || settles[0].op != "complete" || settles[0].seq != 46 {
				t.Fatalf("settlements = %+v, want confirmed message completed", settles)
			}
		})
	}
}

func TestReceiverAutoRenewFencesEveryAttemptReplay(t *testing.T) {
	tests := []struct {
		name        string
		renewErr    error
		wantHandler int32
		wantErrors  int32
	}{
		{name: "renew success", wantHandler: 1},
		{name: "lock lost", renewErr: ErrLockLost, wantErrors: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			base := &fakeReceiveSource{}
			base.receiveFn = func(ctx context.Context, call int, _ receiveRecord) ([]*Message, error) {
				switch call {
				case 1:
					return nil, errors.New("lost receive response")
				case 2:
					message := base.message(48)
					// A slow client clock can make a lease that is stale at the
					// broker still appear comfortably usable locally.
					message.LockedUntil = time.Now().Add(time.Hour)
					return []*Message{message}, nil
				default:
					<-ctx.Done()
					return nil, ctx.Err()
				}
			}
			var renewCalls int32
			source := &deadlineLeaseSource{fakeReceiveSource: base}
			source.renewLeaseFn = func(context.Context, *Message) (time.Time, error) {
				atomic.AddInt32(&renewCalls, 1)
				if test.renewErr != nil {
					return time.Time{}, test.renewErr
				}
				return time.Now().Add(time.Minute), nil
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			var handlerCalls int32
			var observed int32
			receiver := newReceiver(source, "jobs", []ReceiverOption{
				WithAutoRenew(),
				WithErrorHandler(func(err error) {
					if !errors.Is(err, ErrLockLost) {
						t.Errorf("error notification = %v, want ErrLockLost", err)
					}
					atomic.AddInt32(&observed, 1)
					cancel()
				}),
			})
			err := receiver.Run(ctx, func(context.Context, *Message) error {
				atomic.AddInt32(&handlerCalls, 1)
				cancel()
				return nil
			})
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Run error = %v, want context.Canceled", err)
			}
			if got := atomic.LoadInt32(&renewCalls); got != 1 {
				t.Fatalf("renew calls = %d, want one replay fence", got)
			}
			if got := atomic.LoadInt32(&handlerCalls); got != test.wantHandler {
				t.Fatalf("handler calls = %d, want %d", got, test.wantHandler)
			}
			if got := atomic.LoadInt32(&observed); got != test.wantErrors {
				t.Fatalf("error notifications = %d, want %d", got, test.wantErrors)
			}
			receives := base.receiveSnapshot()
			if len(receives) != 2 || receives[0].attemptID == "" || receives[0].attemptID != receives[1].attemptID {
				t.Fatalf("receive attempts = %+v, want one exact replay", receives)
			}
			settles := base.settleSnapshot()
			if test.wantHandler == 1 {
				if len(settles) != 1 || settles[0].op != "complete" || settles[0].seq != 48 {
					t.Fatalf("settlements = %+v, want replayed message completed", settles)
				}
			} else if len(settles) != 0 {
				t.Fatalf("settlements = %+v, want none after replay fence failed", settles)
			}
		})
	}
}

func TestReceiverAutoRenewReplayKeepsLegacyDeadlineUntrusted(t *testing.T) {
	base := &fakeReceiveSource{}
	base.receiveFn = func(ctx context.Context, call int, _ receiveRecord) ([]*Message, error) {
		switch call {
		case 1:
			return nil, errors.New("lost receive response")
		case 2:
			message := base.message(51)
			// A slow client clock makes the replayed broker deadline look far
			// enough away that it must not become a legacy lease estimate.
			message.LockedUntil = time.Now().Add(time.Hour)
			return []*Message{message}, nil
		default:
			<-ctx.Done()
			return nil, ctx.Err()
		}
	}
	thirdRenew := make(chan struct{})
	base.renewFn = func(call int) error {
		if call == 3 {
			close(thirdRenew)
		}
		return nil
	}
	var deadlineRenewCalls int32
	source := &deadlineLeaseSource{fakeReceiveSource: base}
	source.renewLeaseFn = func(context.Context, *Message) (time.Time, error) {
		atomic.AddInt32(&deadlineRenewCalls, 1)
		return time.Time{}, ErrUnsupported
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var handlerCalls int32
	var observed int32
	receiver := newReceiver(source, "jobs", []ReceiverOption{
		WithAutoRenew(),
		WithErrorHandler(func(error) {
			atomic.AddInt32(&observed, 1)
			cancel()
		}),
	})
	err := receiver.Run(ctx, func(context.Context, *Message) error {
		atomic.AddInt32(&handlerCalls, 1)
		select {
		case <-thirdRenew:
		case <-time.After(time.Second):
			t.Error("legacy replay inherited the untrusted future deadline")
		}
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v, want context.Canceled", err)
	}
	if got := atomic.LoadInt32(&deadlineRenewCalls); got != 1 {
		t.Fatalf("deadline-bearing renew calls = %d, want one capability probe", got)
	}
	if got := atomic.LoadInt32(&handlerCalls); got != 1 {
		t.Fatalf("handler calls = %d, want 1", got)
	}
	if got := atomic.LoadInt32(&observed); got != 0 {
		t.Fatalf("error notifications = %d, want 0", got)
	}
	settles := base.settleSnapshot()
	var renews, completes int
	for _, record := range settles {
		switch record.op {
		case "renew":
			renews++
		case "complete":
			completes++
		}
	}
	if renews != 3 || completes != 1 {
		t.Fatalf("settlements = %+v, want three legacy Renews then Complete", settles)
	}
}

func TestReceiverFailsClosedWhenSkewedLeaseRenewalFails(t *testing.T) {
	base := &fakeReceiveSource{}
	message := base.message(47)
	message.LockedUntil = time.Now().Add(-time.Second)
	var renewCalls int32
	source := &deadlineLeaseSource{fakeReceiveSource: base}
	source.renewLeaseFn = func(context.Context, *Message) (time.Time, error) {
		if atomic.AddInt32(&renewCalls, 1) == 1 {
			// Initial confirmation commits, but the fast client clock cannot use
			// the broker's absolute deadline as local retry headroom.
			return time.Now().Add(-time.Second), nil
		}
		return time.Time{}, errors.New("temporary renewal failure")
	}
	var handlerCalls int32
	var observed []error
	receiver := newReceiver(source, "jobs", []ReceiverOption{
		WithAutoRenew(),
		WithErrorHandler(func(err error) { observed = append(observed, err) }),
	})
	receiver.process(context.Background(), message, func(ctx context.Context, _ *Message) error {
		atomic.AddInt32(&handlerCalls, 1)
		<-ctx.Done()
		return ctx.Err()
	}, func(error) { t.Error("transient skew renewal failure must not stop the receiver") })

	if got := atomic.LoadInt32(&handlerCalls); got != 1 {
		t.Fatalf("handler calls = %d, want 1", got)
	}
	if got := atomic.LoadInt32(&renewCalls); got != 2 {
		t.Fatalf("renew calls = %d, want confirmation then one failed renewal", got)
	}
	if len(observed) != 1 || !errors.Is(observed[0], ErrLockLost) {
		t.Fatalf("error notifications = %v, want one ErrLockLost", observed)
	}
	if settles := base.settleSnapshot(); len(settles) != 0 {
		t.Fatalf("settlements = %+v, want none after untrusted lease renewal failed", settles)
	}
}

func TestReceiverCancelsHangingRenewAtTrustedLeaseDeadline(t *testing.T) {
	base := &fakeReceiveSource{}
	message := base.message(49)
	message.LockedUntil = time.Now().Add(250 * time.Millisecond)
	renewStarted := make(chan struct{})
	renewCanceled := make(chan struct{})
	var renewCalls int32
	source := &deadlineLeaseSource{fakeReceiveSource: base}
	source.renewLeaseFn = func(ctx context.Context, _ *Message) (time.Time, error) {
		atomic.AddInt32(&renewCalls, 1)
		close(renewStarted)
		<-ctx.Done()
		close(renewCanceled)
		return time.Time{}, ctx.Err()
	}
	observed := make(chan error, 1)
	receiver := newReceiver(source, "jobs", []ReceiverOption{
		WithAutoRenew(),
		WithErrorHandler(func(err error) { observed <- err }),
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	handlerCanceled := make(chan struct{})
	processDone := make(chan struct{})
	go func() {
		receiver.process(ctx, message, func(handlerCtx context.Context, _ *Message) error {
			<-handlerCtx.Done()
			close(handlerCanceled)
			return handlerCtx.Err()
		}, func(error) { t.Error("hanging renewal must not stop the receiver") })
		close(processDone)
	}()

	select {
	case <-renewStarted:
	case <-time.After(time.Second):
		cancel()
		<-processDone
		t.Fatal("trusted renewal did not start")
	}
	select {
	case <-processDone:
	case <-time.After(time.Second):
		cancel()
		<-processDone
		t.Fatal("trusted hanging renewal outlived its lease deadline")
	}
	select {
	case <-renewCanceled:
	default:
		t.Fatal("trusted hanging renewal context was not canceled")
	}
	select {
	case <-handlerCanceled:
	default:
		t.Fatal("trusted hanging renewal did not cancel the handler")
	}
	if got := atomic.LoadInt32(&renewCalls); got != 1 {
		t.Fatalf("renew calls = %d, want 1", got)
	}
	select {
	case err := <-observed:
		if !errors.Is(err, ErrLockLost) {
			t.Fatalf("error notification = %v, want ErrLockLost", err)
		}
	default:
		t.Fatal("trusted hanging renewal did not report lock loss")
	}
	if settles := base.settleSnapshot(); len(settles) != 0 {
		t.Fatalf("settlements = %+v, want none after hanging renewal", settles)
	}
}

func TestReceiverCancelsHangingRenewAtUntrustedProbeTimeout(t *testing.T) {
	base := &fakeReceiveSource{}
	message := base.message(50)
	message.LockedUntil = time.Now().Add(-time.Second)
	renewStarted := make(chan struct{})
	renewCanceled := make(chan struct{})
	var renewCalls int32
	source := &deadlineLeaseSource{fakeReceiveSource: base}
	source.renewLeaseFn = func(ctx context.Context, _ *Message) (time.Time, error) {
		if atomic.AddInt32(&renewCalls, 1) == 1 {
			// The synchronous dispatch gate succeeds but cannot provide a locally
			// trustworthy deadline under persistent clock skew.
			return time.Now().Add(-time.Second), nil
		}
		close(renewStarted)
		<-ctx.Done()
		close(renewCanceled)
		return time.Time{}, ctx.Err()
	}
	observed := make(chan error, 1)
	receiver := newReceiver(source, "jobs", []ReceiverOption{
		WithAutoRenew(),
		WithErrorHandler(func(err error) { observed <- err }),
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	handlerCanceled := make(chan struct{})
	processDone := make(chan struct{})
	go func() {
		receiver.process(ctx, message, func(handlerCtx context.Context, _ *Message) error {
			<-handlerCtx.Done()
			close(handlerCanceled)
			return handlerCtx.Err()
		}, func(error) { t.Error("hanging renewal must not stop the receiver") })
		close(processDone)
	}()

	select {
	case <-renewStarted:
	case <-time.After(time.Second):
		cancel()
		<-processDone
		t.Fatal("untrusted renewal probe did not start")
	}
	select {
	case <-processDone:
	case <-time.After(time.Second):
		cancel()
		<-processDone
		t.Fatal("untrusted hanging renewal outlived its probe timeout")
	}
	select {
	case <-renewCanceled:
	default:
		t.Fatal("untrusted hanging renewal context was not canceled")
	}
	select {
	case <-handlerCanceled:
	default:
		t.Fatal("untrusted hanging renewal did not cancel the handler")
	}
	if got := atomic.LoadInt32(&renewCalls); got != 2 {
		t.Fatalf("renew calls = %d, want dispatch confirmation then one probe", got)
	}
	select {
	case err := <-observed:
		if !errors.Is(err, ErrLockLost) {
			t.Fatalf("error notification = %v, want ErrLockLost", err)
		}
	default:
		t.Fatal("untrusted hanging renewal did not report lock loss")
	}
	if settles := base.settleSnapshot(); len(settles) != 0 {
		t.Fatalf("settlements = %+v, want none after hanging renewal", settles)
	}
}

func TestAutoRenewIntervalStaysInsideShortLease(t *testing.T) {
	tests := []struct {
		name      string
		remaining time.Duration
		want      time.Duration
	}{
		{name: "one second", remaining: time.Second, want: time.Second / 3},
		{name: "spin guard", remaining: 120 * time.Millisecond, want: 50 * time.Millisecond},
		{name: "twenty milliseconds", remaining: 20 * time.Millisecond, want: 10 * time.Millisecond},
		{name: "long lease", remaining: time.Minute, want: 20 * time.Second},
		{name: "smallest positive", remaining: time.Nanosecond, want: time.Nanosecond},
		{name: "expired", remaining: 0, want: 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := autoRenewInterval(test.remaining); got != test.want {
				t.Fatalf("autoRenewInterval(%s) = %s, want %s", test.remaining, got, test.want)
			}
		})
	}
}

func TestRenewRetryClassification(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "transport", err: errors.New("connection reset"), want: true},
		{name: "ambiguous outcome", err: ErrOutcomeUnknown, want: true},
		{
			name: "invalid renewal acknowledgement wrapped as ambiguous",
			err: &ambiguousOutcomeError{
				operation: "Renew",
				cause:     &invalidSuccessResponseError{cause: errors.New("missing ok")},
			},
			want: true,
		},
		{name: "HTTP 503", err: &HTTPError{StatusCode: 503, cause: errors.New("unavailable")}, want: true},
		{name: "HTTP 503 overrides permission code", err: &HTTPError{StatusCode: 503, cause: ErrPermissionDenied}, want: true},
		{name: "HTTP 429", err: &HTTPError{StatusCode: 429, cause: errors.New("throttled")}, want: true},
		{name: "HTTP 400 overrides internal code", err: &HTTPError{StatusCode: 400, cause: errors.New("internal")}},
		{name: "lock lost", err: ErrLockLost},
		{name: "invalid argument", err: ErrInvalidArgument},
		{name: "permission denied", err: ErrPermissionDenied},
		{name: "unsupported", err: ErrUnsupported},
		{name: "request deadline", err: context.DeadlineExceeded, want: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := retryableRenewError(test.err); got != test.want {
				t.Fatalf("retryableRenewError(%v) = %v, want %v", test.err, got, test.want)
			}
		})
	}
}

func TestSettlementExhaustionPreservesHTTPStatusClassification(t *testing.T) {
	receiver := newReceiver(nil, "jobs", nil)
	tests := []struct {
		name      string
		err       error
		wantFatal bool
	}{
		{name: "503 overrides permission body", err: &HTTPError{StatusCode: 503, cause: ErrPermissionDenied}},
		{name: "403 permission is fatal", err: &HTTPError{StatusCode: 403, cause: ErrPermissionDenied}, wantFatal: true},
		{name: "local permission is fatal", err: ErrPermissionDenied, wantFatal: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fatal := false
			receiver.settleError(test.err, func(error) { fatal = true })
			if fatal != test.wantFatal {
				t.Fatalf("fatal = %v, want %v for %v", fatal, test.wantFatal, test.err)
			}
		})
	}
}

func TestRetryBackoffIsExponentialBoundedAndJittered(t *testing.T) {
	wants := []time.Duration{
		500 * time.Millisecond,
		time.Second,
		2 * time.Second,
		4 * time.Second,
		8 * time.Second,
		10 * time.Second,
		10 * time.Second,
	}
	for failures, want := range wants {
		if got := exponentialBackoff(500*time.Millisecond, 10*time.Second, failures); got != want {
			t.Fatalf("backoff after %d failures = %s, want %s", failures, got, want)
		}
	}
	for i := 0; i < 100; i++ {
		got := jitterBackoff(time.Second)
		if got < 500*time.Millisecond || got > time.Second {
			t.Fatalf("jittered backoff = %s, want [500ms, 1s]", got)
		}
	}
}

func TestReceiverRetriesOneRequestDeadlineRenewFailure(t *testing.T) {
	secondRenew := make(chan struct{})
	source := &fakeReceiveSource{}
	source.renewFn = func(call int) error {
		if call == 1 {
			return context.DeadlineExceeded
		}
		if call == 2 {
			close(secondRenew)
		}
		return nil
	}
	var returned int32
	source.receiveFn = func(ctx context.Context, call int, record receiveRecord) ([]*Message, error) {
		if atomic.CompareAndSwapInt32(&returned, 0, 1) {
			message := source.message(10)
			message.LockedUntil = time.Now().Add(200 * time.Millisecond)
			return []*Message{message}, nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	var observed int32
	ctx, cancel := context.WithCancel(context.Background())
	receiver := newReceiver(source, "jobs", []ReceiverOption{
		WithAutoRenew(),
		WithErrorHandler(func(error) { atomic.AddInt32(&observed, 1) }),
	})
	receiver.renewRetryBackoff = 20 * time.Millisecond
	receiver.maxRenewBackoff = 40 * time.Millisecond
	receiver.jitter = func(delay time.Duration) time.Duration { return delay }
	err := receiver.Run(ctx, func(context.Context, *Message) error {
		select {
		case <-secondRenew:
		case <-time.After(time.Second):
			t.Error("transient renewal was not retried")
		}
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v", err)
	}
	settles := source.settleSnapshot()
	var renews, completes int
	for _, record := range settles {
		switch record.op {
		case "renew":
			renews++
		case "complete":
			completes++
		}
	}
	if renews < 2 || completes != 1 {
		t.Fatalf("settlements = %+v", settles)
	}
	if settles[len(settles)-1].op != "complete" {
		t.Fatalf("renewal overlapped terminal settlement: %+v", settles)
	}
	if got := atomic.LoadInt32(&observed); got != 0 {
		t.Fatalf("error notifications = %d, want 0 after recovered renewal", got)
	}
}

func TestReceiverRetriesHTTP503RenewalDespitePermissionBody(t *testing.T) {
	secondRenew := make(chan struct{})
	source := &fakeReceiveSource{}
	source.renewFn = func(call int) error {
		if call == 1 {
			return &HTTPError{StatusCode: http.StatusServiceUnavailable, cause: ErrPermissionDenied}
		}
		if call == 2 {
			close(secondRenew)
		}
		return nil
	}
	var returned int32
	source.receiveFn = func(ctx context.Context, _ int, _ receiveRecord) ([]*Message, error) {
		if atomic.CompareAndSwapInt32(&returned, 0, 1) {
			message := source.message(11)
			message.LockedUntil = time.Now().Add(200 * time.Millisecond)
			return []*Message{message}, nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	var observed int32
	ctx, cancel := context.WithCancel(context.Background())
	receiver := newReceiver(source, "jobs", []ReceiverOption{
		WithAutoRenew(),
		WithErrorHandler(func(error) { atomic.AddInt32(&observed, 1) }),
	})
	receiver.renewRetryBackoff = 20 * time.Millisecond
	receiver.maxRenewBackoff = 40 * time.Millisecond
	receiver.jitter = func(delay time.Duration) time.Duration { return delay }
	err := receiver.Run(ctx, func(context.Context, *Message) error {
		select {
		case <-secondRenew:
		case <-time.After(time.Second):
			t.Error("HTTP 503 renewal was not retried")
		}
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v, want context.Canceled", err)
	}
	if got := atomic.LoadInt32(&observed); got != 0 {
		t.Fatalf("error notifications = %d, want none after recovered renewal", got)
	}
	settles := source.settleSnapshot()
	var renews, completes int
	for _, record := range settles {
		switch record.op {
		case "renew":
			renews++
		case "complete":
			completes++
		}
	}
	if renews < 2 || completes != 1 {
		t.Fatalf("settlements = %+v, want recovered renewal followed by Complete", settles)
	}
}

func TestReceiverRetriesRenewalThroughTransientWindow(t *testing.T) {
	renewed := make(chan struct{})
	source := &fakeReceiveSource{}
	source.renewFn = func(call int) error {
		if call <= 2 {
			return errors.New("temporary renew failure")
		}
		if call == 3 {
			close(renewed)
		}
		return nil
	}
	var returned int32
	source.receiveFn = func(ctx context.Context, call int, record receiveRecord) ([]*Message, error) {
		if atomic.CompareAndSwapInt32(&returned, 0, 1) {
			message := source.message(20)
			message.LockedUntil = time.Now().Add(500 * time.Millisecond)
			return []*Message{message}, nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	var observed int32
	ctx, cancel := context.WithCancel(context.Background())
	receiver := newReceiver(source, "jobs", []ReceiverOption{
		WithAutoRenew(),
		WithErrorHandler(func(error) { atomic.AddInt32(&observed, 1) }),
	})
	receiver.renewRetryBackoff = 20 * time.Millisecond
	receiver.maxRenewBackoff = 40 * time.Millisecond
	receiver.jitter = func(delay time.Duration) time.Duration { return delay }
	err := receiver.Run(ctx, func(context.Context, *Message) error {
		select {
		case <-renewed:
		case <-time.After(800 * time.Millisecond):
			t.Error("renewal did not recover after the transient window")
		}
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v", err)
	}
	settles := source.settleSnapshot()
	var renews, completes int
	for _, record := range settles {
		switch record.op {
		case "renew":
			renews++
		case "complete":
			completes++
		}
	}
	if renews != 3 || completes != 1 {
		t.Fatalf("settlements = %+v, want three renewals then one completion", settles)
	}
	if got := atomic.LoadInt32(&observed); got != 0 {
		t.Fatalf("error notifications = %d, want 0 after recovered renewal", got)
	}
}

func TestReceiverRequiresContinuedConfirmationForUnusableRenewDeadline(t *testing.T) {
	tests := []struct {
		name       string
		deadline   func() time.Time
		renewCalls int32
	}{
		{name: "zero", deadline: func() time.Time { return time.Time{} }, renewCalls: 2},
		{name: "expired", deadline: func() time.Time { return time.Now().Add(-time.Second) }, renewCalls: 2},
		{name: "inside safety window", deadline: func() time.Time { return time.Now().Add(5 * time.Millisecond) }, renewCalls: 2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			base := &fakeReceiveSource{}
			var delivered int32
			base.receiveFn = func(ctx context.Context, _ int, _ receiveRecord) ([]*Message, error) {
				if atomic.CompareAndSwapInt32(&delivered, 0, 1) {
					message := base.message(30)
					message.LockedUntil = time.Now().Add(300 * time.Millisecond)
					return []*Message{message}, nil
				}
				<-ctx.Done()
				return nil, ctx.Err()
			}
			var renewCalls int32
			source := &deadlineLeaseSource{fakeReceiveSource: base}
			source.renewLeaseFn = func(context.Context, *Message) (time.Time, error) {
				if atomic.AddInt32(&renewCalls, 1) == 1 {
					return test.deadline(), nil
				}
				return time.Time{}, ErrLockLost
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			var observed int32
			handlerCanceled := make(chan struct{})
			receiver := newReceiver(source, "jobs", []ReceiverOption{
				WithAutoRenew(),
				WithErrorHandler(func(err error) {
					if !errors.Is(err, ErrLockLost) {
						t.Errorf("error notification = %v, want ErrLockLost", err)
					}
					atomic.AddInt32(&observed, 1)
					cancel()
				}),
			})
			err := receiver.Run(ctx, func(handlerCtx context.Context, _ *Message) error {
				<-handlerCtx.Done()
				close(handlerCanceled)
				return handlerCtx.Err()
			})
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Run error = %v, want context canceled", err)
			}
			select {
			case <-handlerCanceled:
			default:
				t.Fatal("unusable committed deadline did not cancel handler")
			}
			if got := atomic.LoadInt32(&renewCalls); got != test.renewCalls {
				t.Fatalf("deadline renewal calls = %d, want %d", got, test.renewCalls)
			}
			if got := atomic.LoadInt32(&observed); got != 1 {
				t.Fatalf("error notifications = %d, want one ErrLockLost", got)
			}
			if settles := base.settleSnapshot(); len(settles) != 0 {
				t.Fatalf("settlements after unusable committed deadline = %+v, want none", settles)
			}
		})
	}
}

func TestReceiverCancelsHandlerOnceLeaseIsLost(t *testing.T) {
	source := &fakeReceiveSource{}
	source.renewFn = func(int) error { return errors.New("broker unavailable") }
	var returned int32
	source.receiveFn = func(ctx context.Context, call int, record receiveRecord) ([]*Message, error) {
		if atomic.CompareAndSwapInt32(&returned, 0, 1) {
			message := source.message(21)
			message.LockedUntil = time.Now().Add(120 * time.Millisecond)
			return []*Message{message}, nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var handlerCanceled int32
	var observedMu sync.Mutex
	var observed []error
	receiver := newReceiver(source, "jobs", []ReceiverOption{
		WithAutoRenew(),
		WithErrorHandler(func(err error) {
			observedMu.Lock()
			observed = append(observed, err)
			observedMu.Unlock()
		}),
	})
	receiver.renewRetryBackoff = 20 * time.Millisecond
	receiver.maxRenewBackoff = 40 * time.Millisecond
	receiver.jitter = func(delay time.Duration) time.Duration { return delay }
	err := receiver.Run(ctx, func(handlerCtx context.Context, _ *Message) error {
		select {
		case <-handlerCtx.Done():
			atomic.StoreInt32(&handlerCanceled, 1)
			cancel()
			return handlerCtx.Err()
		case <-time.After(500 * time.Millisecond):
			cancel()
			return nil
		}
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v", err)
	}
	if atomic.LoadInt32(&handlerCanceled) != 1 {
		t.Fatalf("handler was not canceled promptly when its lease expired")
	}
	settles := source.settleSnapshot()
	if len(settles) == 0 {
		t.Fatalf("settlements = %+v, want renewal attempts", settles)
	}
	// Renewal attempts are expected, but a known-lost token must never receive
	// terminal settlement from the managed receiver.
	for _, record := range settles {
		if record.op == "complete" || record.op == "abandon" {
			t.Fatalf("known-lost token received terminal settlement: %+v", settles)
		}
	}
	if len(settles) > 5 {
		t.Fatalf("renewal retries spun near the lease deadline: %+v", settles)
	}
	observedMu.Lock()
	defer observedMu.Unlock()
	if len(observed) != 1 || !errors.Is(observed[0], ErrLockLost) {
		t.Fatalf("error notifications = %v, want one ErrLockLost", observed)
	}
}

func TestReceiverUsesRenewBatchDeadline(t *testing.T) {
	var renewBatchCalls int32
	var renewCalls int32
	renewed := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case wire.PathReceive:
			_ = json.NewEncoder(w).Encode(wire.ReceiveResponse{Messages: []wire.Message{{
				SeqNumber: 1, LockToken: "lock-1",
				LockedUntilMs: time.Now().Add(90*time.Millisecond).UnixNano() / int64(time.Millisecond),
			}}})
		case wire.PathRenewBatch:
			atomic.AddInt32(&renewBatchCalls, 1)
			select {
			case renewed <- struct{}{}:
			default:
			}
			_ = json.NewEncoder(w).Encode(wire.RenewBatchResponse{Results: []wire.SettleItemResult{{
				SeqNumber: 1, Ok: true,
				LockedUntilMs: time.Now().Add(600*time.Millisecond).UnixNano() / int64(time.Millisecond),
			}}})
		case wire.PathRenew:
			atomic.AddInt32(&renewCalls, 1)
			_ = json.NewEncoder(w).Encode(wire.SettleResponse{Ok: true})
		case wire.PathComplete:
			_ = json.NewEncoder(w).Encode(wire.SettleResponse{Ok: true})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	client, err := Open(context.Background(), server.URL)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	receiver := client.Receiver("jobs", WithAutoRenew())
	err = receiver.Run(ctx, func(context.Context, *Message) error {
		select {
		case <-renewed:
		case <-time.After(400 * time.Millisecond):
			t.Error("managed receiver did not call RenewBatch")
		}
		time.Sleep(100 * time.Millisecond)
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v", err)
	}
	if got := atomic.LoadInt32(&renewBatchCalls); got != 1 {
		t.Fatalf("RenewBatch calls = %d, want 1 after deadline reset", got)
	}
	if got := atomic.LoadInt32(&renewCalls); got != 0 {
		t.Fatalf("single Renew calls = %d, want 0", got)
	}
}

func TestReceiverRetriesInvalidRenewBatchAcknowledgement(t *testing.T) {
	var renewCalls int32
	renewed := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case wire.PathReceive:
			_ = json.NewEncoder(w).Encode(wire.ReceiveResponse{Messages: []wire.Message{{
				SeqNumber: 1, LockToken: "lock-1",
				LockedUntilMs: time.Now().Add(150*time.Millisecond).UnixNano() / int64(time.Millisecond),
			}}})
		case wire.PathRenewBatch:
			call := atomic.AddInt32(&renewCalls, 1)
			if call == 1 {
				_, _ = io.WriteString(w, `{"results":[{"seq_number":1}]}`)
				return
			}
			_ = json.NewEncoder(w).Encode(wire.RenewBatchResponse{Results: []wire.SettleItemResult{{
				SeqNumber: 1, Ok: true,
				LockedUntilMs: time.Now().Add(time.Second).UnixNano() / int64(time.Millisecond),
			}}})
			if call == 2 {
				close(renewed)
			}
		case wire.PathComplete:
			_ = json.NewEncoder(w).Encode(wire.SettleResponse{Ok: true})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	client, err := Open(context.Background(), server.URL)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var notifications int32
	receiver := client.Receiver("jobs", WithAutoRenew(), WithErrorHandler(func(error) {
		atomic.AddInt32(&notifications, 1)
	}))
	receiver.renewRetryBackoff = time.Millisecond
	receiver.maxRenewBackoff = time.Millisecond
	receiver.jitter = func(delay time.Duration) time.Duration { return delay }
	err = receiver.Run(ctx, func(context.Context, *Message) error {
		select {
		case <-renewed:
		case <-time.After(500 * time.Millisecond):
			t.Error("invalid renewal acknowledgement was not retried")
		}
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v, want context.Canceled", err)
	}
	if got := atomic.LoadInt32(&renewCalls); got != 2 {
		t.Fatalf("RenewBatch calls = %d, want 2", got)
	}
	if got := atomic.LoadInt32(&notifications); got != 0 {
		t.Fatalf("error notifications = %d, want none after recovered retry", got)
	}
}

func TestRenewCapabilityProbeDoesNotSerializeMessages(t *testing.T) {
	base := &fakeReceiveSource{}
	source := &concurrentLeaseSource{
		fakeReceiveSource: base,
		both:              make(chan struct{}),
		release:           make(chan struct{}),
	}
	receiver := newReceiver(source, "jobs", nil)
	done := make(chan error, 2)
	for seq := int64(1); seq <= 2; seq++ {
		message := base.message(seq)
		go func(m *Message) {
			_, err := receiver.renewMessage(context.Background(), m, time.Second)
			done <- err
		}(message)
	}
	select {
	case <-source.both:
	case <-time.After(2 * time.Second):
		close(source.release)
		t.Fatal("one deadline-bearing renewal blocked another behind capability state")
	}
	close(source.release)
	for i := 0; i < 2; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
}

func TestReceiverPermanentlyFallsBackWhenRenewBatchUnsupported(t *testing.T) {
	var renewBatchCalls int32
	var renewCalls int32
	twoSingles := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case wire.PathReceive:
			_ = json.NewEncoder(w).Encode(wire.ReceiveResponse{Messages: []wire.Message{{
				SeqNumber: 1, LockToken: "lock-1",
				LockedUntilMs: time.Now().Add(300*time.Millisecond).UnixNano() / int64(time.Millisecond),
			}}})
		case wire.PathRenewBatch:
			atomic.AddInt32(&renewBatchCalls, 1)
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(wire.ErrorBody{Message: "no such path: " + wire.PathRenewBatch})
		case wire.PathRenew:
			if atomic.AddInt32(&renewCalls, 1) == 2 {
				close(twoSingles)
			}
			_ = json.NewEncoder(w).Encode(wire.SettleResponse{Ok: true})
		case wire.PathComplete:
			_ = json.NewEncoder(w).Encode(wire.SettleResponse{Ok: true})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	client, err := Open(context.Background(), server.URL)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	receiver := client.Receiver("jobs", WithAutoRenew())
	err = receiver.Run(ctx, func(context.Context, *Message) error {
		select {
		case <-twoSingles:
		case <-time.After(time.Second):
			t.Error("single-message fallback did not keep renewing")
		}
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v", err)
	}
	if got := atomic.LoadInt32(&renewBatchCalls); got != 1 {
		t.Fatalf("RenewBatch calls = %d, want one capability probe", got)
	}
	if got := atomic.LoadInt32(&renewCalls); got < 2 {
		t.Fatalf("single Renew calls = %d, want at least 2", got)
	}
}

func TestReceiverStopsRenewalBeforeSettlementAfterPermanentRenewError(t *testing.T) {
	source := &fakeReceiveSource{}
	source.renewFn = func(int) error { return ErrPermissionDenied }
	var returned int32
	source.receiveFn = func(ctx context.Context, call int, record receiveRecord) ([]*Message, error) {
		if atomic.CompareAndSwapInt32(&returned, 0, 1) {
			message := source.message(11)
			message.LockedUntil = time.Now().Add(20 * time.Millisecond)
			return []*Message{message}, nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}

	errorEntered := make(chan struct{})
	releaseError := make(chan struct{})
	handlerCanceled := make(chan struct{})
	receiver := newReceiver(source, "jobs", []ReceiverOption{
		WithAutoRenew(),
		WithErrorHandler(func(error) {
			close(errorEntered)
			<-releaseError
		}),
	})
	done := make(chan error, 1)
	go func() {
		done <- receiver.Run(context.Background(), func(ctx context.Context, _ *Message) error {
			<-ctx.Done()
			close(handlerCanceled)
			return nil
		})
	}()
	select {
	case <-errorEntered:
	case <-time.After(time.Second):
		t.Fatal("permanent renewal error was not reported")
	}
	select {
	case <-handlerCanceled:
	case <-time.After(2 * time.Second):
		close(releaseError)
		t.Fatal("slow error handler delayed message handler cancellation")
	}
	close(releaseError)
	err := <-done
	if !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("Run error = %v, want ErrPermissionDenied", err)
	}
	settles := source.settleSnapshot()
	if len(settles) != 1 || settles[0].op != "renew" {
		t.Fatalf("settlements = %+v", settles)
	}
}

func TestReceiverJoinsInFlightRenewBeforeComplete(t *testing.T) {
	settler := newBlockingRenewSettler()
	message := &Message{
		SequenceNumber: 12,
		LockedUntil:    time.Now().Add(20 * time.Millisecond),
		queue:          "jobs",
		lockToken:      "token-12",
		s:              settler,
	}
	receiver := newReceiver(nil, "jobs", []ReceiverOption{WithAutoRenew()})
	failures := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		receiver.process(context.Background(), message, func(context.Context, *Message) error {
			<-settler.renewStarted
			return nil
		}, func(err error) {
			failures <- err
		})
		close(done)
	}()

	select {
	case <-settler.renewCanceled:
	case <-time.After(time.Second):
		close(settler.allowRenewReturn)
		t.Fatal("in-flight renewal did not observe cancellation")
	}

	settledEarly := false
	select {
	case <-settler.completeCalled:
		settledEarly = true
	case <-time.After(20 * time.Millisecond):
	}
	close(settler.allowRenewReturn)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("process did not return after renewal stopped")
	}
	select {
	case err := <-failures:
		t.Fatalf("unexpected receiver failure: %v", err)
	default:
	}
	if settledEarly {
		t.Fatal("Complete ran while Renew was still in flight")
	}
	operations := settler.snapshot()
	if len(operations) != 2 || operations[0] != "renew returned" || operations[1] != "complete" {
		t.Fatalf("operations = %v", operations)
	}
}

func TestReceiverStopsIdleRenewLoopBeforeComplete(t *testing.T) {
	source := &fakeReceiveSource{}
	message := source.message(13)
	message.LockedUntil = time.Now().Add(time.Hour)
	receiver := newReceiver(source, "jobs", []ReceiverOption{WithAutoRenew()})
	failures := make(chan error, 1)
	receiver.process(context.Background(), message, func(context.Context, *Message) error {
		return nil
	}, func(err error) {
		failures <- err
	})

	select {
	case err := <-failures:
		t.Fatalf("unexpected receiver failure: %v", err)
	default:
	}
	settles := source.settleSnapshot()
	if len(settles) != 1 || settles[0].op != "complete" {
		t.Fatalf("settlements = %+v", settles)
	}
}

func TestReceiverRejectsOversizedReceiveBatch(t *testing.T) {
	source := &fakeReceiveSource{}
	source.receiveFn = func(context.Context, int, receiveRecord) ([]*Message, error) {
		return []*Message{source.message(1), source.message(2)}, nil
	}
	receiver := newReceiver(source, "jobs", nil)
	err := receiver.Run(context.Background(), func(context.Context, *Message) error { return nil })
	if err == nil || len(source.settleSnapshot()) != 0 {
		t.Fatalf("Run error = %v, settlements = %+v", err, source.settleSnapshot())
	}
}
