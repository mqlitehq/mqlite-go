package mqlite

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSettlementHandoffJoinsCanceledRenewAndRetainsConfirmedLease(t *testing.T) {
	base := &fakeReceiveSource{}
	source := &deadlineLeaseSource{fakeReceiveSource: base}
	message := base.message(1)
	message.LockedUntil = time.Now().Add(500 * time.Millisecond)
	message.localLeaseDeadline = message.LockedUntil
	message.leaseClockResolution = time.Millisecond
	secondStarted, secondCanceled, allowSecond := make(chan struct{}), make(chan struct{}), make(chan struct{})
	thirdStarted, completeStarted := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(allowSecond) }) }
	defer release()
	var calls int32
	source.renewalLeaseFn = func(ctx context.Context, _ *Message) (renewalLease, error) {
		var deadline time.Time
		switch atomic.AddInt32(&calls, 1) {
		case 1:
			deadline = time.Now().Add(time.Second)
		case 2:
			close(secondStarted)
			<-ctx.Done()
			close(secondCanceled)
			<-allowSecond
			deadline = time.Now().Add(time.Hour)
		default:
			close(thirdStarted)
			deadline = time.Now().Add(2 * time.Second)
		}
		return renewalLease{deadline: deadline, clockResolution: time.Millisecond}, nil
	}
	base.settleFn = func(settleRecord, int) error {
		close(completeStarted)
		select {
		case <-thirdStarted:
			return nil
		case <-time.After(2 * time.Second):
			return ErrInvalidArgument
		}
	}
	var observed int32
	r := newReceiver(source, "q", []ReceiverOption{WithAutoRenew(), WithErrorHandler(func(err error) {
		atomic.AddInt32(&observed, 1)
		t.Errorf("handoff error: %v", err)
	})})
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.process(context.Background(), message, func(context.Context, *Message) error { <-secondStarted; return nil }, func(error) {})
	}()
	select {
	case <-secondCanceled:
	case <-time.After(3 * time.Second):
		t.Fatal("handling renewal did not observe handoff cancellation")
	}
	select {
	case <-completeStarted:
		t.Fatal("settlement ran before the handling renewal joined")
	case <-time.After(20 * time.Millisecond):
	}
	release()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("settlement did not use the last confirmed lease after handoff")
	}
	if atomic.LoadInt32(&calls) != 3 || atomic.LoadInt32(&observed) != 0 {
		t.Fatal("handoff lost the confirmed lease or accepted a canceled late renewal")
	}
}

func TestHandlingRenewalGoexitCancelsHandlerAndSkipsSettlement(t *testing.T) {
	base := &fakeReceiveSource{}
	source := &deadlineLeaseSource{fakeReceiveSource: base}
	message := base.message(1)
	message.LockedUntil = time.Now().Add(500 * time.Millisecond)
	message.localLeaseDeadline = message.LockedUntil
	message.leaseClockResolution = time.Millisecond
	var goexited int32
	source.renewalLeaseFn = func(context.Context, *Message) (renewalLease, error) {
		atomic.StoreInt32(&goexited, 1)
		runtime.Goexit()
		return renewalLease{}, nil
	}
	observed := make(chan error, 4)
	r := newReceiver(source, "q", []ReceiverOption{WithAutoRenew(), WithErrorHandler(func(err error) { observed <- err })})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.process(ctx, message, func(handlerCtx context.Context, _ *Message) error { <-handlerCtx.Done(); return handlerCtx.Err() }, func(error) {})
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handling heartbeat Goexit did not cancel and join its handler")
	}
	if atomic.LoadInt32(&goexited) != 1 {
		t.Fatal("test did not exercise transport Goexit")
	}
	select {
	case err := <-observed:
		if !errors.Is(err, ErrLockLost) {
			t.Fatalf("aborted heartbeat=%v, want LockLost", err)
		}
	default:
		t.Fatal("aborted handling heartbeat was silent")
	}
	if len(base.settleSnapshot()) != 0 {
		t.Fatal("aborted heartbeat authorized settlement")
	}
}

func TestSettlementRenewalGoexitDoesNotStrandJoin(t *testing.T) {
	base := &fakeReceiveSource{}
	source := &deadlineLeaseSource{fakeReceiveSource: base}
	message := base.message(1)
	message.LockedUntil = time.Now().Add(500 * time.Millisecond)
	message.localLeaseDeadline = message.LockedUntil
	message.leaseClockResolution = time.Millisecond
	started := make(chan struct{})
	source.renewalLeaseFn = func(context.Context, *Message) (renewalLease, error) {
		close(started)
		runtime.Goexit()
		return renewalLease{}, nil
	}
	base.settleFn = func(settleRecord, int) error { <-started; return nil }
	r := newReceiver(source, "q", []ReceiverOption{WithAutoRenew(), WithErrorHandler(func(err error) { t.Errorf("unexpected error: %v", err) })})
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.process(context.Background(), message, func(context.Context, *Message) error { return nil }, func(error) {})
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("heartbeat Goexit stranded settlement join")
	}
}

func TestSettlementHeartbeatPermanentFailureStopsRunBeforeNotification(t *testing.T) {
	base := &fakeReceiveSource{}
	source := &deadlineLeaseSource{fakeReceiveSource: base}
	message := base.message(1)
	message.LockedUntil = time.Now().Add(500 * time.Millisecond)
	message.localLeaseDeadline = message.LockedUntil
	message.leaseClockResolution = time.Millisecond
	notifyEntered, releaseNotify := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseNotify) }) }
	defer release()
	var receiveContext context.Context
	base.receiveFn = func(ctx context.Context, call int, _ receiveRecord) ([]*Message, error) {
		if call == 1 {
			receiveContext = ctx
			return []*Message{message}, nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	source.renewalLeaseFn = func(context.Context, *Message) (renewalLease, error) { return renewalLease{}, ErrPermissionDenied }
	base.settleFn = func(settleRecord, int) error { <-notifyEntered; return nil }
	var notifications int32
	r := newReceiver(source, "q", []ReceiverOption{WithAutoRenew(), WithConcurrency(1), WithErrorHandler(func(err error) {
		if !errors.Is(err, ErrPermissionDenied) {
			t.Errorf("unexpected error: %v", err)
		}
		if receiveContext == nil || receiveContext.Err() == nil {
			t.Error("Run was not stopped before the permanent renewal notification")
		}
		atomic.AddInt32(&notifications, 1)
		close(notifyEntered)
		<-releaseNotify
	})})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx, func(context.Context, *Message) error { return nil }) }()
	select {
	case <-notifyEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("settlement heartbeat did not report its permanent failure")
	}
	calls := len(base.receiveSnapshot())
	select {
	case <-done:
		t.Fatal("Run returned before the notifying heartbeat joined")
	case <-time.After(20 * time.Millisecond):
	}
	if len(base.receiveSnapshot()) != calls {
		t.Fatal("claim started after permanent renewal failure")
	}
	release()
	select {
	case err := <-done:
		if !errors.Is(err, ErrPermissionDenied) {
			t.Fatalf("Run=%v, want original permanent renewal failure", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not join after notification returned")
	}
	if atomic.LoadInt32(&notifications) != 1 {
		t.Fatal("permanent heartbeat failure was not reported exactly once")
	}
}

func TestSettlementPanicAndGoexitJoinHeartbeat(t *testing.T) {
	for _, goexit := range []bool{false, true} {
		name := "panic"
		if goexit {
			name = "Goexit"
		}
		t.Run(name, func(t *testing.T) {
			base := &fakeReceiveSource{}
			source := &deadlineLeaseSource{fakeReceiveSource: base}
			message := base.message(1)
			message.LockedUntil = time.Now().Add(500 * time.Millisecond)
			message.localLeaseDeadline = message.LockedUntil
			message.leaseClockResolution = time.Millisecond
			started, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			defer unblock()
			source.renewalLeaseFn = func(ctx context.Context, _ *Message) (renewalLease, error) {
				close(started)
				<-ctx.Done()
				close(canceled)
				<-release
				return renewalLease{}, ctx.Err()
			}
			base.settleFn = func(settleRecord, int) error {
				<-started
				if goexit {
					runtime.Goexit()
				}
				panic("controlled settlement panic")
			}
			r := newReceiver(source, "q", []ReceiverOption{WithAutoRenew(), WithErrorHandler(func(err error) { t.Errorf("unexpected error: %v", err) })})
			done := make(chan struct{})
			go func() {
				defer close(done)
				defer func() { _ = recover() }()
				r.process(context.Background(), message, func(context.Context, *Message) error { return nil }, func(error) {})
			}()
			select {
			case <-canceled:
			case <-time.After(3 * time.Second):
				t.Fatal("settlement unwind did not cancel its heartbeat")
			}
			select {
			case <-done:
				t.Fatal("settlement unwind returned before its heartbeat exited")
			case <-time.After(20 * time.Millisecond):
			}
			unblock()
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Fatal("settlement unwind failed to join heartbeat")
			}
		})
	}
}

func TestSettlementRenewalLossIsResolvedByFinalSettlement(t *testing.T) {
	for _, outcome := range []string{"confirmed", "replayed", "lock lost", "unknown"} {
		t.Run(outcome, func(t *testing.T) {
			base := &fakeReceiveSource{}
			source := &deadlineLeaseSource{fakeReceiveSource: base}
			message := base.message(1)
			message.LockedUntil = time.Now().Add(500 * time.Millisecond)
			message.localLeaseDeadline = message.LockedUntil
			message.leaseClockResolution = time.Millisecond
			renewLost := make(chan struct{})
			var once sync.Once
			source.renewalLeaseFn = func(context.Context, *Message) (renewalLease, error) {
				once.Do(func() { close(renewLost) })
				return renewalLease{}, ErrLockLost
			}
			attempts := 0
			complete := func(context.Context) error {
				attempts++
				select {
				case <-renewLost:
				case <-time.After(2 * time.Second):
					return ErrInvalidArgument
				}
				switch outcome {
				case "confirmed":
					return nil
				case "replayed":
					if attempts == 1 {
						return errors.New("lost committed settlement acknowledgement")
					}
					return nil
				case "lock lost":
					return ErrLockLost
				default:
					return errors.New("settlement response unavailable")
				}
			}
			base.settleFn = func(record settleRecord, _ int) error {
				if record.op != "complete" {
					return ErrInvalidArgument
				}
				return complete(context.Background())
			}
			var mu sync.Mutex
			var observed []error
			r := newReceiver(source, "q", []ReceiverOption{WithAutoRenew(), WithErrorHandler(func(err error) {
				mu.Lock()
				observed = append(observed, err)
				mu.Unlock()
			})})
			if outcome == "unknown" {
				r.settlement.totalTimeout = 350 * time.Millisecond
				r.settlement.attemptTimeout = 100 * time.Millisecond
			}
			handlers := 0
			r.process(context.Background(), message, func(context.Context, *Message) error { handlers++; return nil }, func(error) { t.Error("ownership loss must not stop Run") })
			select {
			case <-renewLost:
			default:
				t.Fatal("settlement did not overlap an ownership-losing heartbeat")
			}
			mu.Lock()
			defer mu.Unlock()
			if handlers != 1 {
				t.Fatal("handler repeated")
			}
			if outcome == "confirmed" || outcome == "replayed" {
				if outcome == "replayed" && attempts != 2 {
					t.Fatal("settlement did not replay after the lost acknowledgement")
				}
				if outcome == "replayed" {
					records := base.settleSnapshot()
					if len(records) != 2 || records[0] != records[1] {
						t.Fatal("settlement replay changed its exact request identity")
					}
				}
				if len(observed) != 0 {
					t.Fatalf("own settlement loss was reported: %v", observed)
				}
				return
			}
			if len(observed) != 1 {
				t.Fatalf("observed %d errors, want the single final settlement outcome", len(observed))
			}
			if outcome == "lock lost" {
				if !errors.Is(observed[0], ErrLockLost) {
					t.Fatalf("final error = %v, want lock lost", observed[0])
				}
			} else if !errors.Is(observed[0], ErrOutcomeUnknown) || errors.Is(observed[0], ErrLockLost) {
				t.Fatalf("unconfirmed settlement lost Unknown identity: %v", observed[0])
			}
		})
	}
}

// This standard-library-only fenced source mirrors the real-broker test in the
// main repository. Complete is delayed before its effect, past the old lease.
func TestReceiverRenewsUntilSettlementConfirmed(t *testing.T) {
	for _, cancelDuringSettlement := range []bool{false, true} {
		name := "normal"
		if cancelDuringSettlement {
			name = "Run canceled during settlement"
		}
		t.Run(name, func(t *testing.T) {
			base := &fakeReceiveSource{}
			source := &deadlineLeaseSource{fakeReceiveSource: base}
			message := base.message(1)
			message.LockedUntil = time.Now().Add(time.Second)
			message.localLeaseDeadline = message.LockedUntil
			message.leaseClockResolution = time.Millisecond
			var mu sync.Mutex
			deadline := message.LockedUntil
			var renewals, handlers int32
			source.renewalLeaseFn = func(context.Context, *Message) (renewalLease, error) {
				mu.Lock()
				defer mu.Unlock()
				if !deadline.After(time.Now()) {
					return renewalLease{}, ErrLockLost
				}
				deadline = time.Now().Add(time.Second)
				atomic.AddInt32(&renewals, 1)
				return renewalLease{deadline: deadline, clockResolution: time.Millisecond}, nil
			}
			base.receiveFn = func(ctx context.Context, call int, _ receiveRecord) ([]*Message, error) {
				if call == 1 {
					return []*Message{message}, nil
				}
				<-ctx.Done()
				return nil, ctx.Err()
			}
			started, ack := make(chan struct{}), make(chan struct{})
			base.settleFn = func(record settleRecord, _ int) error {
				if record.op != "complete" {
					return errors.New("unexpected settlement")
				}
				close(started)
				time.Sleep(1250 * time.Millisecond)
				mu.Lock()
				defer mu.Unlock()
				if !deadline.After(time.Now()) {
					return ErrLockLost
				}
				close(ack)
				return nil
			}
			ctx, cancel := context.WithCancel(context.Background())
			observed := make(chan error, 8)
			runDone := make(chan error, 1)
			receiver := newReceiver(source, "jobs", []ReceiverOption{WithAutoRenew(), WithConcurrency(1), WithErrorHandler(func(err error) { observed <- err })})
			go func() {
				runDone <- receiver.Run(ctx, func(context.Context, *Message) error { atomic.AddInt32(&handlers, 1); return nil })
			}()
			defer func() {
				cancel()
				select {
				case err := <-runDone:
					if !errors.Is(err, context.Canceled) {
						t.Errorf("Run = %v, want context.Canceled", err)
					}
				case <-time.After(5 * time.Second):
					t.Error("Run did not join settlement and renewal")
				}
			}()
			select {
			case <-started:
			case <-time.After(5 * time.Second):
				t.Fatal("Complete did not start")
			}
			if cancelDuringSettlement {
				cancel()
			}
			select {
			case <-ack:
			case err := <-observed:
				t.Fatalf("live token lost while Complete was waiting: %v", err)
			case <-time.After(5 * time.Second):
				t.Fatal("Complete was not confirmed")
			}
			if atomic.LoadInt32(&handlers) != 1 || atomic.LoadInt32(&renewals) == 0 {
				t.Fatal("handler repeated or no settlement-phase renewal ran")
			}
			select {
			case err := <-observed:
				t.Fatalf("unexpected receiver error: %v", err)
			default:
			}
		})
	}
}
