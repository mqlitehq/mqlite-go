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

func TestRenewalHandoffStopDoesNotHideInFlightErrors(t *testing.T) {
	for _, tc := range []struct {
		name     string
		response error
		want     error
		fatal    bool
		late     bool
	}{
		{"transient_with_live_lease", &HTTPError{StatusCode: 503}, nil, false, false},
		{"lock_lost", &HTTPError{StatusCode: 409, cause: ErrLockLost}, ErrLockLost, false, false},
		{"unauthenticated", &HTTPError{StatusCode: 401, cause: ErrUnauthenticated}, ErrUnauthenticated, true, false},
		{"permission_denied", &HTTPError{StatusCode: 403, cause: ErrPermissionDenied}, ErrPermissionDenied, true, false},
		{"late_ack_after_deadline", nil, ErrLockLost, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := &fakeReceiveSource{}
			source := &deadlineLeaseSource{fakeReceiveSource: base}
			m := base.message(1)
			started, release, stop := make(chan struct{}), make(chan struct{}), make(chan struct{})
			source.renewalLeaseFn = func(ctx context.Context, _ *Message) (renewalLease, error) {
				close(started)
				if tc.late {
					<-ctx.Done()
					return renewalLease{deadline: time.Now().Add(time.Hour), clockResolution: time.Millisecond}, nil
				}
				<-release
				return renewalLease{}, tc.response
			}
			observed := make(chan error, 2)
			order := make(chan string, 2)
			r := newReceiver(source, "q", []ReceiverOption{WithErrorHandler(func(err error) { order <- "notify"; observed <- err })})
			deadline := time.Now().Add(200 * time.Millisecond)
			state := renewalProgress{deadline: deadline, leaseLength: 200 * time.Millisecond, lease: renewalLease{deadline: deadline, clockResolution: time.Millisecond}}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- r.renewLoop(ctx, m, func() {}, func(error) { order <- "fail" }, &state, false, stop) }()
			<-started
			// Order is deterministic: quiescence was requested before this RPC finished.
			close(stop)
			close(release)
			select {
			case outcome := <-done:
				if (outcome != nil) != (tc.want != nil) {
					t.Fatalf("renewal result=%v", outcome)
				}
			case <-time.After(time.Second):
				t.Fatal("stopped heartbeat did not finish its in-flight response")
			}
			if tc.want == nil {
				if len(observed) != 0 || len(order) != 0 {
					t.Fatal("live transient failure became terminal")
				}
				return
			}
			select {
			case err := <-observed:
				if !errors.Is(err, tc.want) {
					t.Fatalf("error=%v,want %v", err, tc.want)
				}
			default:
				t.Fatal("quiescence swallowed an in-flight failure")
			}
			if tc.fatal {
				if <-order != "fail" || <-order != "notify" {
					t.Fatal("permanent failure ordering changed")
				}
			} else if <-order != "notify" || len(order) != 0 {
				t.Fatal("advisory loss became fatal")
			}
		})
	}
}

func TestSettlementHandoffRetainsLeaseAfterTransientRenewalFailure(t *testing.T) {
	base := &fakeReceiveSource{}
	source := &deadlineLeaseSource{fakeReceiveSource: base}
	message := base.message(1)
	message.LockedUntil = time.Now().Add(500 * time.Millisecond)
	message.localLeaseDeadline = message.LockedUntil
	message.leaseClockResolution = time.Millisecond
	started, handoffStarted, allowError, settlingRenewed := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	var calls int32
	source.renewalLeaseFn = func(ctx context.Context, _ *Message) (renewalLease, error) {
		switch atomic.AddInt32(&calls, 1) {
		case 1:
			return renewalLease{deadline: time.Now().Add(500 * time.Millisecond), clockResolution: time.Millisecond}, nil
		case 2:
			close(started)
			<-allowError
			return renewalLease{}, &HTTPError{StatusCode: 503}
		default:
			close(settlingRenewed)
			return renewalLease{deadline: time.Now().Add(time.Hour), clockResolution: time.Millisecond}, nil
		}
	}
	base.settleFn = func(settleRecord, int) error {
		ctx := context.Background()
		select {
		case <-settlingRenewed:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	observed := make(chan error, 2)
	r := newReceiver(source, "q", []ReceiverOption{WithAutoRenew(), WithErrorHandler(func(err error) { observed <- err })})
	var once sync.Once
	r.settlement.withTimeout = func(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
		once.Do(func() { close(handoffStarted) })
		return context.WithTimeout(ctx, d)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.process(context.Background(), message, func(context.Context, *Message) error { <-started; return nil }, func(error) {})
	}()
	<-handoffStarted
	close(allowError)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handoff did not preserve a live confirmed lease after a transient renewal error")
	}
	if atomic.LoadInt32(&calls) != 3 || len(observed) != 0 {
		t.Fatalf("renew calls/errors=%d/%d", atomic.LoadInt32(&calls), len(observed))
	}
}

func TestSettlementHandoffPreservesFinalRenewalFailure(t *testing.T) {
	for _, tc := range []struct {
		name      string
		response  error
		want      error
		permanent bool
		late      bool
	}{
		{"lock_lost", &HTTPError{StatusCode: 409, cause: ErrLockLost}, ErrLockLost, false, false},
		{"unauthenticated", &HTTPError{StatusCode: 401, cause: ErrUnauthenticated}, ErrUnauthenticated, true, false},
		{"permission_denied", &HTTPError{StatusCode: 403, cause: ErrPermissionDenied}, ErrPermissionDenied, true, false},
		{"late_ack_after_attempt_deadline", nil, ErrLockLost, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := &fakeReceiveSource{}
			source := &deadlineLeaseSource{fakeReceiveSource: base}
			message := base.message(1)
			message.LockedUntil = time.Now().Add(500 * time.Millisecond)
			message.localLeaseDeadline = message.LockedUntil
			message.leaseClockResolution = time.Millisecond
			started, handlerReturned, allowReturn := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var calls, settled int32
			source.renewalLeaseFn = func(ctx context.Context, _ *Message) (renewalLease, error) {
				if atomic.AddInt32(&calls, 1) == 1 {
					return renewalLease{deadline: time.Now().Add(200 * time.Millisecond), clockResolution: time.Millisecond}, nil
				}
				close(started)
				if tc.late {
					<-ctx.Done()
					return renewalLease{deadline: time.Now().Add(time.Hour), clockResolution: time.Millisecond}, nil
				}
				<-allowReturn
				return renewalLease{}, tc.response
			}
			base.settleFn = func(settleRecord, int) error { atomic.AddInt32(&settled, 1); return nil }
			observed := make(chan error, 2)
			order := make(chan string, 2)
			r := newReceiver(source, "q", []ReceiverOption{WithAutoRenew(), WithErrorHandler(func(err error) { order <- "notify"; observed <- err })})
			done := make(chan struct{})
			go func() {
				defer close(done)
				r.process(context.Background(), message, func(context.Context, *Message) error { <-started; close(handlerReturned); return nil }, func(error) { order <- "fail" })
			}()
			<-handlerReturned
			close(allowReturn)
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("handoff failed to join terminal renewal")
			}
			select {
			case err := <-observed:
				if !errors.Is(err, tc.want) {
					t.Fatalf("renewal error=%v, want %v", err, tc.want)
				}
			default:
				t.Fatal("handoff swallowed terminal renewal error")
			}
			if atomic.LoadInt32(&settled) != 0 || len(observed) != 0 {
				t.Fatal("handoff settled after failure or duplicated notification")
			}
			if tc.permanent {
				if <-order != "fail" || <-order != "notify" {
					t.Fatal("permanent failure was not observed before notification")
				}
			} else if <-order != "notify" || len(order) != 0 {
				t.Fatal("advisory renewal failure became fatal")
			}
		})
	}
}

func TestSettlementHandoffSharesConfirmationBudget(t *testing.T) {
	base := &fakeReceiveSource{}
	source := &deadlineLeaseSource{fakeReceiveSource: base}
	message := base.message(1)
	message.LockedUntil = time.Now().Add(500 * time.Millisecond)
	message.localLeaseDeadline = message.LockedUntil
	message.leaseClockResolution = time.Millisecond
	started, canceled, allowExit := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var calls, settled, budgets int32
	source.renewalLeaseFn = func(ctx context.Context, _ *Message) (renewalLease, error) {
		if atomic.AddInt32(&calls, 1) == 1 {
			return renewalLease{deadline: time.Now().Add(time.Second), clockResolution: time.Millisecond}, nil
		}
		close(started)
		<-ctx.Done()
		close(canceled)
		<-allowExit
		return renewalLease{deadline: time.Now().Add(time.Hour), clockResolution: time.Millisecond}, nil
	}
	base.settleFn = func(settleRecord, int) error { atomic.AddInt32(&settled, 1); return nil }
	observed := make(chan error, 2)
	r := newReceiver(source, "q", []ReceiverOption{WithAutoRenew(), WithErrorHandler(func(err error) { observed <- err })})
	r.settlement.totalTimeout = 30 * time.Millisecond
	r.settlement.withTimeout = func(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
		if d == r.settlement.totalTimeout {
			atomic.AddInt32(&budgets, 1)
		}
		return context.WithTimeout(ctx, d)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.process(context.Background(), message, func(context.Context, *Message) error { <-started; return nil }, func(error) {})
	}()
	select {
	case <-canceled:
	case <-time.After(2 * time.Second):
		close(allowExit)
		t.Fatal("handoff did not cancel at the shared confirmation deadline")
	}
	select {
	case <-done:
		close(allowExit)
		t.Fatal("handoff deadline released its worker before renewal joined")
	case <-time.After(20 * time.Millisecond):
	}
	close(allowExit)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handoff did not join after cancellation")
	}
	if atomic.LoadInt32(&budgets) != 1 || atomic.LoadInt32(&settled) != 0 {
		t.Fatal("handoff reset its budget or attempted settlement after exhaustion")
	}
	select {
	case err := <-observed:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("handoff deadline error=%v", err)
		}
	default:
		t.Fatal("exhausted confirmation budget was silent")
	}
}

func TestSettlementHandoffFinishesHealthyRenewal(t *testing.T) {
	base := &fakeReceiveSource{}
	source := &deadlineLeaseSource{fakeReceiveSource: base}
	message := base.message(1)
	message.LockedUntil = time.Now().Add(500 * time.Millisecond)
	message.localLeaseDeadline = message.LockedUntil
	message.leaseClockResolution = time.Millisecond
	started, handlerReturned, allowReturn, exited := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	canceled := make(chan struct{}, 1)
	var calls int32
	source.renewalLeaseFn = func(ctx context.Context, _ *Message) (renewalLease, error) {
		switch atomic.AddInt32(&calls, 1) {
		case 1:
			return renewalLease{deadline: time.Now().Add(time.Second), clockResolution: time.Millisecond}, nil
		case 2:
			close(started)
			select {
			case <-ctx.Done():
				canceled <- struct{}{}
			case <-allowReturn:
			}
			close(exited)
			return renewalLease{deadline: time.Now().Add(time.Hour), clockResolution: time.Millisecond}, nil
		default:
			return renewalLease{deadline: time.Now().Add(time.Hour), clockResolution: time.Millisecond}, nil
		}
	}
	var settled int32
	base.settleFn = func(_ settleRecord, _ int) error {
		ctx := context.Background()
		select {
		case <-exited:
		default:
			t.Error("Complete preceded renewal join")
		}
		atomic.AddInt32(&settled, 1)
		// A fresh confirmed lease must replace the old short schedule during handoff.
		select {
		case <-time.After(700 * time.Millisecond):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	observed := make(chan error, 4)
	r := newReceiver(source, "q", []ReceiverOption{WithAutoRenew(), WithErrorHandler(func(err error) { observed <- err })})
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.process(context.Background(), message, func(context.Context, *Message) error { <-started; close(handlerReturned); return nil }, func(error) {})
	}()
	<-handlerReturned
	select {
	case <-canceled:
		close(allowReturn)
		<-done
		t.Fatal("normal handoff canceled a healthy in-flight renewal")
	case <-time.After(20 * time.Millisecond):
	}
	close(allowReturn)
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("healthy renewal did not join before settlement")
	}
	if atomic.LoadInt32(&calls) != 2 || atomic.LoadInt32(&settled) != 1 || len(observed) != 0 {
		t.Fatalf("renew/settle/errors = %d/%d/%d", atomic.LoadInt32(&calls), atomic.LoadInt32(&settled), len(observed))
	}
}

func TestSettlementHandoffAfterRunCancellationJoinsRenewAndRetainsConfirmedLease(t *testing.T) {
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
	ctx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.process(ctx, message, func(context.Context, *Message) error { <-secondStarted; cancelRun(); return nil }, func(error) {})
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
