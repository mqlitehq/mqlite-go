package mqlite

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mqlitehq/mqlite-go/internal/wire"
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

// A definitive rejection of Renew is distinct from token loss caused by the
// terminal operation's own commit. Exercise public HTTP calls in the actual
// settlement phase and keep the terminal acknowledgement/error independent.
func TestReceiverReportsDefinitiveRenewalErrorDuringSettlement(t *testing.T) {
	fixedTraceTime := time.Unix(1, 0)
	for _, traceClock := range []struct {
		name string
		now  func() time.Time
	}{
		{name: "real-trace", now: time.Now},
		{name: "fixed-trace", now: func() time.Time { return fixedTraceTime }},
	} {
		for _, operation := range []string{"complete", "abandon"} {
			for _, finalStatus := range []int{200, 400} {
				t.Run(fmt.Sprintf("%s/%d/%s", operation, finalStatus, traceClock.name), func(t *testing.T) {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					var receives, renews, settles int32
					var mu sync.Mutex
					var handlerReturned, settlementEntered, renewStarted, renewClosedAt, releasedAt, renewNotified settlementPhaseEvent
					var sequence uint64
					// All event counters and observations use this same mutex. The clock
					// is diagnostic only: Windows can give adjacent events equal ticks.
					recordLocked := func() settlementPhaseEvent {
						sequence++
						return settlementPhaseEvent{sequence: sequence, at: traceClock.now()}
					}
					var observed []error
					renewalClosed := make(chan struct{})
					var closeRenew sync.Once
					terminalPath := wire.PathComplete
					if operation == "abandon" {
						terminalPath = wire.PathAbandon
					}
					stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
						_, _ = ioutil.ReadAll(request.Body)
						w.Header().Set("Content-Type", "application/json")
						w.Header().Set(wire.ServerTimeHeader, strconv.FormatInt(time.Now().UnixNano()/int64(time.Millisecond), 10))
						switch request.URL.Path {
						case wire.PathReceive:
							if atomic.AddInt32(&receives, 1) == 1 {
								_, _ = fmt.Fprintf(w, `{"messages":[{"seq_number":7,"lock_token":"lock-7","locked_until_ms":%d}]}`, time.Now().Add(time.Second).UnixNano()/int64(time.Millisecond))
							} else {
								select {
								case <-ctx.Done():
								case <-request.Context().Done():
								}
							}
						case terminalPath:
							atomic.AddInt32(&settles, 1)
							mu.Lock()
							settlementEntered = recordLocked()
							mu.Unlock()
							select {
							case <-renewalClosed:
							case <-ctx.Done():
								return
							}
							timer := time.NewTimer(200 * time.Millisecond)
							defer timer.Stop()
							select {
							case <-timer.C:
							case <-ctx.Done():
								return
							}
							mu.Lock()
							releasedAt = recordLocked()
							mu.Unlock()
							w.WriteHeader(finalStatus)
							if finalStatus == 200 {
								_, _ = io.WriteString(w, `{"ok":true}`)
							} else {
								_, _ = io.WriteString(w, `{"code":"invalid_argument","message":"settlement-control"}`)
							}
						case wire.PathRenewBatch:
							atomic.AddInt32(&renews, 1)
							w.WriteHeader(400)
							_, _ = io.WriteString(w, `{"code":"invalid_argument","message":"renewal-control"}`)
						default:
							t.Errorf("unexpected route %s", request.URL.Path)
							w.WriteHeader(400)
						}
					}))
					defer stub.Close()
					base := http.DefaultTransport.(*http.Transport).Clone()
					defer base.CloseIdleConnections()
					transport := acknowledgementRoundTripper(func(request *http.Request) (*http.Response, error) {
						if request.URL.Path == wire.PathRenewBatch {
							mu.Lock()
							renewStarted = recordLocked()
							mu.Unlock()
						}
						response, err := base.RoundTrip(request)
						if err != nil {
							return response, err
						}
						path := request.URL.Path
						response.Body = &settlementObservedResponse{ReadCloser: response.Body, closed: func(eof bool) {
							if !eof {
								return
							}
							if path == wire.PathRenewBatch {
								mu.Lock()
								renewClosedAt = recordLocked()
								mu.Unlock()
								closeRenew.Do(func() { close(renewalClosed) })
							}
							if path == terminalPath {
								cancel()
							}
						}}
						return response, nil
					})
					client, err := Open(ctx, stub.URL, WithHTTPClient(&http.Client{Transport: transport}))
					if err != nil {
						t.Fatal(err)
					}
					defer client.Close()
					err = client.Receiver("q", WithAutoRenew(), WithErrorHandler(func(err error) {
						mu.Lock()
						observed = append(observed, err)
						if strings.Contains(err.Error(), "renewal-control") {
							renewNotified = recordLocked()
						}
						mu.Unlock()
					})).Run(ctx, func(context.Context, *Message) error {
						mu.Lock()
						handlerReturned = recordLocked()
						mu.Unlock()
						if operation == "abandon" {
							return errors.New("handler failed")
						}
						return nil
					})
					if !errors.Is(err, context.Canceled) {
						t.Fatalf("Run=%v, want joined final-response cancellation", err)
					}
					mu.Lock()
					defer mu.Unlock()
					// The response-close channel and unchanged 200ms timer establish
					// the hold. Strict event order must not depend on clock resolution.
					if !settlementPhaseOrdered(handlerReturned, settlementEntered, renewStarted, renewClosedAt, releasedAt) {
						t.Fatalf("settlement phase not established: handler=%s settle=%s renew=%s closed=%s released=%s", handlerReturned, settlementEntered, renewStarted, renewClosedAt, releasedAt)
					}
					if !settlementPhaseOrdered(renewClosedAt, renewNotified, releasedAt) {
						t.Fatalf("renewal400 was not reported while settlement remained pending: notification=%s closed=%s released=%s", renewNotified, renewClosedAt, releasedAt)
					}
					if traceClock.name == "fixed-trace" {
						for _, event := range []settlementPhaseEvent{handlerReturned, settlementEntered, renewStarted, renewClosedAt, renewNotified, releasedAt} {
							if !event.at.Equal(fixedTraceTime) {
								t.Fatalf("trace-only fixed clock was not used: %s", event)
							}
						}
						t.Logf("identical trace timestamps preserve strict sequence: %s %s %s %s %s %s", handlerReturned, settlementEntered, renewStarted, renewClosedAt, renewNotified, releasedAt)
					}
					if atomic.LoadInt32(&renews) != 1 || atomic.LoadInt32(&settles) != 1 {
						t.Fatalf("renew/terminal calls=%d/%d, want one of each", renews, settles)
					}
					want := 1
					if finalStatus == 400 {
						want = 2
					}
					if len(observed) != want {
						t.Fatalf("observed=%v, want independent renewal and final settlement results", observed)
					}
					for _, failure := range observed {
						var he *HTTPError
						if !errors.Is(failure, ErrInvalidArgument) || errors.Is(failure, ErrLockLost) || errors.Is(failure, ErrOutcomeUnknown) || !errors.As(failure, &he) || he.StatusCode != 400 {
							t.Fatalf("definitive error category lost: %v", failure)
						}
					}
					if finalStatus == 400 && !strings.Contains(observed[1].Error(), "settlement-control") {
						t.Fatalf("final settlement cause lost: %v", observed)
					}
				})
			}
		}
	}
}

// settlementPhaseEvent belongs only to this observer. SDK and broker clocks,
// deadlines and timers always keep their ordinary time.Now behavior.
type settlementPhaseEvent struct {
	sequence uint64
	at       time.Time
}

func (e settlementPhaseEvent) String() string {
	return fmt.Sprintf("%d@%s", e.sequence, e.at)
}

func settlementPhaseOrdered(events ...settlementPhaseEvent) bool {
	var previous uint64
	for _, event := range events {
		if event.sequence <= previous {
			return false
		}
		previous = event.sequence
	}
	return len(events) != 0
}

func TestSettlementPhaseOrderRejectsMissingOrReversedEvents(t *testing.T) {
	for _, clock := range []string{"fixed", "quantized"} {
		t.Run(clock, func(t *testing.T) {
			good := make([]settlementPhaseEvent, 6)
			for i := range good {
				stamp := time.Unix(1, 0)
				if clock == "quantized" {
					stamp = stamp.Add(time.Duration(i/2) * time.Second)
				}
				good[i] = settlementPhaseEvent{sequence: uint64(i + 1), at: stamp}
			}
			if !settlementPhaseOrdered(good...) {
				t.Fatal("coarse diagnostic timestamps must not reject ordered events")
			}
			for _, fault := range []string{"missing", "duplicate", "reversed"} {
				bad := append([]settlementPhaseEvent(nil), good...)
				switch fault {
				case "missing":
					bad[4].sequence = 0
				case "duplicate":
					bad[4].sequence = bad[3].sequence
				case "reversed":
					bad[3], bad[4] = bad[4], bad[3]
				}
				if settlementPhaseOrdered(bad...) {
					t.Fatalf("%s event must fail despite equal or coarse timestamps", fault)
				}
			}
		})
	}
}

type settlementObservedResponse struct {
	io.ReadCloser
	eof    bool
	once   sync.Once
	closed func(bool)
}

func (b *settlementObservedResponse) Read(dst []byte) (int, error) {
	n, err := b.ReadCloser.Read(dst)
	if err == io.EOF {
		b.eof = true
	}
	return n, err
}
func (b *settlementObservedResponse) Close() error {
	err := b.ReadCloser.Close()
	b.once.Do(func() { b.closed(b.eof && err == nil) })
	return err
}
