package mqlite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strconv"
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
	renewLeaseFn   func(context.Context, *Message) (time.Time, error)
	renewalLeaseFn func(context.Context, *Message) (renewalLease, error)
}

func (s *deadlineLeaseSource) renewLease(ctx context.Context, message *Message) (renewalLease, error) {
	if s.renewalLeaseFn != nil {
		return s.renewalLeaseFn(ctx, message)
	}
	deadline, err := s.renewLeaseFn(ctx, message)
	return renewalLease{
		deadline:          deadline,
		responseRTT:       message.leaseResponseRTT,
		clockResolution:   message.leaseClockResolution,
		clockUnavailable:  message.leaseClockUnavailable,
		deadlineUntrusted: deadline.IsZero(),
	}, err
}

func (s *concurrentLeaseSource) renewLease(context.Context, *Message) (renewalLease, error) {
	if atomic.AddInt32(&s.calls, 1) == 2 {
		close(s.both)
	}
	<-s.release
	return renewalLease{deadline: time.Now().Add(time.Second)}, nil
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
	receiver.receiveJitter = func(delay time.Duration) time.Duration { return delay }
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
	if len(settles) != 2 || settles[0].op != "renew" || settles[1].op != "complete" {
		t.Fatalf("settlements = %+v, want replay fence then completion", settles)
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
	receiver.receiveJitter = func(delay time.Duration) time.Duration { return delay }
	receiver.receiveWait = func(ctx context.Context, delay time.Duration) bool {
		return ctx.Err() == nil
	}
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

func TestReceiverRetryUsesBoundedDelayVectorAndOneAttemptID(t *testing.T) {
	permanent := &HTTPError{StatusCode: http.StatusForbidden, cause: ErrPermissionDenied}
	source := &fakeReceiveSource{}
	source.receiveFn = func(context.Context, int, receiveRecord) ([]*Message, error) {
		if len(source.receiveSnapshot()) <= 6 {
			return nil, errors.New("lost receive response")
		}
		return nil, permanent
	}
	receiver := newReceiver(source, "jobs", nil)
	var generated []time.Duration
	receiver.receiveJitter = func(delay time.Duration) time.Duration {
		generated = append(generated, delay)
		return delay
	}
	var waits []time.Duration
	receiver.receiveWait = func(ctx context.Context, delay time.Duration) bool {
		waits = append(waits, delay)
		return ctx.Err() == nil
	}

	err := receiver.Run(context.Background(), func(context.Context, *Message) error { return nil })
	if err != permanent {
		t.Fatalf("Run error = %v, want %v", err, permanent)
	}
	wantWaits := []time.Duration{0, 500 * time.Millisecond, time.Second, 2 * time.Second, 3 * time.Second, 3 * time.Second}
	if !equalDurations(waits, wantWaits) {
		t.Fatalf("retry waits = %v, want %v", waits, wantWaits)
	}
	wantGenerated := []time.Duration{500 * time.Millisecond, time.Second, 2 * time.Second, 3 * time.Second, 3 * time.Second}
	if !equalDurations(generated, wantGenerated) {
		t.Fatalf("generated delays = %v, want %v", generated, wantGenerated)
	}
	receives := source.receiveSnapshot()
	if len(receives) != 7 {
		t.Fatalf("receive calls = %+v, want six transient failures and one permanent failure", receives)
	}
	for i := 1; i < len(receives); i++ {
		if receives[i].attemptID != receives[0].attemptID {
			t.Fatalf("AttemptID changed at call %d: %+v", i+1, receives)
		}
	}
}

func TestReceiverRetryUsesOnlyValid429And503Hints(t *testing.T) {
	permanent := &HTTPError{StatusCode: http.StatusForbidden, cause: ErrPermissionDenied}
	errorsByCall := []error{
		&HTTPError{StatusCode: http.StatusServiceUnavailable, cause: errors.New("draining"), retryAfter: 8 * time.Second, retryAfterOK: true},
		&HTTPError{StatusCode: http.StatusServiceUnavailable, cause: errors.New("retry now"), retryAfterOK: true},
		&HTTPError{StatusCode: http.StatusTooManyRequests, cause: errors.New("rate limited"), retryAfter: 1500 * time.Millisecond, retryAfterOK: true},
		&HTTPError{StatusCode: http.StatusInternalServerError, cause: errors.New("server error"), retryAfter: 9 * time.Second, retryAfterOK: true},
		&HTTPError{StatusCode: http.StatusServiceUnavailable, cause: errors.New("malformed hint"), retryAfter: 9 * time.Second},
		permanent,
	}
	source := &fakeReceiveSource{}
	source.receiveFn = func(context.Context, int, receiveRecord) ([]*Message, error) {
		return nil, errorsByCall[len(source.receiveSnapshot())-1]
	}
	receiver := newReceiver(source, "jobs", nil)
	receiver.receiveJitter = func(delay time.Duration) time.Duration { return delay }
	var waits []time.Duration
	receiver.receiveWait = func(ctx context.Context, delay time.Duration) bool {
		waits = append(waits, delay)
		return ctx.Err() == nil
	}

	err := receiver.Run(context.Background(), func(context.Context, *Message) error { return nil })
	if err != permanent {
		t.Fatalf("Run error = %v, want %v", err, permanent)
	}
	want := []time.Duration{8 * time.Second, 500 * time.Millisecond, 1500 * time.Millisecond, 2 * time.Second, 3 * time.Second}
	if !equalDurations(waits, want) {
		t.Fatalf("retry waits = %v, want %v", waits, want)
	}
}

func TestReceiverRetryWaitStopsOnContextCancellation(t *testing.T) {
	source := &fakeReceiveSource{}
	source.receiveFn = func(context.Context, int, receiveRecord) ([]*Message, error) {
		if len(source.receiveSnapshot()) == 1 {
			return nil, errors.New("lost receive response")
		}
		return nil, &HTTPError{
			StatusCode:   http.StatusServiceUnavailable,
			cause:        errors.New("broker is draining"),
			retryAfter:   time.Hour,
			retryAfterOK: true,
		}
	}
	backingOff := make(chan struct{}, 1)
	receiver := newReceiver(source, "jobs", []ReceiverOption{WithErrorHandler(func(error) {
		backingOff <- struct{}{}
	})})
	receiver.receiveJitter = func(delay time.Duration) time.Duration { return delay }
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- receiver.Run(ctx, func(context.Context, *Message) error { return nil })
	}()
	select {
	case <-backingOff:
		cancel()
	case <-time.After(time.Second):
		cancel()
		t.Fatal("Receiver did not enter retry backoff")
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run error = %v, want context.Canceled", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("context cancellation did not interrupt Receive retry wait")
	}
	if receives := source.receiveSnapshot(); len(receives) != 2 {
		t.Fatalf("receive calls = %+v, want immediate replay then canceled wait", receives)
	}
}

func TestReceiverSuccessStartsFreshRetryRound(t *testing.T) {
	permanent := &HTTPError{StatusCode: http.StatusForbidden, cause: ErrPermissionDenied}
	source := &fakeReceiveSource{}
	source.receiveFn = func(context.Context, int, receiveRecord) ([]*Message, error) {
		switch len(source.receiveSnapshot()) {
		case 1, 3:
			return nil, errors.New("lost receive response")
		case 2:
			return nil, nil
		default:
			return nil, permanent
		}
	}
	receiver := newReceiver(source, "jobs", nil)
	receiver.receiveJitter = func(delay time.Duration) time.Duration { return delay }
	var waits []time.Duration
	receiver.receiveWait = func(ctx context.Context, delay time.Duration) bool {
		waits = append(waits, delay)
		return ctx.Err() == nil
	}

	err := receiver.Run(context.Background(), func(context.Context, *Message) error { return nil })
	if err != permanent {
		t.Fatalf("Run error = %v, want %v", err, permanent)
	}
	if want := []time.Duration{0, 0}; !equalDurations(waits, want) {
		t.Fatalf("retry waits = %v, want %v after success reset", waits, want)
	}
	receives := source.receiveSnapshot()
	if len(receives) != 4 || receives[0].attemptID != receives[1].attemptID || receives[2].attemptID != receives[3].attemptID {
		t.Fatalf("receive attempts = %+v, want one ID per retry round", receives)
	}
	if receives[0].attemptID == receives[2].attemptID {
		t.Fatalf("successful round reused AttemptID %q", receives[0].attemptID)
	}
}

func equalDurations(got, want []time.Duration) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func TestReceiverRetriesBrokerCanceledReceiveWithSameAttempt(t *testing.T) {
	source := &fakeReceiveSource{}
	source.receiveFn = func(ctx context.Context, call int, _ receiveRecord) ([]*Message, error) {
		switch call {
		case 1:
			return nil, &HTTPError{StatusCode: 499, cause: context.Canceled}
		case 2:
			return []*Message{source.message(1)}, nil
		default:
			<-ctx.Done()
			return nil, ctx.Err()
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	receiver := newReceiver(source, "jobs", nil)
	receiver.receiveJitter = func(delay time.Duration) time.Duration { return delay }
	err := receiver.Run(ctx, func(context.Context, *Message) error {
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v, want context.Canceled after test shutdown", err)
	}
	receives := source.receiveSnapshot()
	if len(receives) != 2 {
		t.Fatalf("receive calls = %+v, want shutdown response then successful replay", receives)
	}
	if receives[0].attemptID == "" || receives[0].attemptID != receives[1].attemptID {
		t.Fatalf("attempt IDs = %q / %q, want one exact retry", receives[0].attemptID, receives[1].attemptID)
	}
	settles := source.settleSnapshot()
	if len(settles) != 2 || settles[0].op != "renew" || settles[1].op != "complete" {
		t.Fatalf("settlements = %+v, want replay fence then completion", settles)
	}
}

func TestReceiverSurvivesHTTPDrainAndRestartWithSameAttempt(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	firstAttempt := make(chan string, 1)
	firstServer := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != wire.PathReceive {
			http.NotFound(w, req)
			return
		}
		var receive wire.ReceiveRequest
		if err := json.NewDecoder(req.Body).Decode(&receive); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Connection", "close")
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(wire.ErrorBody{Code: "unavailable", Message: "broker is draining"})
		_ = listener.Close()
		firstAttempt <- receive.AttemptID
	})}
	firstDone := make(chan error, 1)
	go func() { firstDone <- firstServer.Serve(listener) }()
	defer firstServer.Close()

	client, err := Open(context.Background(), "http://"+address)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	retryObserved := make(chan error, 1)
	receiver := client.Receiver("jobs", WithErrorHandler(func(err error) {
		select {
		case retryObserved <- err:
		default:
		}
	}))
	receiver.receiveJitter = func(delay time.Duration) time.Duration { return delay }
	requestedRetryDelay := make(chan time.Duration, 1)
	var recordFirstWait sync.Once
	receiver.receiveWait = func(ctx context.Context, delay time.Duration) bool {
		recordFirstWait.Do(func() { requestedRetryDelay <- delay })
		if delay > 5*time.Millisecond {
			delay = 5 * time.Millisecond
		}
		return waitContext(ctx, delay)
	}
	var handlerCalls int32
	runDone := make(chan error, 1)
	go func() {
		runDone <- receiver.Run(ctx, func(_ context.Context, message *Message) error {
			atomic.AddInt32(&handlerCalls, 1)
			if string(message.Body) != "after-restart" {
				t.Errorf("handler body = %q, want after-restart", message.Body)
			}
			return nil
		})
	}()

	var attemptBeforeRestart string
	select {
	case attemptBeforeRestart = <-firstAttempt:
	case <-time.After(2 * time.Second):
		t.Fatal("Receiver did not reach the draining broker")
	}
	if attemptBeforeRestart == "" {
		t.Fatal("draining Receive omitted receive_attempt_id")
	}
	select {
	case <-firstDone:
	case <-time.After(2 * time.Second):
		t.Fatal("first HTTP listener did not stop")
	}
	// Assert the unscaled policy delay before the test seam shortens its physical
	// wait. The broker's first 503 carries Retry-After: 1, so it overrides the
	// otherwise immediate first replay.
	select {
	case delay := <-requestedRetryDelay:
		if delay != time.Second {
			t.Fatalf("first retry delay = %s, want 1s from Retry-After", delay)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Receiver did not schedule the draining broker retry")
	}
	// Wait for the following connection refusal so this covers an actual broker
	// gap, not two handlers in one process.
	select {
	case <-retryObserved:
	case <-time.After(2 * time.Second):
		t.Fatal("Receiver did not survive the between-process connection failure")
	}

	listener, err = net.Listen("tcp", address)
	if err != nil {
		t.Fatalf("restart listener on %s: %v", address, err)
	}
	replayedAttempt := make(chan string, 1)
	completed := make(chan struct{}, 1)
	var receiveCalls int32
	secondServer := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		now := time.Now()
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set(wire.ServerTimeHeader, strconv.FormatInt(now.UnixNano()/int64(time.Millisecond), 10))
		switch req.URL.Path {
		case wire.PathReceive:
			var receive wire.ReceiveRequest
			if err := json.NewDecoder(req.Body).Decode(&receive); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if atomic.AddInt32(&receiveCalls, 1) != 1 {
				<-req.Context().Done()
				return
			}
			replayedAttempt <- receive.AttemptID
			_ = json.NewEncoder(w).Encode(wire.ReceiveResponse{Messages: []wire.Message{{
				SeqNumber: 1, LockToken: "lock-1", DeliveryCount: 1, Body: []byte("after-restart"),
				LockedUntilMs: now.Add(5*time.Second).UnixNano() / int64(time.Millisecond),
			}}})
		case wire.PathRenewBatch:
			_ = json.NewEncoder(w).Encode(wire.RenewBatchResponse{Results: []wire.SettleItemResult{{
				SeqNumber: 1, Ok: true,
				LockedUntilMs: now.Add(5*time.Second).UnixNano() / int64(time.Millisecond),
			}}})
		case wire.PathComplete:
			_ = json.NewEncoder(w).Encode(wire.SettleResponse{Ok: true})
			select {
			case completed <- struct{}{}:
			default:
			}
		default:
			http.NotFound(w, req)
		}
	})}
	secondDone := make(chan error, 1)
	go func() { secondDone <- secondServer.Serve(listener) }()
	defer secondServer.Close()

	select {
	case got := <-replayedAttempt:
		if got != attemptBeforeRestart {
			t.Fatalf("AttemptID changed across drain/restart: %q -> %q", attemptBeforeRestart, got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Receiver did not reconnect after broker restart")
	}
	select {
	case <-completed:
		cancel()
	case <-time.After(2 * time.Second):
		t.Fatal("Receiver did not complete the replayed message")
	}
	select {
	case err := <-runDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run error = %v, want context.Canceled after test shutdown", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Receiver did not stop after test cancellation")
	}
	if got := atomic.LoadInt32(&handlerCalls); got != 1 {
		t.Fatalf("handler calls = %d, want one", got)
	}
	if err := secondServer.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-secondDone:
	case <-time.After(2 * time.Second):
		t.Fatal("restarted HTTP server did not stop")
	}
}

func TestReceiverDoesNotRetryBrokerCanceledReceiveAfterCallerCancellation(t *testing.T) {
	source := &fakeReceiveSource{}
	ctx, cancel := context.WithCancel(context.Background())
	source.receiveFn = func(context.Context, int, receiveRecord) ([]*Message, error) {
		cancel()
		return nil, &HTTPError{StatusCode: 499, cause: context.Canceled}
	}
	receiver := newReceiver(source, "jobs", nil)
	var handlerCalls int32
	err := receiver.Run(ctx, func(context.Context, *Message) error {
		atomic.AddInt32(&handlerCalls, 1)
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v, want caller context cancellation", err)
	}
	if receives := source.receiveSnapshot(); len(receives) != 1 {
		t.Fatalf("receive calls = %+v, want no retry after caller cancellation", receives)
	}
	if got := atomic.LoadInt32(&handlerCalls); got != 0 {
		t.Fatalf("handler calls = %d, want none after caller cancellation", got)
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

func TestReceiverFencesStaleReceiveReplayBeforeDispatch(t *testing.T) {
	tests := []struct {
		name           string
		deadline       func() time.Time
		wantSequence   int64
		wantReceives   int
		wantErrors     int32
		wantOperations []string
	}{
		{name: "zero deadline", deadline: func() time.Time { return time.Time{} }, wantSequence: 2, wantReceives: 3, wantErrors: 1, wantOperations: []string{"complete"}},
		{name: "expired deadline", deadline: func() time.Time { return time.Now().Add(-time.Second) }, wantSequence: 1, wantReceives: 2, wantOperations: []string{"renew", "complete"}},
		{name: "inside safety window", deadline: func() time.Time { return time.Now().Add(5 * time.Millisecond) }, wantSequence: 1, wantReceives: 2, wantOperations: []string{"renew", "complete"}},
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
			if got := atomic.LoadInt64(&handled); got != test.wantSequence {
				t.Fatalf("handled sequence = %d, want %d", got, test.wantSequence)
			}
			receives := source.receiveSnapshot()
			if len(receives) != test.wantReceives {
				t.Fatalf("receive calls = %+v, want %d", receives, test.wantReceives)
			}
			if receives[0].attemptID == "" || receives[0].attemptID != receives[1].attemptID {
				t.Fatalf("replay attempt IDs = %q / %q, want equal non-empty IDs", receives[0].attemptID, receives[1].attemptID)
			}
			if len(receives) > 2 && receives[2].attemptID == receives[0].attemptID {
				t.Fatalf("fresh receive reused stale attempt ID %q", receives[2].attemptID)
			}
			if got := atomic.LoadInt32(&observed); got != test.wantErrors {
				t.Fatalf("error notifications = %d, want %d", got, test.wantErrors)
			}
			settles := source.settleSnapshot()
			if len(settles) != len(test.wantOperations) {
				t.Fatalf("settlements = %+v, want operations %v", settles, test.wantOperations)
			}
			for i, operation := range test.wantOperations {
				if settles[i].op != operation || settles[i].seq != test.wantSequence {
					t.Fatalf("settlements = %+v, want operations %v for sequence %d", settles, test.wantOperations, test.wantSequence)
				}
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
			message.LockedUntil = time.Now().Add(120 * time.Millisecond)
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
			// Keep this lifecycle test above the pathological short-lease guard;
			// dedicated coverage below verifies that sub-60ms exact leases fail closed.
			message.LockedUntil = time.Now().Add(120 * time.Millisecond)
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
			source.renewalLeaseFn = func(context.Context, *Message) (renewalLease, error) {
				if atomic.AddInt32(&renewCalls, 1) == 2 {
					close(renewedAfterDispatch)
				}
				// Renewal committed at the broker, but its absolute deadline
				// remains in the past according to the client's fast clock.
				return renewalLease{deadline: time.Now().Add(-time.Second), clockResolution: time.Second}, nil
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

func TestReceiverUntrustedLeaseAllowsNetworkRTTAboveCadence(t *testing.T) {
	base := &fakeReceiveSource{}
	message := base.message(52)
	message.LockedUntil = time.Now().Add(-time.Second)
	var renewCalls int32
	source := &deadlineLeaseSource{fakeReceiveSource: base}
	source.renewalLeaseFn = func(ctx context.Context, _ *Message) (renewalLease, error) {
		atomic.AddInt32(&renewCalls, 1)
		timer := time.NewTimer(150 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return renewalLease{}, ctx.Err()
		case <-timer.C:
			// The broker renews successfully, but its absolute deadline remains
			// unusable according to the client's fast wall clock.
			return renewalLease{deadline: time.Now().Add(-time.Second), clockResolution: time.Second}, nil
		}
	}
	var observed int32
	receiver := newReceiver(source, "jobs", []ReceiverOption{
		WithAutoRenew(),
		WithErrorHandler(func(error) { atomic.AddInt32(&observed, 1) }),
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	handlerCompleted := make(chan struct{})
	receiver.process(ctx, message, func(handlerCtx context.Context, _ *Message) error {
		timer := time.NewTimer(500 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-handlerCtx.Done():
			return handlerCtx.Err()
		case <-timer.C:
			close(handlerCompleted)
			return nil
		}
	}, func(error) { t.Error("transient skew renewal must not stop the receiver") })

	select {
	case <-handlerCompleted:
	default:
		t.Fatal("ordinary network RTT canceled the handler")
	}
	if got := atomic.LoadInt32(&renewCalls); got < 2 {
		t.Fatalf("renew calls = %d, want initial confirmation plus managed renewal", got)
	}
	if got := atomic.LoadInt32(&observed); got != 0 {
		t.Fatalf("error notifications = %d, want 0", got)
	}
	settles := base.settleSnapshot()
	if len(settles) != 1 || settles[0].op != "complete" || settles[0].seq != 52 {
		t.Fatalf("settlements = %+v, want completed message", settles)
	}
}

func TestReceiverUntrustedLeaseRenewalDoesNotSpin(t *testing.T) {
	base := &fakeReceiveSource{}
	message := base.message(54)
	message.LockedUntil = time.Now().Add(-time.Second)
	var renewCalls int32
	source := &deadlineLeaseSource{fakeReceiveSource: base}
	source.renewalLeaseFn = func(context.Context, *Message) (renewalLease, error) {
		atomic.AddInt32(&renewCalls, 1)
		return renewalLease{deadline: time.Now().Add(-time.Second), clockResolution: time.Second}, nil
	}
	receiver := newReceiver(source, "jobs", []ReceiverOption{WithAutoRenew()})
	receiver.process(context.Background(), message, func(context.Context, *Message) error {
		time.Sleep(800 * time.Millisecond)
		return nil
	}, func(error) { t.Error("untrusted renewal must not stop the receiver") })
	if got := atomic.LoadInt32(&renewCalls); got < 2 || got > 4 {
		t.Fatalf("renew calls = %d, want initial confirmation and at most three paced renewals", got)
	}
	settles := base.settleSnapshot()
	if len(settles) != 1 || settles[0].op != "complete" {
		t.Fatalf("settlements = %+v, want completion", settles)
	}
}

func TestReceiverSubsecondTrustedLeaseRenewsBeforeDeadline(t *testing.T) {
	var renewCalls int32
	var receiveCalls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		now := time.Now()
		w.Header().Set(wire.ServerTimeHeader, strconv.FormatInt(now.UnixNano()/int64(time.Millisecond), 10))
		switch req.URL.Path {
		case wire.PathReceive:
			if atomic.AddInt32(&receiveCalls, 1) > 1 {
				<-req.Context().Done()
				return
			}
			_ = json.NewEncoder(w).Encode(wire.ReceiveResponse{Messages: []wire.Message{{
				SeqNumber: 55, LockToken: "lock-55",
				LockedUntilMs: now.Add(120*time.Millisecond).UnixNano() / int64(time.Millisecond),
			}}})
		case wire.PathRenewBatch:
			atomic.AddInt32(&renewCalls, 1)
			_ = json.NewEncoder(w).Encode(wire.RenewBatchResponse{Results: []wire.SettleItemResult{{
				SeqNumber: 55, Ok: true,
				LockedUntilMs: now.Add(120*time.Millisecond).UnixNano() / int64(time.Millisecond),
			}}})
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
	defer client.Close()
	ctx, cancel := context.WithCancel(context.Background())
	receiver := client.Receiver("jobs", WithAutoRenew(), WithErrorHandler(func(err error) {
		t.Errorf("short trusted lease renewal: %v", err)
	}))
	err = receiver.Run(ctx, func(context.Context, *Message) error {
		time.Sleep(120 * time.Millisecond)
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v, want context.Canceled", err)
	}
	if got := atomic.LoadInt32(&renewCalls); got < 2 || got > 20 {
		t.Fatalf("renew calls = %d, want repeated bounded renewal for a real 40ms lease", got)
	}
}

func TestReceiverBoundsInitialRenewalAttempt(t *testing.T) {
	base := &fakeReceiveSource{}
	message := base.message(53)
	message.LockedUntil = time.Now().Add(-time.Second)
	renewCanceled := make(chan struct{})
	source := &deadlineLeaseSource{fakeReceiveSource: base}
	source.renewLeaseFn = func(ctx context.Context, _ *Message) (time.Time, error) {
		<-ctx.Done()
		close(renewCanceled)
		return time.Time{}, ctx.Err()
	}
	observed := make(chan error, 1)
	receiver := newReceiver(source, "jobs", []ReceiverOption{
		WithAutoRenew(),
		WithErrorHandler(func(err error) { observed <- err }),
	})
	receiver.renewAttemptTimeout = 50 * time.Millisecond
	var handlerCalls int32
	done := make(chan struct{})
	go func() {
		receiver.process(context.Background(), message, func(context.Context, *Message) error {
			atomic.AddInt32(&handlerCalls, 1)
			return nil
		}, func(error) { t.Error("initial renewal timeout must not stop the receiver") })
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("initial renewal outlived its watchdog")
	}
	select {
	case <-renewCanceled:
	default:
		t.Fatal("initial renewal context was not canceled")
	}
	if got := atomic.LoadInt32(&handlerCalls); got != 0 {
		t.Fatalf("handler calls = %d, want 0", got)
	}
	select {
	case err := <-observed:
		if !errors.Is(err, ErrLockLost) {
			t.Fatalf("error notification = %v, want ErrLockLost", err)
		}
	default:
		t.Fatal("initial renewal timeout was not reported")
	}
}

func TestReceiverReplayInitialRenewalUsesMappedLeaseBudget(t *testing.T) {
	base := &fakeReceiveSource{}
	message := base.message(56)
	message.LockedUntil = time.Now().Add(10 * time.Second)
	message.localLeaseDeadline = time.Now().Add(10 * time.Second)
	source := &deadlineLeaseSource{fakeReceiveSource: base}
	source.renewLeaseFn = func(ctx context.Context, _ *Message) (time.Time, error) {
		timer := time.NewTimer(1100 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return time.Time{}, ctx.Err()
		case <-timer.C:
			return time.Now().Add(10 * time.Second), nil
		}
	}
	receiver := newReceiver(source, "jobs", nil)
	var handlerCalls int32
	receiver.processWithHandlerRelease(context.Background(), message, func(context.Context, *Message) error {
		atomic.AddInt32(&handlerCalls, 1)
		return nil
	}, func() {}, func(error) { t.Error("successful replay fence must not stop the receiver") }, true)
	if got := atomic.LoadInt32(&handlerCalls); got != 1 {
		t.Fatalf("handler calls = %d, want 1 after slow replay fence", got)
	}
	settles := base.settleSnapshot()
	if len(settles) != 1 || settles[0].op != "complete" {
		t.Fatalf("settlements = %+v, want completion", settles)
	}
}

func TestReceiverCapsMappedInitialRenewalBudget(t *testing.T) {
	receiver := newReceiver(&fakeReceiveSource{}, "jobs", nil)
	ctx, cancel := receiver.initialRenewalAttemptContext(context.Background(), time.Now().Add(time.Hour))
	defer cancel()
	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("initial renewal context has no deadline")
	}
	remaining := time.Until(deadline)
	if remaining < maxInitialRenewAttemptTimeout-100*time.Millisecond || remaining > maxInitialRenewAttemptTimeout {
		t.Fatalf("initial renewal budget = %s, want at most %s", remaining, maxInitialRenewAttemptTimeout)
	}
}

func TestReceiverLongPollLowerBoundStillAllowsInitialRenewal(t *testing.T) {
	base := &fakeReceiveSource{}
	message := base.message(60)
	message.localLeaseDeadline = time.Now().Add(-time.Millisecond)
	// The complete long-poll RTT is charged against the lease. A message that
	// arrived near the end of an 8s wait with a fresh 5s broker lock can therefore
	// have an already-expired conservative lower bound on receipt.
	message.LockedUntil = time.Now().Add(5 * time.Second)
	message.leaseResponseRTT = 8 * time.Second
	message.leaseClockResolution = time.Millisecond
	var renewCalls int32
	source := &deadlineLeaseSource{fakeReceiveSource: base}
	source.renewalLeaseFn = func(ctx context.Context, _ *Message) (renewalLease, error) {
		atomic.AddInt32(&renewCalls, 1)
		if err := ctx.Err(); err != nil {
			return renewalLease{}, fmt.Errorf("initial renewal was canceled before dispatch: %v", err)
		}
		return renewalLease{deadline: time.Now().Add(5 * time.Second), clockResolution: time.Millisecond}, nil
	}
	receiver := newReceiver(source, "jobs", []ReceiverOption{
		WithAutoRenew(),
		WithErrorHandler(func(err error) { t.Errorf("initial renewal diagnostic = %v", err) }),
	})
	receiver.renewAttemptTimeout = 200 * time.Millisecond
	var handlerCalls int32
	receiver.process(context.Background(), message, func(context.Context, *Message) error {
		atomic.AddInt32(&handlerCalls, 1)
		return nil
	}, func(error) { t.Error("bounded long-poll confirmation must remain message-scoped") })
	if got := atomic.LoadInt32(&renewCalls); got != 1 {
		t.Fatalf("renew calls = %d, want one bounded initial confirmation", got)
	}
	if got := atomic.LoadInt32(&handlerCalls); got != 1 {
		t.Fatalf("handler calls = %d, want one after confirmed fresh broker lease", got)
	}
	settles := base.settleSnapshot()
	if len(settles) != 1 || settles[0].op != "complete" {
		t.Fatalf("settlements = %+v, want completion", settles)
	}
}

func TestReceiverMappedHighRTTRenewalCannotOutliveLeaseHorizon(t *testing.T) {
	base := &fakeReceiveSource{}
	message := base.message(57)
	message.localLeaseDeadline = time.Now().Add(250 * time.Millisecond)
	message.LockedUntil = message.localLeaseDeadline
	message.leaseResponseRTT = 200 * time.Millisecond
	message.leaseClockResolution = time.Millisecond
	renewCanceled := make(chan struct{})
	source := &deadlineLeaseSource{fakeReceiveSource: base}
	source.renewLeaseFn = func(ctx context.Context, _ *Message) (time.Time, error) {
		<-ctx.Done()
		close(renewCanceled)
		return time.Time{}, ctx.Err()
	}
	observed := make(chan error, 1)
	receiver := newReceiver(source, "jobs", []ReceiverOption{
		WithAutoRenew(),
		WithErrorHandler(func(err error) { observed <- err }),
	})
	started := time.Now()
	var handlerCalls int32
	receiver.process(context.Background(), message, func(context.Context, *Message) error {
		atomic.AddInt32(&handlerCalls, 1)
		return nil
	}, func(error) { t.Error("lease loss must remain message-scoped") })
	elapsed := time.Since(started)
	if elapsed > 500*time.Millisecond {
		t.Fatalf("mapped renewal ran for %s, want the 250ms lease horizon rather than the 1s fallback", elapsed)
	}
	select {
	case <-renewCanceled:
	default:
		t.Fatal("mapped renewal context was not canceled at the lease horizon")
	}
	if got := atomic.LoadInt32(&handlerCalls); got != 0 {
		t.Fatalf("handler calls = %d, want none before initial confirmation", got)
	}
	select {
	case err := <-observed:
		if !errors.Is(err, ErrLockLost) {
			t.Fatalf("diagnostic = %v, want ErrLockLost", err)
		}
	default:
		t.Fatal("lease horizon exhaustion was not reported")
	}
}

func TestReceiverConfirmsMeasuredHighRTTLeaseBeforeDispatch(t *testing.T) {
	base := &fakeReceiveSource{}
	message := base.message(59)
	message.localLeaseDeadline = time.Now().Add(600 * time.Millisecond)
	message.LockedUntil = message.localLeaseDeadline
	message.leaseResponseRTT = 400 * time.Millisecond
	message.leaseClockResolution = time.Millisecond
	var renewCalls int32
	source := &deadlineLeaseSource{fakeReceiveSource: base}
	source.renewLeaseFn = func(ctx context.Context, _ *Message) (time.Time, error) {
		call := atomic.AddInt32(&renewCalls, 1)
		if call > 1 {
			<-ctx.Done()
			return time.Time{}, ctx.Err()
		}
		timer := time.NewTimer(400 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return time.Time{}, ctx.Err()
		case <-timer.C:
			return time.Now().Add(600 * time.Millisecond), nil
		}
	}
	receiver := newReceiver(source, "jobs", []ReceiverOption{
		WithAutoRenew(),
		WithErrorHandler(func(err error) { t.Errorf("high-RTT confirmation failed: %v", err) }),
	})
	var handlerCalls int32
	receiver.process(context.Background(), message, func(context.Context, *Message) error {
		atomic.AddInt32(&handlerCalls, 1)
		return nil
	}, func(error) { t.Error("high-RTT confirmation must remain message-scoped") })
	if got := atomic.LoadInt32(&handlerCalls); got != 1 {
		t.Fatalf("handler calls = %d, want one after early confirmation", got)
	}
	if got := atomic.LoadInt32(&renewCalls); got < 1 || got > 2 {
		t.Fatalf("renew calls = %d, want initial confirmation and at most one canceled background attempt", got)
	}
	settles := base.settleSnapshot()
	if len(settles) != 1 || settles[0].op != "complete" {
		t.Fatalf("settlements = %+v, want completion", settles)
	}
}

func TestReceiverFailsClosedInsteadOfHotLoopingOnPathologicalExactLease(t *testing.T) {
	tests := []struct {
		name      string
		remaining time.Duration
	}{
		{name: "below minimum interval", remaining: 45 * time.Millisecond},
		{name: "inside safety window", remaining: time.Millisecond},
		{name: "already expired", remaining: -time.Millisecond},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			base := &fakeReceiveSource{}
			message := base.message(58)
			message.localLeaseDeadline = time.Now().Add(200 * time.Millisecond)
			message.LockedUntil = message.localLeaseDeadline
			message.leaseResponseRTT = 100 * time.Millisecond
			message.leaseClockResolution = time.Millisecond
			var renewCalls int32
			source := &deadlineLeaseSource{fakeReceiveSource: base}
			source.renewLeaseFn = func(context.Context, *Message) (time.Time, error) {
				atomic.AddInt32(&renewCalls, 1)
				return time.Now().Add(test.remaining), nil
			}
			observed := make(chan error, 1)
			receiver := newReceiver(source, "jobs", []ReceiverOption{
				WithAutoRenew(),
				WithErrorHandler(func(err error) { observed <- err }),
			})
			var handlerCalls int32
			receiver.process(context.Background(), message, func(context.Context, *Message) error {
				atomic.AddInt32(&handlerCalls, 1)
				return nil
			}, func(error) { t.Error("pathological lease must remain message-scoped") })
			if got := atomic.LoadInt32(&renewCalls); got != 1 {
				t.Fatalf("renew calls = %d, want one confirmation rather than a hot loop", got)
			}
			if got := atomic.LoadInt32(&handlerCalls); got != 0 {
				t.Fatalf("handler calls = %d, want fail-closed before dispatch", got)
			}
			select {
			case err := <-observed:
				if !errors.Is(err, ErrLockLost) {
					t.Fatalf("diagnostic = %v, want ErrLockLost", err)
				}
			default:
				t.Fatal("pathological lease was not reported")
			}
		})
	}
}

func TestReceiverUsesServerTimeForSkewedLeaseCadence(t *testing.T) {
	tests := []struct {
		name      string
		setHeader func(http.Header, time.Time)
	}{
		{
			name: "millisecond broker header",
			setHeader: func(header http.Header, serverNow time.Time) {
				header.Set(wire.ServerTimeHeader, strconv.FormatInt(serverNow.UnixNano()/int64(time.Millisecond), 10))
			},
		},
		{
			name: "legacy HTTP Date",
			setHeader: func(header http.Header, serverNow time.Time) {
				header.Set("Date", serverNow.UTC().Format(http.TimeFormat))
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var renewCalls int32
			var receiveCalls int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				serverNow := time.Now().Add(-30 * time.Second)
				test.setHeader(w.Header(), serverNow)
				switch req.URL.Path {
				case wire.PathReceive:
					if atomic.AddInt32(&receiveCalls, 1) > 1 {
						<-req.Context().Done()
						return
					}
					_ = json.NewEncoder(w).Encode(wire.ReceiveResponse{Messages: []wire.Message{{
						SeqNumber: 1, LockToken: "lock-1",
						// This is only 20ms ahead on the local wall clock, but about
						// 30s ahead according to the broker clock in the response.
						LockedUntilMs: time.Now().Add(20*time.Millisecond).UnixNano() / int64(time.Millisecond),
					}}})
				case wire.PathRenewBatch:
					atomic.AddInt32(&renewCalls, 1)
					_ = json.NewEncoder(w).Encode(wire.RenewBatchResponse{Results: []wire.SettleItemResult{{
						SeqNumber: 1, Ok: true,
						LockedUntilMs: time.Now().Add(20*time.Millisecond).UnixNano() / int64(time.Millisecond),
					}}})
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
			defer client.Close()
			ctx, cancel := context.WithCancel(context.Background())
			receiver := client.Receiver("jobs", WithAutoRenew())
			err = receiver.Run(ctx, func(context.Context, *Message) error {
				time.Sleep(500 * time.Millisecond)
				cancel()
				return nil
			})
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Run error = %v, want context.Canceled", err)
			}
			if got := atomic.LoadInt32(&renewCalls); got != 0 {
				t.Fatalf("RenewBatch calls = %d, want 0 with server-relative lease", got)
			}
		})
	}
}

func TestReceiverDateFallbackSurvivesShortLeaseAndMeasuredRTT(t *testing.T) {
	const responseDelay = 150 * time.Millisecond
	var sequence int64
	var renewCalls int32
	var completeCalls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case wire.PathReceive:
			time.Sleep(responseDelay)
			serverNow := time.Now().Add(-2 * time.Second)
			w.Header().Set("Date", serverNow.UTC().Format(http.TimeFormat))
			seq := atomic.AddInt64(&sequence, 1)
			_ = json.NewEncoder(w).Encode(wire.ReceiveResponse{Messages: []wire.Message{{
				SeqNumber: seq, LockToken: fmt.Sprintf("lock-%d", seq),
				LockedUntilMs: serverNow.Add(time.Second).UnixNano() / int64(time.Millisecond),
			}}})
		case wire.PathRenewBatch:
			var request wire.RenewBatchRequest
			if err := json.NewDecoder(req.Body).Decode(&request); err != nil || len(request.Messages) != 1 {
				t.Errorf("RenewBatch request = %+v, err %v", request, err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			time.Sleep(responseDelay)
			serverNow := time.Now().Add(-2 * time.Second)
			w.Header().Set("Date", serverNow.UTC().Format(http.TimeFormat))
			atomic.AddInt32(&renewCalls, 1)
			_ = json.NewEncoder(w).Encode(wire.RenewBatchResponse{Results: []wire.SettleItemResult{{
				SeqNumber: request.Messages[0].SeqNumber, Ok: true,
				LockedUntilMs: serverNow.Add(time.Second).UnixNano() / int64(time.Millisecond),
			}}})
		case wire.PathComplete:
			atomic.AddInt32(&completeCalls, 1)
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
	defer client.Close()
	receiver := newReceiver(client, "jobs", []ReceiverOption{
		WithAutoRenew(),
		WithErrorHandler(func(err error) { t.Errorf("receiver diagnostic = %v", err) }),
	})
	for i := 0; i < 5; i++ {
		messages, err := client.Receive(context.Background(), "jobs")
		if err != nil {
			t.Fatalf("Receive run %d: %v", i, err)
		}
		if len(messages) != 1 || messages[0].leaseClockResolution != time.Second {
			t.Fatalf("Receive run %d messages = %+v, want one Date-mapped lease", i, messages)
		}
		receiver.process(context.Background(), messages[0], func(context.Context, *Message) error {
			time.Sleep(500 * time.Millisecond)
			return nil
		}, func(err error) { t.Errorf("fatal receiver error on run %d: %v", i, err) })
	}
	if got := atomic.LoadInt32(&completeCalls); got != 5 {
		t.Fatalf("Complete calls = %d, want 5", got)
	}
	if got := atomic.LoadInt32(&renewCalls); got < 5 {
		t.Fatalf("RenewBatch calls = %d, want at least one confirmation per run", got)
	}
}

func TestReceiverMissingTimeHeadersUseUntrustedCadenceAcrossFastBrokerClock(t *testing.T) {
	const responseDelay = 20 * time.Millisecond
	var renewCalls int32
	var completeCalls int32
	secondRenew := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		// A present nil Date entry suppresses net/http's automatic Date header.
		// Simulate a proxy that strips both the broker time header and HTTP Date.
		w.Header()["Date"] = nil
		serverNow := time.Now().Add(100 * time.Second)
		switch req.URL.Path {
		case wire.PathReceive:
			time.Sleep(responseDelay)
			_ = json.NewEncoder(w).Encode(wire.ReceiveResponse{Messages: []wire.Message{{
				SeqNumber: 71, LockToken: "lock-71",
				LockedUntilMs: serverNow.Add(30*time.Second).UnixNano() / int64(time.Millisecond),
			}}})
		case wire.PathRenewBatch:
			var request wire.RenewBatchRequest
			if err := json.NewDecoder(req.Body).Decode(&request); err != nil || len(request.Messages) != 1 {
				t.Errorf("RenewBatch request = %+v, err %v", request, err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			time.Sleep(responseDelay)
			call := atomic.AddInt32(&renewCalls, 1)
			if call == 2 {
				close(secondRenew)
			}
			_ = json.NewEncoder(w).Encode(wire.RenewBatchResponse{Results: []wire.SettleItemResult{{
				SeqNumber: request.Messages[0].SeqNumber, Ok: true,
				LockedUntilMs: serverNow.Add(30*time.Second).UnixNano() / int64(time.Millisecond),
			}}})
		case wire.PathComplete:
			atomic.AddInt32(&completeCalls, 1)
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
	defer client.Close()
	messages, err := client.Receive(context.Background(), "jobs")
	if err != nil || len(messages) != 1 {
		t.Fatalf("Receive messages = %d, err %v", len(messages), err)
	}
	message := messages[0]
	if !message.leaseClockUnavailable || !message.localLeaseDeadline.IsZero() {
		t.Fatalf("missing-header lease state = unavailable %v, local deadline %v", message.leaseClockUnavailable, message.localLeaseDeadline)
	}
	if message.leaseResponseRTT < responseDelay {
		t.Fatalf("missing-header response RTT = %s, want at least %s", message.leaseResponseRTT, responseDelay)
	}

	var warnings int32
	receiver := newReceiver(client, "jobs", []ReceiverOption{
		WithAutoRenew(),
		WithErrorHandler(func(err error) {
			if err.Error() != "mqlite: broker time headers unavailable; lease scheduling is using degraded clock mode" {
				t.Errorf("receiver diagnostic = %v", err)
				return
			}
			atomic.AddInt32(&warnings, 1)
		}),
	})
	receiver.process(context.Background(), message, func(context.Context, *Message) error {
		if got := atomic.LoadInt32(&renewCalls); got != 1 {
			t.Fatalf("renew calls before handler = %d, want one synchronous confirmation", got)
		}
		select {
		case <-secondRenew:
			return nil
		case <-time.After(2 * time.Second):
			t.Error("untrusted renewal cadence did not run")
			return nil
		}
	}, func(err error) { t.Errorf("fatal receiver error = %v", err) })
	if got := atomic.LoadInt32(&renewCalls); got < 2 {
		t.Fatalf("RenewBatch calls = %d, want synchronous confirmation plus bounded background renewal", got)
	}
	if got := atomic.LoadInt32(&completeCalls); got != 1 {
		t.Fatalf("Complete calls = %d, want one", got)
	}
	if got := atomic.LoadInt32(&warnings); got != 1 {
		t.Fatalf("degraded-clock diagnostics = %d, want one", got)
	}
}

func TestReceiverDoesNotMutateMessageDuringBackgroundRenewal(t *testing.T) {
	var renewCalls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		serverNow := time.Now()
		w.Header().Set(wire.ServerTimeHeader, strconv.FormatInt(serverNow.UnixNano()/int64(time.Millisecond), 10))
		switch req.URL.Path {
		case wire.PathReceive:
			_ = json.NewEncoder(w).Encode(wire.ReceiveResponse{Messages: []wire.Message{{
				SeqNumber: 61, LockToken: "lock-61",
				LockedUntilMs: serverNow.Add(300*time.Millisecond).UnixNano() / int64(time.Millisecond),
			}}})
		case wire.PathRenewBatch:
			var request wire.RenewBatchRequest
			if err := json.NewDecoder(req.Body).Decode(&request); err != nil || len(request.Messages) != 1 {
				t.Errorf("RenewBatch request = %+v, err %v", request, err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			atomic.AddInt32(&renewCalls, 1)
			_ = json.NewEncoder(w).Encode(wire.RenewBatchResponse{Results: []wire.SettleItemResult{{
				SeqNumber: request.Messages[0].SeqNumber, Ok: true,
				LockedUntilMs: serverNow.Add(300*time.Millisecond).UnixNano() / int64(time.Millisecond),
			}}})
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
	defer client.Close()
	messages, err := client.Receive(context.Background(), "jobs")
	if err != nil || len(messages) != 1 {
		t.Fatalf("Receive messages = %d, err %v", len(messages), err)
	}
	receiver := newReceiver(client, "jobs", []ReceiverOption{
		WithAutoRenew(),
		WithErrorHandler(func(err error) { t.Errorf("renewal diagnostic = %v", err) }),
	})
	receiver.process(context.Background(), messages[0], func(context.Context, *Message) error {
		until := time.Now().Add(350 * time.Millisecond)
		for time.Now().Before(until) {
			// Copying and formatting the public value is legal application code. All
			// renewal metadata must therefore remain goroutine-local after dispatch.
			_ = fmt.Sprintf("%v", *messages[0])
			runtime.Gosched()
		}
		return nil
	}, func(error) { t.Error("renewal failure must remain message-scoped") })
	if got := atomic.LoadInt32(&renewCalls); got < 2 {
		t.Fatalf("RenewBatch calls = %d, want concurrent background renewals", got)
	}
}

func TestReceiverReportsMissingBrokerTimeHeadersOnce(t *testing.T) {
	t.Run("one diagnostic for receiver lifetime", func(t *testing.T) {
		source := &fakeReceiveSource{}
		var warnings int32
		var handlers int32
		receiver := newReceiver(source, "jobs", []ReceiverOption{WithErrorHandler(func(err error) {
			if err.Error() != "mqlite: broker time headers unavailable; lease scheduling is using degraded clock mode" {
				t.Errorf("diagnostic = %v", err)
			}
			atomic.AddInt32(&warnings, 1)
		})})
		for seq := int64(1); seq <= 2; seq++ {
			message := source.message(seq)
			message.leaseClockUnavailable = true
			receiver.process(context.Background(), message, func(context.Context, *Message) error {
				atomic.AddInt32(&handlers, 1)
				return nil
			}, func(error) { t.Error("diagnostic must not stop the receiver") })
		}
		if got := atomic.LoadInt32(&warnings); got != 1 {
			t.Fatalf("diagnostics = %d, want one", got)
		}
		if got := atomic.LoadInt32(&handlers); got != 2 {
			t.Fatalf("handler calls = %d, want two", got)
		}
	})

	t.Run("callback cancellation prevents dispatch", func(t *testing.T) {
		source := &fakeReceiveSource{}
		ctx, cancel := context.WithCancel(context.Background())
		var handlers int32
		receiver := newReceiver(source, "jobs", []ReceiverOption{WithErrorHandler(func(error) { cancel() })})
		message := source.message(3)
		message.leaseClockUnavailable = true
		receiver.process(ctx, message, func(context.Context, *Message) error {
			atomic.AddInt32(&handlers, 1)
			return nil
		}, func(error) { t.Error("diagnostic must not stop the receiver") })
		if got := atomic.LoadInt32(&handlers); got != 0 {
			t.Fatalf("handler calls = %d, want none after callback cancellation", got)
		}
		if settles := source.settleSnapshot(); len(settles) != 0 {
			t.Fatalf("settlements = %+v, want none after callback cancellation", settles)
		}
	})

	t.Run("first missing renewal header is reported", func(t *testing.T) {
		base := &fakeReceiveSource{}
		message := base.message(4)
		message.localLeaseDeadline = time.Now().Add(120 * time.Millisecond)
		message.leaseClockResolution = time.Millisecond
		warning := make(chan struct{})
		var warningOnce sync.Once
		source := &deadlineLeaseSource{fakeReceiveSource: base}
		source.renewalLeaseFn = func(context.Context, *Message) (renewalLease, error) {
			return renewalLease{deadline: time.Now().Add(time.Second), clockUnavailable: true}, nil
		}
		receiver := newReceiver(source, "jobs", []ReceiverOption{
			WithAutoRenew(),
			WithErrorHandler(func(err error) {
				if err.Error() != "mqlite: broker time headers unavailable; lease scheduling is using degraded clock mode" {
					t.Errorf("diagnostic = %v", err)
				}
				warningOnce.Do(func() { close(warning) })
			}),
		})
		receiver.process(context.Background(), message, func(context.Context, *Message) error {
			select {
			case <-warning:
				return nil
			case <-time.After(time.Second):
				t.Error("missing renewal headers were not reported")
				return nil
			}
		}, func(error) { t.Error("degraded clock diagnostic must not stop the receiver") })
		if settles := base.settleSnapshot(); len(settles) != 1 || settles[0].op != "complete" {
			t.Fatalf("settlements = %+v, want completion after diagnostic", settles)
		}
	})
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
	source.renewalLeaseFn = func(context.Context, *Message) (renewalLease, error) {
		if atomic.AddInt32(&renewCalls, 1) == 1 {
			// Initial confirmation commits, but the fast client clock cannot use
			// the broker's absolute deadline as local retry headroom.
			return renewalLease{deadline: time.Now().Add(-time.Second), clockResolution: time.Second}, nil
		}
		return renewalLease{}, errors.New("temporary renewal failure")
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
	source.renewalLeaseFn = func(ctx context.Context, _ *Message) (renewalLease, error) {
		if atomic.AddInt32(&renewCalls, 1) == 1 {
			// The synchronous dispatch gate succeeds but cannot provide a locally
			// trustworthy deadline under persistent clock skew.
			return renewalLease{deadline: time.Now().Add(-time.Second), clockResolution: time.Second}, nil
		}
		close(renewStarted)
		<-ctx.Done()
		close(renewCanceled)
		return renewalLease{}, ctx.Err()
	}
	observed := make(chan error, 1)
	receiver := newReceiver(source, "jobs", []ReceiverOption{
		WithAutoRenew(),
		WithErrorHandler(func(err error) { observed <- err }),
	})
	receiver.renewAttemptTimeout = 50 * time.Millisecond
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
		name        string
		remaining   time.Duration
		responseRTT time.Duration
		want        time.Duration
	}{
		{name: "one second", remaining: time.Second, want: (time.Second - leaseDeadlineSafety) / 3},
		{name: "spin guard", remaining: 120 * time.Millisecond, want: 50 * time.Millisecond},
		{name: "twenty milliseconds", remaining: 20 * time.Millisecond, want: 0},
		{name: "measured RTT starts immediately", remaining: 600 * time.Millisecond, responseRTT: 400 * time.Millisecond, want: 0},
		{name: "measured RTT caps ordinary cadence", remaining: time.Second, responseRTT: 400 * time.Millisecond, want: 180 * time.Millisecond},
		{name: "long lease", remaining: time.Minute, want: (time.Minute - leaseDeadlineSafety) / 3},
		{name: "smallest positive", remaining: time.Nanosecond, want: 0},
		{name: "expired", remaining: 0, want: 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := autoRenewInterval(test.remaining, test.responseRTT); got != test.want {
				t.Fatalf("autoRenewInterval(%s, %s) = %s, want %s", test.remaining, test.responseRTT, got, test.want)
			}
		})
	}
}

func TestInitialLeaseConfirmationAccountsForClockPrecisionAndRTT(t *testing.T) {
	tests := []struct {
		name      string
		lease     renewalLease
		remaining time.Duration
		want      bool
	}{
		{name: "ordinary exact lease", lease: renewalLease{clockResolution: time.Millisecond}, remaining: time.Second},
		{name: "exact lease near measured RTT", lease: renewalLease{clockResolution: time.Millisecond, responseRTT: 400 * time.Millisecond}, remaining: 600 * time.Millisecond, want: true},
		{name: "coarse Date lower bound", lease: renewalLease{clockResolution: time.Second}, remaining: 800 * time.Millisecond, want: true},
		{name: "pathological exact lease", lease: renewalLease{clockResolution: time.Millisecond}, remaining: 45 * time.Millisecond, want: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := needsInitialLeaseConfirmation(test.lease, test.remaining); got != test.want {
				t.Fatalf("needsInitialLeaseConfirmation = %v, want %v", got, test.want)
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
		{name: "broker HTTP 499 wraps cancellation", err: &HTTPError{StatusCode: 499, cause: context.Canceled}, want: true},
		{name: "HTTP 400 overrides internal code", err: &HTTPError{StatusCode: 400, cause: errors.New("internal")}},
		{name: "caller cancellation", err: context.Canceled},
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
			source.renewalLeaseFn = func(context.Context, *Message) (renewalLease, error) {
				if atomic.AddInt32(&renewCalls, 1) == 1 {
					return renewalLease{deadline: test.deadline(), deadlineUntrusted: true}, nil
				}
				return renewalLease{}, ErrLockLost
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
		w.Header().Set(wire.ServerTimeHeader, strconv.FormatInt(time.Now().UnixNano()/int64(time.Millisecond), 10))
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
		w.Header().Set(wire.ServerTimeHeader, strconv.FormatInt(time.Now().UnixNano()/int64(time.Millisecond), 10))
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
			_, err := receiver.renewMessage(context.Background(), m, time.Second, renewalLease{})
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
	secondReceive := make(chan struct{})
	source.receiveFn = func(ctx context.Context, call int, record receiveRecord) ([]*Message, error) {
		if atomic.CompareAndSwapInt32(&returned, 0, 1) {
			message := source.message(11)
			message.LockedUntil = time.Now().Add(120 * time.Millisecond)
			return []*Message{message}, nil
		}
		close(secondReceive)
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
	select {
	case <-secondReceive:
		close(releaseError)
		<-done
		t.Fatal("receiver issued another claim while reporting permanent renewal failure")
	case <-time.After(100 * time.Millisecond):
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
	if receives := source.receiveSnapshot(); len(receives) != 1 {
		t.Fatalf("receive calls = %d, want one", len(receives))
	}
}

func TestPermanentRenewalStopsRunBeforeHandlerAndNotification(t *testing.T) {
	events := make([]string, 0, 3)
	receiver := newReceiver(nil, "jobs", []ReceiverOption{
		WithErrorHandler(func(error) { events = append(events, "notify") }),
	})
	receiver.stopPermanentRenewal(
		func() { events = append(events, "cancel-handler") },
		ErrPermissionDenied,
		func(error) { events = append(events, "fail-run") },
	)
	if got := fmt.Sprint(events); got != "[fail-run cancel-handler notify]" {
		t.Fatalf("permanent renewal order = %s, want receiver stop before handler cancellation and notification", got)
	}
}

func TestReceiverJoinsInFlightRenewBeforeComplete(t *testing.T) {
	settler := newBlockingRenewSettler()
	message := &Message{
		SequenceNumber: 12,
		LockedUntil:    time.Now().Add(120 * time.Millisecond),
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
