package mqlite

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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
	mu        sync.Mutex
	receives  []receiveRecord
	settles   []settleRecord
	receiveFn func(context.Context, int, receiveRecord) ([]*Message, error)
	settleErr error
	renewed   chan struct{}
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
	renewed := s.renewed
	s.mu.Unlock()
	if record.op == "renew" && renewed != nil {
		select {
		case renewed <- struct{}{}:
		default:
		}
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
	receiver := newReceiver(source, "jobs", nil)
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
	settles := source.settleSnapshot()
	if len(settles) != 1 || settles[0].op != "complete" {
		t.Fatalf("settlements = %+v", settles)
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
	receiver.renewFloor = 2 * time.Millisecond
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
