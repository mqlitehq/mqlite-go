package mqlite

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const sustainedReceiverMessages = 20000

type sustainedReceiveSource struct {
	mu     sync.Mutex
	next   int64
	total  int64
	queue  string
	cancel context.CancelFunc

	completed  []uint32
	settled    int64
	renewed    int64
	unexpected int64
}

func (s *sustainedReceiveSource) receiveOne(ctx context.Context, queue string, max int, _ int64, _ receiveMode, _ string) ([]*Message, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.Lock()
	remaining := s.total - s.next
	if remaining <= 0 {
		s.mu.Unlock()
		<-ctx.Done()
		return nil, ctx.Err()
	}
	count := int64(max)
	if count > remaining {
		count = remaining
	}
	start := s.next + 1
	s.next += count
	s.mu.Unlock()

	if queue != s.queue {
		atomic.AddInt64(&s.unexpected, 1)
	}
	deadline := time.Now().Add(time.Hour)
	messages := make([]*Message, count)
	for i := range messages {
		sequence := start + int64(i)
		messages[i] = &Message{
			SequenceNumber: sequence,
			Body:           []byte("sentinel"),
			LockedUntil:    deadline,
			queue:          queue,
			lockToken:      "soak-" + strconv.FormatInt(sequence, 10),
			s:              s,
		}
	}
	return messages, nil
}

func (s *sustainedReceiveSource) complete(_ context.Context, queue string, seq int64, token string) error {
	if seq <= 0 || seq > s.total || queue != s.queue || token != "soak-"+strconv.FormatInt(seq, 10) {
		atomic.AddInt64(&s.unexpected, 1)
	} else {
		atomic.AddUint32(&s.completed[seq], 1)
	}
	if atomic.AddInt64(&s.settled, 1) == s.total {
		s.cancel()
	}
	return nil
}

func (s *sustainedReceiveSource) abandon(context.Context, string, int64, string, int64) error {
	atomic.AddInt64(&s.unexpected, 1)
	return nil
}

func (s *sustainedReceiveSource) reject(context.Context, string, int64, string, string, string) error {
	atomic.AddInt64(&s.unexpected, 1)
	return nil
}

func (s *sustainedReceiveSource) deferMsg(context.Context, string, int64, string) error {
	atomic.AddInt64(&s.unexpected, 1)
	return nil
}

func (s *sustainedReceiveSource) renew(context.Context, string, int64, string) error {
	atomic.AddInt64(&s.renewed, 1)
	return nil
}

// TestReceiverSustainedLifecycle is a bounded CI soak. It exercises the same
// Receiver.Run instance through enough message lifecycles to expose retained
// workers, crossed settlement identities, or missing terminal joins without
// relying on unstable process-RSS thresholds.
func TestReceiverSustainedLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	source := &sustainedReceiveSource{
		total:     sustainedReceiverMessages,
		queue:     "sentinel-subscription",
		cancel:    cancel,
		completed: make([]uint32, sustainedReceiverMessages+1),
	}
	handled := make([]uint32, sustainedReceiverMessages+1)
	var handledCount int64
	receiver := newReceiver(source, source.queue, []ReceiverOption{
		WithConcurrency(32),
		WithPrefetch(32),
		WithAutoRenew(),
	})

	err := receiver.Run(ctx, func(_ context.Context, message *Message) error {
		if message.SequenceNumber <= 0 || message.SequenceNumber > sustainedReceiverMessages {
			atomic.AddInt64(&source.unexpected, 1)
			return nil
		}
		atomic.AddUint32(&handled[message.SequenceNumber], 1)
		atomic.AddInt64(&handledCount, 1)
		return nil
	})
	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("receiver soak timed out: handled=%d completed=%d want=%d",
			atomic.LoadInt64(&handledCount), atomic.LoadInt64(&source.settled), sustainedReceiverMessages)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v, want context.Canceled", err)
	}
	if got := atomic.LoadInt64(&source.settled); got != sustainedReceiverMessages {
		t.Fatalf("completed messages = %d, want %d", got, sustainedReceiverMessages)
	}
	if got := atomic.LoadInt64(&source.renewed); got != 0 {
		t.Fatalf("unexpected renewals = %d", got)
	}
	if got := atomic.LoadInt64(&source.unexpected); got != 0 {
		t.Fatalf("unexpected operations = %d", got)
	}
	for seq := 1; seq <= sustainedReceiverMessages; seq++ {
		if got := atomic.LoadUint32(&handled[seq]); got != 1 {
			t.Fatalf("handler count for sequence %d = %d, want 1", seq, got)
		}
		if got := atomic.LoadUint32(&source.completed[seq]); got != 1 {
			t.Fatalf("Complete count for sequence %d = %d, want 1", seq, got)
		}
	}
}
