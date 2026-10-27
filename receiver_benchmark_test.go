package mqlite

import (
	"context"
	"testing"
	"time"
)

type benchmarkSettler struct{}

func (benchmarkSettler) complete(context.Context, string, int64, string) error { return nil }
func (benchmarkSettler) abandon(context.Context, string, int64, string, int64) error {
	return nil
}
func (benchmarkSettler) reject(context.Context, string, int64, string, string, string) error {
	return nil
}
func (benchmarkSettler) deferMsg(context.Context, string, int64, string) error { return nil }
func (benchmarkSettler) renew(context.Context, string, int64, string) error    { return nil }

func benchmarkReceiverProcess(b *testing.B, autoRenew, parallel bool) {
	settler := benchmarkSettler{}
	message := &Message{
		SequenceNumber: 1,
		LockedUntil:    time.Now().Add(time.Hour),
		queue:          "jobs",
		lockToken:      "benchmark-token",
		s:              settler,
	}
	opts := []ReceiverOption(nil)
	if autoRenew {
		opts = append(opts, WithAutoRenew())
	}
	receiver := newReceiver(nil, "jobs", opts)
	handler := func(context.Context, *Message) error { return nil }
	fail := func(err error) { panic(err) }
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	if parallel {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				receiver.process(ctx, message, handler, fail)
			}
		})
		return
	}
	for i := 0; i < b.N; i++ {
		receiver.process(ctx, message, handler, fail)
	}
}

// BenchmarkReceiverProcess isolates the managed receiver's local lifecycle
// cost. The long lease prevents network renewal, so AutoRenew measures only the
// goroutine cancellation and pre-settlement join rather than broker latency.
func BenchmarkReceiverProcess(b *testing.B) {
	b.Run("manual", func(b *testing.B) {
		benchmarkReceiverProcess(b, false, false)
	})
	b.Run("auto-renew-idle", func(b *testing.B) {
		benchmarkReceiverProcess(b, true, false)
	})
	b.Run("auto-renew-idle-parallel", func(b *testing.B) {
		benchmarkReceiverProcess(b, true, true)
	})
}
