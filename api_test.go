package mqlite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mqlitehq/mqlite-go/internal/wire"
)

func TestReceivePreservesDeliveryIdentityForSettlement(t *testing.T) {
	const (
		queue     = "jobs"
		lockToken = "lock-for-delivery-7"
	)
	enqueuedAt := int64(1700000000123)
	visibleAt := int64(1700000000456)
	expiresAt := int64(1700000100000)
	lockedUntil := int64(1700000015000)
	var requestErr error
	var receiveCalls, completeCalls int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case wire.PathReceive:
			receiveCalls++
			var request wire.ReceiveRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				requestErr = err
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if request.Queue != queue || request.MaxMessages != 3 || request.WaitTimeMs != 250 || request.AttemptID != "attempt-1" {
				requestErr = fmt.Errorf("unexpected receive request: %+v", request)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			response := wire.ReceiveResponse{Messages: []wire.Message{{
				SeqNumber: 7, Body: []byte("work"), MessageID: "message-1",
				LockToken: lockToken, DeliveryCount: 2, EnqueuedAtMs: enqueuedAt,
				VisibleAtMs: visibleAt, ExpiresAtMs: expiresAt, LockedUntilMs: lockedUntil,
			}}}
			_ = json.NewEncoder(w).Encode(response)
		case wire.PathComplete:
			completeCalls++
			var request wire.SettleRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				requestErr = err
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if request.Queue != queue || request.SeqNumber != 7 || request.LockToken != lockToken {
				requestErr = fmt.Errorf("unexpected complete request: %+v", request)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			_, _ = io.WriteString(w, `{"ok":true}`)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	client, err := Open(context.Background(), server.URL)
	if err != nil {
		t.Fatal(err)
	}
	messages, err := client.Receive(context.Background(), queue, RecvOpts{Max: 3, Wait: 250 * time.Millisecond, Attempt: "attempt-1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 {
		t.Fatalf("messages = %d, want 1", len(messages))
	}
	message := messages[0]
	if message.SequenceNumber != 7 || string(message.Body) != "work" || message.MessageID != "message-1" || message.DeliveryCount != 2 {
		t.Fatalf("message = %+v", message)
	}
	if message.LockToken() != lockToken {
		t.Fatalf("lock token = %q, want %q", message.LockToken(), lockToken)
	}
	assertEpochMillis(t, "EnqueuedAt", message.EnqueuedAt, enqueuedAt)
	assertEpochMillis(t, "VisibleAt", message.VisibleAt, visibleAt)
	assertEpochMillis(t, "ExpiresAt", message.ExpiresAt, expiresAt)
	assertEpochMillis(t, "LockedUntil", message.LockedUntil, lockedUntil)
	if err := message.Complete(context.Background()); err != nil {
		t.Fatal(err)
	}
	if requestErr != nil {
		t.Fatal(requestErr)
	}
	if receiveCalls != 1 || completeCalls != 1 {
		t.Fatalf("receive calls = %d, complete calls = %d", receiveCalls, completeCalls)
	}
}

func assertEpochMillis(t *testing.T, field string, got time.Time, want int64) {
	t.Helper()
	gotMillis := got.UnixNano() / int64(time.Millisecond)
	if gotMillis != want {
		t.Errorf("%s = %dms, want %dms", field, gotMillis, want)
	}
}

func TestSettlementFalseMeansLockLost(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"ok":false}`)
	}))
	defer server.Close()
	client, err := Open(context.Background(), server.URL)
	if err != nil {
		t.Fatal(err)
	}
	message := client.Message("jobs", 7, "stale-lock")
	if err := message.Renew(context.Background()); !errors.Is(err, ErrLockLost) {
		t.Fatalf("error = %v, want ErrLockLost", err)
	}
}

func TestBatchSettlementWireContract(t *testing.T) {
	var requestErr error
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request wire.CompleteBatchRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			requestErr = err
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if r.URL.Path != wire.PathCompleteBatch || request.Queue != "jobs" || len(request.Messages) != 2 ||
			request.Messages[0].SeqNumber != 7 || request.Messages[0].LockToken != "lock-7" ||
			request.Messages[1].SeqNumber != 8 || request.Messages[1].LockToken != "lock-8" {
			requestErr = fmt.Errorf("unexpected batch request: path=%q body=%+v", r.URL.Path, request)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(wire.CompleteBatchResponse{Results: []wire.SettleItemResult{
			{SeqNumber: 7, Ok: true},
			{SeqNumber: 8, Ok: false},
		}})
	}))
	defer server.Close()

	client, err := Open(context.Background(), server.URL)
	if err != nil {
		t.Fatal(err)
	}
	results, err := client.CompleteBatch(context.Background(), "jobs",
		client.Message("jobs", 7, "lock-7"), client.Message("jobs", 8, "lock-8"))
	if err != nil {
		t.Fatal(err)
	}
	if requestErr != nil {
		t.Fatal(requestErr)
	}
	if len(results) != 2 || !results[0].Ok || results[1].Ok {
		t.Fatalf("results = %+v", results)
	}
}

func TestBatchSettlementRejectsNilWithoutRequest(t *testing.T) {
	var requests int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requests, 1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	client, err := Open(context.Background(), server.URL)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := client.CompleteBatch(context.Background(), "jobs", (*Message)(nil)); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("CompleteBatch error = %v, want ErrInvalidArgument", err)
	}
	if _, err := client.RenewBatch(context.Background(), "jobs", (*Message)(nil)); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("RenewBatch error = %v, want ErrInvalidArgument", err)
	}
	if got := atomic.LoadInt32(&requests); got != 0 {
		t.Fatalf("requests = %d, want 0", got)
	}
}

func TestRenewBatchWireContract(t *testing.T) {
	const deadline = int64(1700000000000)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != wire.PathRenewBatch {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		var request wire.RenewBatchRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil || request.Queue != "jobs" || len(request.Messages) != 2 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(wire.RenewBatchResponse{Results: []wire.SettleItemResult{
			{SeqNumber: 7, Ok: true, LockedUntilMs: deadline},
			{SeqNumber: 8, Ok: false},
		}})
	}))
	defer server.Close()
	client, err := Open(context.Background(), server.URL)
	if err != nil {
		t.Fatal(err)
	}
	results, err := client.RenewBatch(context.Background(), "jobs",
		client.Message("jobs", 7, "lock-7"), client.Message("jobs", 8, "lock-8"))
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 || !results[0].Ok || results[0].LockedUntil.UnixNano()/int64(time.Millisecond) != deadline || results[1].Ok || !results[1].LockedUntil.IsZero() {
		t.Fatalf("results = %+v", results)
	}
}

func TestBatchSettlementRejectsMalformedSuccess(t *testing.T) {
	tests := []struct {
		name string
		path string
		body string
		want error
	}{
		{name: "complete truncated", path: wire.PathCompleteBatch, body: `{"results":[{"seq_number":7,"ok":true}]}`, want: ErrOutcomeUnknown},
		{name: "complete reordered", path: wire.PathCompleteBatch, body: `{"results":[{"seq_number":8,"ok":true},{"seq_number":7,"ok":true}]}`, want: ErrOutcomeUnknown},
		{name: "renew missing deadline", path: wire.PathRenewBatch, body: `{"results":[{"seq_number":7,"ok":true},{"seq_number":8,"ok":false}]}`},
		{name: "renew stale with deadline", path: wire.PathRenewBatch, body: `{"results":[{"seq_number":7,"ok":true,"locked_until_ms":1700000000000},{"seq_number":8,"ok":false,"locked_until_ms":1700000000001}]}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != test.path {
					w.WriteHeader(http.StatusNotFound)
					return
				}
				_, _ = io.WriteString(w, test.body)
			}))
			defer server.Close()
			client, err := Open(context.Background(), server.URL)
			if err != nil {
				t.Fatal(err)
			}
			messages := []*Message{client.Message("jobs", 7, "lock-7"), client.Message("jobs", 8, "lock-8")}
			if test.path == wire.PathCompleteBatch {
				_, err = client.CompleteBatch(context.Background(), "jobs", messages...)
			} else {
				_, err = client.RenewBatch(context.Background(), "jobs", messages...)
			}
			if err == nil {
				t.Fatal("malformed success was accepted")
			}
			if test.want != nil && !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want errors.Is(_, %v)", err, test.want)
			}
		})
	}
}

func TestCancelWireContract(t *testing.T) {
	var requestErr error
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != wire.PathCancel {
			requestErr = fmt.Errorf("path = %q", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		var request wire.CancelRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			requestErr = err
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if request.Queue != "jobs" || request.SeqNumber != 19 {
			requestErr = fmt.Errorf("unexpected cancel request: %+v", request)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer server.Close()
	client, err := Open(context.Background(), server.URL)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Cancel(context.Background(), "jobs", 19); err != nil {
		t.Fatal(err)
	}
	if requestErr != nil {
		t.Fatal(requestErr)
	}
}

func TestCancelBrokerNotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, `{"code":"not_found","message":"message was not scheduled"}`)
	}))
	defer server.Close()
	client, err := Open(context.Background(), server.URL)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Cancel(context.Background(), "jobs", 19); !errors.Is(err, ErrNotFound) {
		t.Fatalf("error = %v, want ErrNotFound", err)
	}
}

func TestCancelFalseAcknowledgementIsMalformed(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"ok":false}`)
	}))
	defer server.Close()
	client, err := Open(context.Background(), server.URL)
	if err != nil {
		t.Fatal(err)
	}
	err = client.Cancel(context.Background(), "jobs", 19)
	if err == nil {
		t.Fatal("false acknowledgement: expected malformed response error")
	}
	if errors.Is(err, ErrNotFound) {
		t.Fatalf("false acknowledgement was misclassified as ErrNotFound: %v", err)
	}
}
