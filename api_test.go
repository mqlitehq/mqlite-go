package mqlite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
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
