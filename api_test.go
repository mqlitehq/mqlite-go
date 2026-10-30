package mqlite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mqlitehq/mqlite-go/internal/wire"
)

func TestCreateQueueRejectsUnsafeLockDurationWithoutRequest(t *testing.T) {
	var calls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&calls, 1)
		_, _ = io.WriteString(w, `{}`)
	}))
	defer server.Close()

	client, err := Open(context.Background(), server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	for _, lockDuration := range []time.Duration{-time.Nanosecond, time.Nanosecond, time.Millisecond, time.Second - time.Nanosecond} {
		err := client.CreateQueue(context.Background(), "jobs", QueueConfig{LockDuration: lockDuration})
		if !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("CreateQueue lock duration %s error = %v, want ErrInvalidArgument", lockDuration, err)
		}
	}
	if got := atomic.LoadInt32(&calls); got != 0 {
		t.Fatalf("invalid CreateQueue calls made %d HTTP requests, want none", got)
	}
}

func TestCreateQueueLockDurationBoundary(t *testing.T) {
	var requestsMu sync.Mutex
	var requests []wire.CreateQueueRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != wire.PathCreateQueue {
			http.NotFound(w, r)
			return
		}
		var request wire.CreateQueueRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		requestsMu.Lock()
		requests = append(requests, request)
		requestsMu.Unlock()
		_, _ = io.WriteString(w, `{}`)
	}))
	defer server.Close()

	client, err := Open(context.Background(), server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	for _, test := range []struct {
		name       string
		duration   time.Duration
		wantMillis int64
	}{
		{name: "broker default", duration: 0, wantMillis: 0},
		{name: "minimum", duration: time.Second, wantMillis: 1000},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := client.CreateQueue(context.Background(), "jobs", QueueConfig{LockDuration: test.duration}); err != nil {
				t.Fatal(err)
			}
			requestsMu.Lock()
			requestCount := len(requests)
			if requestCount != 1 {
				requestsMu.Unlock()
				t.Fatalf("CreateQueue made %d requests, want one", requestCount)
			}
			request := requests[0]
			requests = requests[:0]
			requestsMu.Unlock()
			if request.Name != "jobs" || request.Config.LockDurationMs != test.wantMillis {
				t.Fatalf("CreateQueue request = %+v, want name jobs and lock_duration_ms %d", request, test.wantMillis)
			}
		})
	}
}

func TestSendInvalidSuccessIsOutcomeUnknown(t *testing.T) {
	const marker = "untrusted-ack-marker"
	tests := []struct {
		name          string
		body          string
		contentLength int64
		call          func(*Client) error
	}{
		{
			name: "SendOne truncated JSON",
			body: `{"seq_numbers":["` + marker,
			call: func(client *Client) error {
				_, err := client.SendOne(context.Background(), "jobs", OutMessage{Body: []byte("one")})
				return err
			},
		},
		{
			name: "Send batch truncated JSON",
			body: `{"seq_numbers":["` + marker,
			call: func(client *Client) error {
				_, err := client.Send(context.Background(), "jobs",
					OutMessage{Body: []byte("one")},
					OutMessage{Body: []byte("two")})
				return err
			},
		},
		{
			name:          "SendOne oversized acknowledgement",
			body:          marker,
			contentLength: 1 << 30,
			call: func(client *Client) error {
				_, err := client.SendOne(context.Background(), "jobs", OutMessage{Body: []byte("one")})
				return err
			},
		},
		{
			name: "Send sequence count mismatch",
			body: `{"seq_numbers":[11]}`,
			call: func(client *Client) error {
				_, err := client.Send(context.Background(), "jobs",
					OutMessage{Body: []byte("one")},
					OutMessage{Body: []byte("two")})
				return err
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var calls int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				atomic.AddInt32(&calls, 1)
				if test.contentLength != 0 {
					w.Header().Set("Content-Length", strconv.FormatInt(test.contentLength, 10))
				}
				w.WriteHeader(http.StatusOK)
				_, _ = io.WriteString(w, test.body)
			}))
			defer server.Close()

			client, err := Open(context.Background(), server.URL)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()

			err = test.call(client)
			if !errors.Is(err, ErrOutcomeUnknown) {
				t.Fatalf("Send error = %v, want ErrOutcomeUnknown", err)
			}
			if strings.Contains(err.Error(), marker) {
				t.Fatalf("Send error reflected untrusted acknowledgement body: %v", err)
			}
			if got := atomic.LoadInt32(&calls); got != 1 {
				t.Fatalf("Send made %d requests, want one", got)
			}
		})
	}
}

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

func TestTerminalSettlementInvalidSuccessIsOutcomeUnknown(t *testing.T) {
	const marker = "semantic-ack-marker"
	tests := []struct {
		name string
		call func(context.Context, *Message) error
	}{
		{name: "complete", call: func(ctx context.Context, message *Message) error { return message.Complete(ctx) }},
		{name: "abandon", call: func(ctx context.Context, message *Message) error { return message.Abandon(ctx) }},
		{name: "reject", call: func(ctx context.Context, message *Message) error { return message.Reject(ctx) }},
		{name: "defer", call: func(ctx context.Context, message *Message) error { return message.Defer(ctx) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var calls int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				atomic.AddInt32(&calls, 1)
				_, _ = io.WriteString(w, `{"future":"`+marker+`"}`)
			}))
			defer server.Close()
			client, err := Open(context.Background(), server.URL)
			if err != nil {
				t.Fatal(err)
			}
			err = test.call(context.Background(), client.Message("jobs", 7, "lock-7"))
			if !errors.Is(err, ErrOutcomeUnknown) {
				t.Fatalf("error = %v, want ErrOutcomeUnknown", err)
			}
			if strings.Contains(err.Error(), marker) {
				t.Fatalf("error reflected successful response content: %v", err)
			}
			if got := atomic.LoadInt32(&calls); got != 1 {
				t.Fatalf("requests = %d, want 1", got)
			}
		})
	}
}

func TestRenewInvalidSuccessIsOutcomeUnknown(t *testing.T) {
	const marker = "renew-ack-marker"
	var calls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		_, _ = io.WriteString(w, `{"future":"`+marker+`"}`)
	}))
	defer server.Close()
	client, err := Open(context.Background(), server.URL)
	if err != nil {
		t.Fatal(err)
	}
	err = client.Message("jobs", 7, "lock-7").Renew(context.Background())
	if !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("error = %v, want ErrOutcomeUnknown", err)
	}
	if errors.Is(err, ErrLockLost) {
		t.Fatalf("missing acknowledgement was misclassified as ErrLockLost: %v", err)
	}
	var invalid *invalidSuccessResponseError
	if !errors.As(err, &invalid) {
		t.Fatalf("error = %v, want invalid successful response", err)
	}
	if strings.Contains(err.Error(), marker) {
		t.Fatalf("error reflected successful response content: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("requests = %d, want 1", got)
	}
}

func TestReceiveRequiresExplicitMessages(t *testing.T) {
	const marker = "receive-ack-marker"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"future":"`+marker+`"}`)
	}))
	defer server.Close()
	client, err := Open(context.Background(), server.URL)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Receive(context.Background(), "jobs"); err == nil {
		t.Fatal("Receive accepted a response without messages")
	} else if strings.Contains(err.Error(), marker) {
		t.Fatalf("error reflected successful response content: %v", err)
	}
}

func TestAtMostOnceReceiveInvalidSuccessIsOutcomeUnknown(t *testing.T) {
	const marker = "at-most-once-ack-marker"
	var calls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		var request wire.ReceiveRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if request.ReceiveMode != int(receiveAndDelete) {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		_, _ = io.WriteString(w, `{"future":"`+marker+`"}`)
	}))
	defer server.Close()
	client, err := Open(context.Background(), server.URL)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Receive(context.Background(), "jobs", RecvOpts{AtMostOnce: true})
	if !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("error = %v, want ErrOutcomeUnknown", err)
	}
	var invalid *invalidSuccessResponseError
	if !errors.As(err, &invalid) {
		t.Fatalf("error = %v, want preserved invalid success cause", err)
	}
	if strings.Contains(err.Error(), marker) {
		t.Fatalf("error reflected successful response content: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("requests = %d, want 1", got)
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

func TestRenewBatchInvalidSuccessIsOutcomeUnknown(t *testing.T) {
	const marker = "renew-batch-ack-marker"
	var calls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		_, _ = io.WriteString(w, `{"results":[{"seq_number":7,"future":"`+marker+`"}]}`)
	}))
	defer server.Close()
	client, err := Open(context.Background(), server.URL)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.RenewBatch(context.Background(), "jobs", client.Message("jobs", 7, "lock-7"))
	if !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("error = %v, want ErrOutcomeUnknown", err)
	}
	var invalid *invalidSuccessResponseError
	if !errors.As(err, &invalid) {
		t.Fatalf("error = %v, want preserved invalid success cause", err)
	}
	if strings.Contains(err.Error(), marker) {
		t.Fatalf("error reflected successful response content: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("requests = %d, want 1", got)
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
		{name: "complete with renewal deadline", path: wire.PathCompleteBatch, body: `{"results":[{"seq_number":7,"ok":true,"locked_until_ms":1700000000000},{"seq_number":8,"ok":true}]}`, want: ErrOutcomeUnknown},
		{name: "complete missing item acknowledgement", path: wire.PathCompleteBatch, body: `{"results":[{"seq_number":7},{"seq_number":8,"ok":false}]}`, want: ErrOutcomeUnknown},
		{name: "complete truncated JSON", path: wire.PathCompleteBatch, body: `{"results":[`, want: ErrOutcomeUnknown},
		{name: "renew missing deadline", path: wire.PathRenewBatch, body: `{"results":[{"seq_number":7,"ok":true},{"seq_number":8,"ok":false}]}`, want: ErrOutcomeUnknown},
		{name: "renew missing item acknowledgement", path: wire.PathRenewBatch, body: `{"results":[{"seq_number":7,"locked_until_ms":1700000000000},{"seq_number":8,"ok":false}]}`, want: ErrOutcomeUnknown},
		{name: "renew stale with deadline", path: wire.PathRenewBatch, body: `{"results":[{"seq_number":7,"ok":true,"locked_until_ms":1700000000000},{"seq_number":8,"ok":false,"locked_until_ms":1700000000001}]}`, want: ErrOutcomeUnknown},
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
			if test.name == "complete truncated JSON" {
				var syntax *json.SyntaxError
				if !errors.As(err, &syntax) {
					t.Fatalf("error = %v, want preserved JSON syntax cause", err)
				}
			}
		})
	}
}

func TestCompleteBatchHTTPRejectionIsNotOutcomeUnknown(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(w, `{"code":"permission_denied","message":"not allowed"}`)
	}))
	defer server.Close()
	client, err := Open(context.Background(), server.URL)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.CompleteBatch(context.Background(), "jobs", client.Message("jobs", 7, "lock-7"))
	if !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("error = %v, want ErrPermissionDenied", err)
	}
	if errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("HTTP rejection was misclassified as ErrOutcomeUnknown: %v", err)
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

func TestCancelFalseAcknowledgementIsOutcomeUnknown(t *testing.T) {
	var calls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		_, _ = io.WriteString(w, `{"ok":false}`)
	}))
	defer server.Close()
	client, err := Open(context.Background(), server.URL)
	if err != nil {
		t.Fatal(err)
	}
	err = client.Cancel(context.Background(), "jobs", 19)
	if !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("error = %v, want ErrOutcomeUnknown", err)
	}
	if errors.Is(err, ErrNotFound) {
		t.Fatalf("false acknowledgement was misclassified as ErrNotFound: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("requests = %d, want 1", got)
	}
}

func TestCancelInvalidSuccessIsOutcomeUnknown(t *testing.T) {
	const marker = "cancel-ack-marker"
	var calls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		_, _ = io.WriteString(w, `{"future":"`+marker+`"}`)
	}))
	defer server.Close()
	client, err := Open(context.Background(), server.URL)
	if err != nil {
		t.Fatal(err)
	}
	err = client.Cancel(context.Background(), "jobs", 19)
	if !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("error = %v, want ErrOutcomeUnknown", err)
	}
	if strings.Contains(err.Error(), marker) {
		t.Fatalf("error reflected successful response content: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("requests = %d, want 1", got)
	}
}

func TestReceiverRetriesInvalidTerminalSettlementAcknowledgement(t *testing.T) {
	tests := []struct {
		name       string
		path       string
		handlerErr error
	}{
		{name: "complete", path: wire.PathComplete},
		{name: "abandon", path: wire.PathAbandon, handlerErr: errors.New("handler failed")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var mu sync.Mutex
			var settlements []wire.SettleRequest
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case wire.PathReceive:
					_ = json.NewEncoder(w).Encode(wire.ReceiveResponse{Messages: []wire.Message{{
						SeqNumber: 7, LockToken: "lock-7",
						LockedUntilMs: time.Now().Add(time.Minute).UnixNano() / int64(time.Millisecond),
					}}})
				case test.path:
					var request wire.SettleRequest
					if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					mu.Lock()
					settlements = append(settlements, request)
					call := len(settlements)
					mu.Unlock()
					if call == 1 {
						_, _ = io.WriteString(w, `{"future":"not-an-ack"}`)
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
			var notifications int32
			receiver := client.Receiver("jobs", WithErrorHandler(func(error) {
				atomic.AddInt32(&notifications, 1)
			}))
			receiver.settleBackoff = time.Millisecond
			receiver.maxSettleBackoff = time.Millisecond
			receiver.jitter = func(delay time.Duration) time.Duration { return delay }
			ctx, cancel := context.WithCancel(context.Background())
			err = receiver.Run(ctx, func(context.Context, *Message) error {
				cancel()
				return test.handlerErr
			})
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Run error = %v, want context.Canceled", err)
			}
			mu.Lock()
			got := append([]wire.SettleRequest(nil), settlements...)
			mu.Unlock()
			if len(got) != 2 || got[0] != got[1] {
				t.Fatalf("settlements = %+v, want two identical requests", got)
			}
			if got[0].Queue != "jobs" || got[0].SeqNumber != 7 || got[0].LockToken != "lock-7" {
				t.Fatalf("settlement = %+v, want original delivery identity", got[0])
			}
			if n := atomic.LoadInt32(&notifications); n != 0 {
				t.Fatalf("error notifications = %d, want 0 after recovered retry", n)
			}
		})
	}
}
