package mqlite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mqlitehq/mqlite-go/internal/wire"
)

func TestParseDSNContract(t *testing.T) {
	tests := []struct {
		name         string
		dsn          string
		wantEndpoint string
		wantToken    string
		wantErr      bool
	}{
		{name: "product scheme adds port", dsn: "mqlite://broker", wantEndpoint: "http://broker:6754"},
		{name: "secure product scheme", dsn: "mqlites://broker", wantEndpoint: "https://broker:6754"},
		{name: "explicit port", dsn: "mqlite://broker:9000", wantEndpoint: "http://broker:9000"},
		{name: "embedded token", dsn: "mqlite://secret@broker", wantEndpoint: "http://broker:6754", wantToken: "secret"},
		{name: "escaped colon in token", dsn: "mqlite://tok%3Aen@broker", wantEndpoint: "http://broker:6754", wantToken: "tok:en"},
		{name: "tls query compatibility", dsn: "mqlite://broker?tls=true", wantEndpoint: "https://broker:6754"},
		{name: "explicit plaintext query", dsn: "mqlite://broker?tls=false", wantEndpoint: "http://broker:6754"},
		{name: "secure scheme cannot be downgraded", dsn: "mqlites://broker?tls=false", wantEndpoint: "https://broker:6754"},
		{name: "ipv6", dsn: "mqlite://[::1]", wantEndpoint: "http://[::1]:6754"},
		{name: "http base path", dsn: "http://broker.example/base/", wantEndpoint: "http://broker.example/base"},
		{name: "empty", dsn: "", wantErr: true},
		{name: "unsupported scheme", dsn: "ftp://broker", wantErr: true},
		{name: "missing host", dsn: "mqlite://", wantErr: true},
		{name: "unknown product query", dsn: "mqlite://broker?region=us", wantErr: true},
		{name: "duplicate tls query", dsn: "mqlite://broker?tls=true&tls=false", wantErr: true},
		{name: "invalid tls query", dsn: "mqlite://broker?tls=1", wantErr: true},
		{name: "malformed product query", dsn: "mqlite://broker?tls=true&bad=%ZZ", wantErr: true},
		{name: "empty product query", dsn: "mqlite://broker?", wantErr: true},
		{name: "product path", dsn: "mqlite://broker/base", wantErr: true},
		{name: "product password component", dsn: "mqlite://token:password@broker", wantErr: true},
		{name: "malformed product token", dsn: "mqlite://token%ZZ@broker", wantErr: true},
		{name: "http credentials", dsn: "http://token@broker", wantErr: true},
		{name: "http query", dsn: "http://broker?token=secret", wantErr: true},
		{name: "empty http query", dsn: "http://broker?", wantErr: true},
		{name: "fragment", dsn: "https://broker/#fragment", wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			endpoint, token, err := parseDSN(test.dsn)
			if (err != nil) != test.wantErr {
				t.Fatalf("parseDSN(%q) error = %v, wantErr %v", test.dsn, err, test.wantErr)
			}
			if test.wantErr {
				return
			}
			if endpoint != test.wantEndpoint {
				t.Errorf("endpoint = %q, want %q", endpoint, test.wantEndpoint)
			}
			if token != test.wantToken {
				t.Errorf("token = %q, want %q", token, test.wantToken)
			}
		})
	}
}

func TestConnectionStringErrorsDoNotExposeCredentials(t *testing.T) {
	const marker = "credential-marker-7462"
	dsns := []string{
		"mqlite://" + marker + "%ZZ@broker",
		"mqlite://" + marker + ":password@broker",
		"mqlite://" + marker + "@broker/path",
		"mqlite://" + marker + "@broker?tls=true&bad=%ZZ",
	}
	for _, dsn := range dsns {
		checks := []struct {
			name string
			call func() error
		}{
			{name: "parse", call: func() error { _, _, err := parseDSN(dsn); return err }},
			{name: "open", call: func() error { _, err := Open(context.Background(), dsn); return err }},
			{name: "identity", call: func() error { _, err := EndpointIdentity(dsn); return err }},
		}
		for _, check := range checks {
			t.Run(check.name, func(t *testing.T) {
				err := check.call()
				if err == nil {
					t.Fatal("invalid credential-bearing connection string was accepted")
				}
				if strings.Contains(err.Error(), marker) {
					t.Fatal("connection string error exposed credential material")
				}
			})
		}
	}
}

func TestOpenTokenOverrideAndSendWireContract(t *testing.T) {
	var requestErr error
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			requestErr = fmt.Errorf("method = %s", r.Method)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if r.URL.Path != wire.PathSend {
			requestErr = fmt.Errorf("path = %q", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if got := r.Header.Get("Authorization"); got != "Bearer option-token" {
			requestErr = fmt.Errorf("Authorization = %q", got)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			requestErr = fmt.Errorf("Content-Type = %q", got)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if got := r.Header.Get("Accept"); got != "application/json" {
			requestErr = fmt.Errorf("Accept = %q", got)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var request wire.SendRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			requestErr = err
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if request.Queue != "jobs" || len(request.Messages) != 1 || string(request.Messages[0].Body) != "payload" || request.TTLMs != 1500 {
			requestErr = fmt.Errorf("unexpected request: %+v", request)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"seq_numbers":[41]}`)
	}))
	defer server.Close()

	client, err := Open(context.Background(), "mqlite://dsn-token@unused", WithToken("option-token"))
	if err != nil {
		t.Fatal(err)
	}
	client.endpoint = server.URL

	sequence, err := client.SendOne(context.Background(), "jobs", OutMessage{Body: []byte("payload"), TTL: 1500 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if requestErr != nil {
		t.Fatal(requestErr)
	}
	if sequence != 41 {
		t.Fatalf("sequence = %d, want 41", sequence)
	}
}

func TestHTTPErrorMapping(t *testing.T) {
	tests := []struct {
		name      string
		status    int
		body      string
		want      error
		deleteSub bool
	}{
		{name: "unauthorized status wins", status: http.StatusUnauthorized, body: `{"code":"other","message":"bad token"}`, want: ErrUnauthenticated},
		{name: "forbidden status wins", status: http.StatusForbidden, body: `{"code":"other","message":"not allowed"}`, want: ErrPermissionDenied},
		{name: "unknown route", status: http.StatusNotFound, body: `{"message":"no such path /new"}`, want: ErrUnsupported},
		{name: "ordinary not found", status: http.StatusNotFound, body: `{"code":"not_found","message":"missing"}`, want: ErrNotFound},
		{name: "subscription not found", status: http.StatusNotFound, body: `{"code":"not_found","message":"missing"}`, want: ErrSubscriptionNotFound, deleteSub: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(test.status)
				_, _ = io.WriteString(w, test.body)
			}))
			defer server.Close()
			client, err := Open(context.Background(), server.URL)
			if err != nil {
				t.Fatal(err)
			}
			if test.deleteSub {
				_, err = client.DeleteSubscriptionWithOptions(context.Background(), "events", "worker")
			} else {
				_, err = client.SendOne(context.Background(), "jobs", OutMessage{Body: []byte("x")})
			}
			if !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want errors.Is(_, %v)", err, test.want)
			}
		})
	}
}

func TestDeleteDoesNotFollowRedirect(t *testing.T) {
	var destinationHits int32
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&destinationHits, 1)
		_, _ = io.WriteString(w, `{"deleted":true,"state":"deleted"}`)
	}))
	defer destination.Close()

	var sourceHits int32
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&sourceHits, 1)
		http.Redirect(w, r, destination.URL+wire.PathDeleteQueue, http.StatusTemporaryRedirect)
	}))
	defer source.Close()

	client, err := Open(context.Background(), source.URL)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.DeleteQueueWithOptions(context.Background(), "jobs", DeleteQueueOpts{Force: true})
	if !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("error = %v, want ErrOutcomeUnknown", err)
	}
	if got := atomic.LoadInt32(&sourceHits); got != 1 {
		t.Fatalf("source requests = %d, want 1", got)
	}
	if got := atomic.LoadInt32(&destinationHits); got != 0 {
		t.Fatalf("redirect destination requests = %d, want 0", got)
	}
}

func TestDeleteAmbiguousOutcomes(t *testing.T) {
	tests := []struct {
		name   string
		status int
		body   string
	}{
		{name: "server error", status: http.StatusServiceUnavailable, body: `{"code":"unavailable","message":"try later"}`},
		{name: "invalid acknowledgement", status: http.StatusOK, body: `{}`},
		{name: "oversized acknowledgement", status: http.StatusOK, body: strings.Repeat("x", wire.MaxDeleteResponseBytes+1)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var requests int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				atomic.AddInt32(&requests, 1)
				w.WriteHeader(test.status)
				_, _ = io.WriteString(w, test.body)
			}))
			defer server.Close()
			client, err := Open(context.Background(), server.URL)
			if err != nil {
				t.Fatal(err)
			}
			_, err = client.DeleteQueueWithOptions(context.Background(), "jobs")
			if !errors.Is(err, ErrOutcomeUnknown) {
				t.Fatalf("error = %v, want ErrOutcomeUnknown", err)
			}
			if got := atomic.LoadInt32(&requests); got != 1 {
				t.Fatalf("requests = %d, want 1", got)
			}
		})
	}
}

type failingRoundTripper struct {
	calls int32
}

func (r *failingRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	atomic.AddInt32(&r.calls, 1)
	return nil, errors.New("connection reset")
}

func TestDeleteTransportFailureIsOutcomeUnknown(t *testing.T) {
	transport := &failingRoundTripper{}
	client, err := Open(context.Background(), "http://broker.invalid", WithHTTPClient(&http.Client{Transport: transport}))
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.DeleteSubscriptionWithOptions(context.Background(), "events", "worker", DeleteSubscriptionOpts{Force: true})
	if !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("error = %v, want ErrOutcomeUnknown", err)
	}
	if got := atomic.LoadInt32(&transport.calls); got != 1 {
		t.Fatalf("transport calls = %d, want 1", got)
	}
}

func TestDeleteSuccessWireContract(t *testing.T) {
	var requestErr error
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != wire.PathDeleteSubscription {
			requestErr = fmt.Errorf("path = %q", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		var request wire.DeleteSubscriptionRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			requestErr = err
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if request.Topic != "events" || request.Name != "worker" || !request.Force {
			requestErr = fmt.Errorf("unexpected request: %+v", request)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		_, _ = io.WriteString(w, `{"deleted":false,"state":"deleting"}`)
	}))
	defer server.Close()
	client, err := Open(context.Background(), server.URL)
	if err != nil {
		t.Fatal(err)
	}
	result, err := client.DeleteSubscriptionWithOptions(context.Background(), "events", "worker", DeleteSubscriptionOpts{Force: true})
	if err != nil {
		t.Fatal(err)
	}
	if requestErr != nil {
		t.Fatal(requestErr)
	}
	if result.Deleted || result.State != "deleting" {
		t.Fatalf("result = %+v", result)
	}
}

func TestReadBounded(t *testing.T) {
	data, err := readBounded(strings.NewReader("1234"), 4)
	if err != nil || string(data) != "1234" {
		t.Fatalf("at limit: data=%q error=%v", data, err)
	}
	if _, err := readBounded(strings.NewReader("12345"), 4); err == nil {
		t.Fatal("over limit: expected error")
	}
	readerErr := errors.New("read failed")
	if _, err := readBounded(errorReader{err: readerErr}, 4); !errors.Is(err, readerErr) {
		t.Fatalf("read error = %v, want %v", err, readerErr)
	}
}

func TestResponseLimitCoversBrokerPayloadBudget(t *testing.T) {
	// The broker bounds a Receive/Peek page after 32 MiB of raw message bodies.
	// []byte is base64 in JSON, so the client must admit that legal payload plus
	// framing and metadata rather than applying the same raw-byte limit again.
	const brokerRawBodyBudget = int64(32 << 20)
	base64Bytes := ((brokerRawBodyBudget + 2) / 3) * 4
	if maxResponseBytes <= base64Bytes {
		t.Fatalf("maxResponseBytes = %d, must exceed base64 payload size %d", maxResponseBytes, base64Bytes)
	}
}

type errorReader struct {
	err error
}

func (r errorReader) Read([]byte) (int, error) { return 0, r.err }
