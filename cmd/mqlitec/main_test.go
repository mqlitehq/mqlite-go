package main

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func runAgainst(t *testing.T, handler http.Handler, args ...string) (string, error) {
	t.Helper()
	server := httptest.NewServer(handler)
	defer server.Close()
	command := append([]string{"--endpoint", server.URL}, args...)
	return captureStdout(t, func() error { return run(command) })
}

func captureStdout(t *testing.T, fn func() error) (string, error) {
	t.Helper()
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	old := os.Stdout
	os.Stdout = writer
	runErr := fn()
	_ = writer.Close()
	os.Stdout = old
	data, readErr := ioutil.ReadAll(reader)
	_ = reader.Close()
	if readErr != nil {
		t.Fatal(readErr)
	}
	return string(data), runErr
}

func TestVersion(t *testing.T) {
	output, err := captureStdout(t, func() error { return run([]string{"version"}) })
	if err != nil {
		t.Fatal(err)
	}
	if output != "mqlitec 0.1.0\n" {
		t.Fatalf("output = %q", output)
	}
}

func TestGlobalHelpIsSuccessful(t *testing.T) {
	if err := run([]string{"--help"}); err != nil {
		t.Fatalf("help: %v", err)
	}
}

func TestStatusUsesEndpointAndBearerToken(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/mqlite.v1.AdminService/Status" {
			t.Errorf("path = %q", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer secret" {
			t.Errorf("authorization = %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"version":"0.4.1","backend":"sqlite","remote":false,"location":"test","schema_version":"7","ping_ms":1,"db_size_bytes":0,"queues":0,"subscriptions":0,"uptime_ms":1,"auth":true}`))
	}))
	defer server.Close()

	output, err := captureStdout(t, func() error {
		return run([]string{"--endpoint", server.URL, "--token", "secret", "status"})
	})
	if err != nil {
		t.Fatal(err)
	}
	var value map[string]interface{}
	if err := json.Unmarshal([]byte(output), &value); err != nil {
		t.Fatalf("invalid JSON %q: %v", output, err)
	}
	if value["Version"] != "0.4.1" && value["version"] != "0.4.1" {
		t.Fatalf("unexpected status: %s", output)
	}
}

func TestSendRequiresFlagsBeforePositionals(t *testing.T) {
	err := run([]string{"--endpoint", "http://127.0.0.1:1", "send", "orders", "body", "--ttl", "1s"})
	if err == nil || !strings.Contains(err.Error(), "usage: mqlitec send") {
		t.Fatalf("error = %v", err)
	}
}

func TestSendCommand(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/mqlite.v1.QueueService/Send" {
			t.Errorf("path = %q", r.URL.Path)
		}
		var request struct {
			Queue    string `json:"queue"`
			Messages []struct {
				Body      []byte `json:"body"`
				MessageID string `json:"message_id"`
				Subject   string `json:"subject"`
			} `json:"messages"`
			TTLMs int64 `json:"ttl_ms"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode request: %v", err)
		}
		if request.Queue != "orders" || len(request.Messages) != 1 ||
			string(request.Messages[0].Body) != "payload" ||
			request.Messages[0].MessageID != "message-1" ||
			request.Messages[0].Subject != "created" || request.TTLMs != 3000 {
			t.Errorf("request = %+v", request)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"seq_numbers":[17]}`))
	})
	output, err := runAgainst(t, handler, "send", "--message-id", "message-1", "--subject", "created", "--ttl", "3s", "orders", "payload")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output, `"seq_number": 17`) {
		t.Fatalf("output = %q", output)
	}
}

func TestReceiveCommandCompletesMessages(t *testing.T) {
	received := 0
	completed := 0
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/mqlite.v1.QueueService/Receive":
			received++
			_, _ = w.Write([]byte(`{"messages":[{"seq_number":5,"body":"aGVsbG8=","message_id":"m-5","delivery_count":1,"lock_token":"lock-5"}]}`))
		case "/mqlite.v1.QueueService/Complete":
			completed++
			var request struct {
				Queue     string `json:"queue"`
				SeqNumber int64  `json:"seq_number"`
				LockToken string `json:"lock_token"`
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Errorf("decode complete: %v", err)
			}
			if request.Queue != "orders" || request.SeqNumber != 5 || request.LockToken != "lock-5" {
				t.Errorf("complete request = %+v", request)
			}
			_, _ = w.Write([]byte(`{"ok":true}`))
		default:
			t.Errorf("path = %q", r.URL.Path)
			http.NotFound(w, r)
		}
	})
	output, err := runAgainst(t, handler, "receive", "--max", "1", "orders")
	if err != nil {
		t.Fatal(err)
	}
	if received != 1 || completed != 1 || !strings.Contains(output, `"body": "hello"`) {
		t.Fatalf("received=%d completed=%d output=%q", received, completed, output)
	}
	if strings.Contains(output, "lock-5") {
		t.Fatalf("default output exposed lock token: %q", output)
	}
}

func TestSubscribeAndDeleteCommands(t *testing.T) {
	subscribed := 0
	deleted := 0
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/mqlite.v1.AdminService/Subscribe":
			subscribed++
			var request struct {
				Topic  string `json:"topic"`
				Name   string `json:"name"`
				TTLMs  int64  `json:"ttl_ms"`
				Filter struct {
					Expr string `json:"expr"`
				} `json:"filter"`
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Errorf("decode subscribe: %v", err)
			}
			if request.Topic != "events" || request.Name != "worker" || request.TTLMs != 90000 || request.Filter.Expr != "subject == 'created'" {
				t.Errorf("subscribe request = %+v", request)
			}
			_, _ = w.Write([]byte(`{}`))
		case "/mqlite.v1.AdminService/DeleteSubscription":
			deleted++
			var request struct {
				Topic string `json:"topic"`
				Name  string `json:"name"`
				Force bool   `json:"force"`
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Errorf("decode delete: %v", err)
			}
			if request.Topic != "events" || request.Name != "worker" || !request.Force {
				t.Errorf("delete request = %+v", request)
			}
			_, _ = w.Write([]byte(`{"deleted":true,"state":"deleted"}`))
		default:
			t.Errorf("path = %q", r.URL.Path)
			http.NotFound(w, r)
		}
	})
	if _, err := runAgainst(t, handler, "subscribe", "--expr", "subject == 'created'", "--ttl", "90s", "events", "worker"); err != nil {
		t.Fatal(err)
	}
	output, err := runAgainst(t, handler, "delete-subscription", "--force", "events", "worker")
	if err != nil {
		t.Fatal(err)
	}
	if subscribed != 1 || deleted != 1 || !strings.Contains(output, `"deleted": true`) {
		t.Fatalf("subscribed=%d deleted=%d output=%q", subscribed, deleted, output)
	}
}
