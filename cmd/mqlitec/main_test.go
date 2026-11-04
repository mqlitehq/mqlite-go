package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
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
	if output != "mqlitec 0.1.1\n" {
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

func TestReceiveCommandWritesWholeBatchBeforeSettlement(t *testing.T) {
	output, err := ioutil.TempFile("", "mqlitec-receive-output-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(output.Name())
	defer output.Close()
	old := os.Stdout
	os.Stdout = output
	defer func() { os.Stdout = old }()
	var completed int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/mqlite.v1.QueueService/Receive":
			_, _ = w.Write([]byte(`{"messages":[{"seq_number":5,"body":"aGVsbG8=","lock_token":"lock-5"},{"seq_number":6,"body":"d29ybGQ=","lock_token":"lock-6"}]}`))
		case "/mqlite.v1.QueueService/Complete":
			data, readErr := ioutil.ReadFile(output.Name())
			var result struct {
				Messages []struct{ Body string } `json:"messages"`
			}
			if readErr != nil || json.Unmarshal(data, &result) != nil || len(result.Messages) != 2 ||
				result.Messages[0].Body != "hello" || result.Messages[1].Body != "world" {
				t.Errorf("Complete began before the complete batch was written: output=%q, read error=%v", data, readErr)
			}
			if bytes.Contains(data, []byte("lock-")) {
				t.Error("auto-ack output exposed lock tokens")
			}
			atomic.AddInt32(&completed, 1)
			_, _ = w.Write([]byte(`{"ok":true}`))
		default:
			t.Errorf("unexpected path %q", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	if err := run([]string{"--endpoint", server.URL, "receive", "--max", "2", "orders"}); err != nil {
		t.Fatal(err)
	}
	if got := atomic.LoadInt32(&completed); got != 2 {
		t.Fatalf("Complete calls = %d, want 2", got)
	}
}

func TestReceiveCommandOutputFailureDoesNotComplete(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(fmt.Sprintf("partial=%v", partial), func(t *testing.T) {
			reader, writer, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			defer writer.Close()
			readDone := make(chan struct{})
			if partial {
				go func() {
					defer close(readDone)
					defer reader.Close()
					var prefix [32]byte
					_, _ = reader.Read(prefix[:])
				}()
			} else {
				_ = reader.Close()
				close(readDone)
			}
			old := os.Stdout
			os.Stdout = writer
			defer func() { os.Stdout = old }()
			var completed int32
			// Exceed even the usual Linux 1 MiB pipe-capacity ceiling so the
			// partial reader must close before the complete batch can be written.
			body := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte("x"), 2*1024*1024))
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/mqlite.v1.QueueService/Receive":
					_, _ = fmt.Fprintf(w, `{"messages":[{"seq_number":5,"body":%q,"lock_token":"lock-5"},{"seq_number":6,"body":"d29ybGQ=","lock_token":"lock-6"}]}`, body)
				case "/mqlite.v1.QueueService/Complete":
					atomic.AddInt32(&completed, 1)
					_, _ = w.Write([]byte(`{"ok":true}`))
				default:
					t.Errorf("unexpected path %q", r.URL.Path)
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			if err := run([]string{"--endpoint", server.URL, "receive", "--max", "2", "orders"}); err == nil {
				t.Fatal("receive with broken output succeeded")
			}
			<-readDone
			if got := atomic.LoadInt32(&completed); got != 0 {
				t.Fatalf("Complete calls after output failure = %d, want 0", got)
			}
		})
	}
}

func TestReceiveCommandRefusesUnusableStdoutBeforeClaim(t *testing.T) {
	for _, closed := range []bool{false, true} {
		t.Run(fmt.Sprintf("closed=%v", closed), func(t *testing.T) {
			output, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer output.Close()
			if closed {
				_ = output.Close()
			}
			old := os.Stdout
			os.Stdout = output
			defer func() { os.Stdout = old }()
			var calls int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				atomic.AddInt32(&calls, 1)
				_, _ = w.Write([]byte(`{"messages":[]}`))
			}))
			defer server.Close()
			if err := run([]string{"--endpoint", server.URL, "receive", "orders"}); err == nil {
				t.Fatal("receive accepted unusable stdout")
			}
			if got := atomic.LoadInt32(&calls); got != 0 {
				t.Fatalf("RPC calls with unusable stdout = %d, want 0", got)
			}
		})
	}
}

func TestReceiveCommandNoAckKeepsLockTokens(t *testing.T) {
	var completed int32
	output, err := runAgainst(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(r.URL.Path, "/Receive") {
			_, _ = w.Write([]byte(`{"messages":[{"seq_number":5,"body":"aGVsbG8=","lock_token":"lock-5"}]}`))
		} else {
			atomic.AddInt32(&completed, 1)
			_, _ = w.Write([]byte(`{"ok":true}`))
		}
	}), "receive", "--no-ack", "orders")
	if err != nil || !strings.Contains(output, "lock-5") || atomic.LoadInt32(&completed) != 0 {
		t.Fatalf("no-ack output=%q, err=%v, settlements=%d", output, err, completed)
	}
}

func TestReceiveCommandSettlementFailurePreservesPrintedBatch(t *testing.T) {
	output, err := runAgainst(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(r.URL.Path, "/Receive") {
			_, _ = w.Write([]byte(`{"messages":[{"seq_number":5,"body":"aGVsbG8=","lock_token":"lock-5"}]}`))
		} else {
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"code":"lock_lost","message":"lease expired"}`))
		}
	}), "receive", "orders")
	if err == nil || !strings.Contains(err.Error(), "complete sequence 5") || !strings.Contains(output, `"body": "hello"`) {
		t.Fatalf("output=%q, settlement error=%v", output, err)
	}
}

func TestReceiveCommandBrokenStdoutProcess(t *testing.T) {
	// Re-exec the test binary to exercise the real fd 1/SIGPIPE behavior without
	// killing the test runner. A broken pipe must never reach Complete.
	if endpoint := os.Getenv("MQLITEC_OUTPUT_TEST_ENDPOINT"); endpoint != "" {
		os.Args = []string{"mqlitec", "--endpoint", endpoint, "receive", "orders"}
		main()
		return
	}
	var received, completed int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(r.URL.Path, "/Receive") {
			atomic.AddInt32(&received, 1)
			_, _ = w.Write([]byte(`{"messages":[{"seq_number":5,"body":"aGVsbG8=","lock_token":"lock-5"}]}`))
		} else {
			atomic.AddInt32(&completed, 1)
			_, _ = w.Write([]byte(`{"ok":true}`))
		}
	}))
	defer server.Close()
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	_ = reader.Close()
	defer writer.Close()
	command := exec.Command(os.Args[0], "-test.run=^TestReceiveCommandBrokenStdoutProcess$")
	command.Env = append(os.Environ(), "MQLITEC_OUTPUT_TEST_ENDPOINT="+server.URL)
	command.Stdout = writer
	var stderr bytes.Buffer
	command.Stderr = &stderr
	if err := command.Run(); err == nil {
		t.Fatal("process with a broken stdout pipe exited successfully")
	}
	if atomic.LoadInt32(&received) != 1 || atomic.LoadInt32(&completed) != 0 {
		t.Fatalf("broken-pipe process Receive=%d, Complete=%d, stderr=%q", received, completed, stderr.String())
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
