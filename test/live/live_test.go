package live_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mqlitehq/mqlite-go"
)

func TestBrokerRoundTripCorruptedDelivery(t *testing.T) {
	endpoint, token := os.Getenv("MQLITE_TEST_ENDPOINT"), os.Getenv("MQLITE_TEST_TOKEN")
	if endpoint == "" || token == "" {
		t.Skip("set MQLITE_TEST_ENDPOINT and MQLITE_TEST_TOKEN for the live broker test")
	}
	target, err := url.Parse(endpoint)
	if err != nil {
		t.Fatal("invalid test endpoint")
	}
	for _, atMostOnce := range []bool{false, true} {
		t.Run(fmt.Sprint(atMostOnce), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			client, err := mqlite.Open(ctx, endpoint, mqlite.WithToken(token))
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			queue := fmt.Sprintf("sdk-corrupt-%d", time.Now().UnixNano())
			if err := client.CreateQueue(ctx, queue, mqlite.QueueConfig{}); err != nil {
				t.Fatal(err)
			}
			defer func() {
				cleanupCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
				defer stop()
				_, _ = client.DeleteQueueWithOptions(cleanupCtx, queue, mqlite.DeleteQueueOpts{Force: true})
			}()
			seq, err := client.SendOne(ctx, queue, mqlite.OutMessage{Body: []byte("retained payload")})
			if err != nil {
				t.Fatal(err)
			}
			reverse := httputil.NewSingleHostReverseProxy(target)
			var receives int32
			reverse.ModifyResponse = func(resp *http.Response) error {
				if strings.HasSuffix(resp.Request.URL.Path, "/Receive") && atomic.AddInt32(&receives, 1) == 1 {
					_ = resp.Body.Close()
					const damaged = `{"messages":[null]}`
					resp.Body = io.NopCloser(strings.NewReader(damaged))
					resp.ContentLength = int64(len(damaged))
					resp.Header.Set("Content-Length", fmt.Sprint(len(damaged)))
				}
				return nil
			}
			proxy := httptest.NewServer(reverse)
			defer proxy.Close()
			proxied, err := mqlite.Open(ctx, proxy.URL, mqlite.WithToken(token))
			if err != nil {
				t.Fatal(err)
			}
			defer proxied.Close()
			opts := mqlite.RecvOpts{AtMostOnce: atMostOnce, Attempt: "corrupted-receive"}
			messages, err := proxied.Receive(ctx, queue, opts)
			if err == nil || len(messages) != 0 || errors.Is(err, mqlite.ErrOutcomeUnknown) != atMostOnce {
				t.Fatalf("corrupted committed receive = (%+v, %v)", messages, err)
			}
			stats, err := client.Stats(ctx, queue)
			if err != nil {
				t.Fatal(err)
			}
			if atMostOnce {
				if stats.Total != 0 {
					t.Fatalf("receive-and-delete did not commit: %+v", stats)
				}
			} else {
				if stats.Locked != 1 {
					t.Fatalf("damaged response did not preserve the original claim: %+v", stats)
				}
				messages, err = proxied.Receive(ctx, queue, opts)
				if err != nil || len(messages) != 1 || messages[0].SequenceNumber != seq || messages[0].DeliveryCount != 1 || string(messages[0].Body) != "retained payload" {
					t.Fatalf("same-attempt recovery = (%+v, %v)", messages, err)
				}
				if err := messages[0].Complete(ctx); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestBrokerRoundTrip(t *testing.T) {
	endpoint := os.Getenv("MQLITE_TEST_ENDPOINT")
	token := os.Getenv("MQLITE_TEST_TOKEN")
	if endpoint == "" || token == "" {
		t.Skip("set MQLITE_TEST_ENDPOINT and MQLITE_TEST_TOKEN for the live broker test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	client, err := mqlite.Open(ctx, endpoint, mqlite.WithToken(token))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	queue := "sdk-live-" + suffix
	topic := "sdk-topic-" + suffix
	subscription := "sdk-sub-" + suffix

	if err := client.CreateQueue(ctx, queue, mqlite.QueueConfig{LockDuration: 10 * time.Second}); err != nil {
		t.Fatalf("create queue: %v", err)
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = client.DeleteQueueWithOptions(cleanupCtx, queue, mqlite.DeleteQueueOpts{Force: true})
	}()
	seq, err := client.SendOne(ctx, queue, mqlite.OutMessage{
		Body: []byte("go-1.13-compatible"), MessageID: "message-" + suffix, Subject: "probe",
	})
	if err != nil || seq <= 0 {
		t.Fatalf("send: seq=%d err=%v", seq, err)
	}
	messages, err := client.Receive(ctx, queue, mqlite.RecvOpts{Max: 1, Wait: 5 * time.Second})
	if err != nil || len(messages) != 1 || string(messages[0].Body) != "go-1.13-compatible" {
		t.Fatalf("receive: messages=%d err=%v", len(messages), err)
	}
	if messages[0].LockToken() == "" {
		t.Fatal("receive returned no lock token")
	}
	if err := messages[0].Complete(ctx); err != nil {
		t.Fatalf("complete: %v", err)
	}
	stats, err := client.Stats(ctx, queue)
	if err != nil || stats.Total != 0 {
		t.Fatalf("stats after complete: %+v err=%v", stats, err)
	}

	if err := client.Subscribe(ctx, topic, subscription, nil); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = client.DeleteSubscriptionWithOptions(cleanupCtx, topic, subscription, mqlite.DeleteSubscriptionOpts{Force: true})
	}()
	const fanoutMessages = 16
	receiverCtx, stopReceiver := context.WithCancel(ctx)
	defer stopReceiver()
	received := make(chan string, fanoutMessages)
	receiverDone := make(chan error, 1)
	var receivedCount int32
	go func() {
		receiverDone <- client.Receiver(subscription,
			mqlite.WithConcurrency(4),
			mqlite.WithPrefetch(4),
			mqlite.WithAutoRenew(),
		).Run(receiverCtx, func(_ context.Context, message *mqlite.Message) error {
			received <- string(message.Body)
			if atomic.AddInt32(&receivedCount, 1) == fanoutMessages {
				stopReceiver()
			}
			return nil
		})
	}()
	published := make([]mqlite.OutMessage, fanoutMessages)
	for i := range published {
		published[i] = mqlite.OutMessage{
			Body:      []byte(fmt.Sprintf("fanout-%d", i)),
			MessageID: fmt.Sprintf("fanout-%s-%d", suffix, i),
		}
	}
	sequences, err := client.Send(ctx, topic, published...)
	if err != nil || len(sequences) != fanoutMessages {
		stopReceiver()
		t.Fatalf("publish: sequences=%d err=%v", len(sequences), err)
	}
	for i, sequence := range sequences {
		if sequence <= 0 {
			stopReceiver()
			t.Fatalf("publish sequence %d = %d, want positive fan-out acknowledgement", i, sequence)
		}
	}
	select {
	case err := <-receiverDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("subscription receiver: %v", err)
		}
	case <-ctx.Done():
		stopReceiver()
		t.Fatalf("subscription receiver timeout: %v", ctx.Err())
	}
	seen := make(map[string]int, fanoutMessages)
	for i := 0; i < fanoutMessages; i++ {
		seen[<-received]++
	}
	for i := 0; i < fanoutMessages; i++ {
		body := fmt.Sprintf("fanout-%d", i)
		if seen[body] != 1 {
			t.Fatalf("delivery count for %q = %d, want 1", body, seen[body])
		}
	}
	stats, err = client.Stats(ctx, subscription)
	if err != nil || stats.Total != 0 {
		t.Fatalf("subscription stats after receiver: %+v err=%v", stats, err)
	}
	page, err := client.ListSubscriptionsWithOptions(ctx, mqlite.ListSubscriptionsOptions{Topic: topic, Limit: 10})
	if err != nil || page.Total != 1 || len(page.Subscriptions) != 1 ||
		page.Subscriptions[0].Name != subscription || page.Subscriptions[0].ExpiresAtMs != 0 {
		t.Fatalf("subscription inventory: %+v err=%v", page, err)
	}
	deleted, err := client.DeleteSubscriptionWithOptions(ctx, topic, subscription)
	if err != nil || !deleted.Deleted || deleted.State != "deleted" {
		t.Fatalf("delete subscription: %+v err=%v", deleted, err)
	}

	status, err := client.Status(ctx)
	if err != nil || status.Version == "" || !status.Auth {
		t.Fatalf("status: %+v err=%v", status, err)
	}
}
