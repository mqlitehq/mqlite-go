package live_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/mqlitehq/mqlite-go"
)

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

	if err := client.SubscribeWithOptions(ctx, topic, subscription, nil, mqlite.SubscribeOptions{TTL: 2 * time.Minute}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = client.DeleteSubscriptionWithOptions(cleanupCtx, topic, subscription, mqlite.DeleteSubscriptionOpts{Force: true})
	}()
	if _, err := client.SendOne(ctx, topic, mqlite.OutMessage{Body: []byte("fanout")}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	messages, err = client.Receive(ctx, subscription, mqlite.RecvOpts{Max: 1, Wait: 5 * time.Second})
	if err != nil || len(messages) != 1 || string(messages[0].Body) != "fanout" {
		t.Fatalf("subscription receive: messages=%d err=%v", len(messages), err)
	}
	if err := messages[0].Complete(ctx); err != nil {
		t.Fatalf("complete subscription message: %v", err)
	}
	page, err := client.ListSubscriptionsWithOptions(ctx, mqlite.ListSubscriptionsOptions{Topic: topic, Limit: 10})
	if err != nil || page.Total != 1 || len(page.Subscriptions) != 1 || page.Subscriptions[0].Name != subscription {
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
