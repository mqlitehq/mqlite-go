// Command mqlitec is a small diagnostic client for a remote MQLite broker.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/mqlitehq/mqlite-go"
	"github.com/mqlitehq/mqlite-go/internal/version"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	global := flag.NewFlagSet("mqlitec", flag.ContinueOnError)
	global.SetOutput(os.Stderr)
	endpoint := global.String("endpoint", os.Getenv("MQLITE_ENDPOINT"), "broker URL or mqlite:// connection string")
	token := global.String("token", os.Getenv("MQLITE_TOKEN"), "bearer token (prefer MQLITE_TOKEN)")
	global.Usage = usage
	if err := global.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	args = global.Args()
	if len(args) == 0 {
		usage()
		return errors.New("command is required")
	}
	command, args := args[0], args[1:]
	if command == "version" {
		if len(args) != 0 {
			return errors.New("usage: mqlitec version")
		}
		fmt.Println("mqlitec", version.Version)
		return nil
	}
	if command == "help" || command == "-h" || command == "--help" {
		usage()
		return nil
	}
	if *endpoint == "" {
		return errors.New("broker endpoint is required (--endpoint or MQLITE_ENDPOINT)")
	}

	ctx, cancel := signalContext()
	defer cancel()
	client, err := mqlite.Open(ctx, *endpoint, mqlite.WithToken(*token))
	if err != nil {
		return err
	}
	defer client.Close()

	switch command {
	case "status":
		if len(args) != 0 {
			return errors.New("usage: mqlitec status")
		}
		status, err := client.Status(ctx)
		if err != nil {
			return err
		}
		return writeJSON(status)
	case "send":
		return send(ctx, client, args)
	case "receive":
		return receive(ctx, client, args)
	case "subscribe":
		return subscribe(ctx, client, args)
	case "delete-subscription":
		return deleteSubscription(ctx, client, args)
	default:
		usage()
		return fmt.Errorf("unknown command %q", command)
	}
}

func signalContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	go func() {
		select {
		case <-ch:
			cancel()
		case <-ctx.Done():
		}
		signal.Stop(ch)
	}()
	return ctx, cancel
}

func send(ctx context.Context, client *mqlite.Client, args []string) error {
	fs := flag.NewFlagSet("send", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	messageID := fs.String("message-id", "", "deduplication/idempotency key")
	subject := fs.String("subject", "", "message subject")
	ttl := fs.Duration("ttl", 0, "message lifetime")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	pos := fs.Args()
	if len(pos) != 2 {
		return errors.New("usage: mqlitec send [flags] <queue-or-topic> <body>")
	}
	seq, err := client.SendOne(ctx, pos[0], mqlite.OutMessage{
		Body: []byte(pos[1]), MessageID: *messageID, Subject: *subject, TTL: *ttl,
	})
	if err != nil {
		return err
	}
	return writeJSON(struct {
		SequenceNumber int64 `json:"seq_number"`
	}{SequenceNumber: seq})
}

func receive(ctx context.Context, client *mqlite.Client, args []string) error {
	fs := flag.NewFlagSet("receive", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	max := fs.Int("max", 1, "maximum messages")
	wait := fs.Duration("wait", 0, "long-poll duration")
	noAck := fs.Bool("no-ack", false, "leave messages locked and print lock tokens")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	pos := fs.Args()
	if len(pos) != 1 {
		return errors.New("usage: mqlitec receive [flags] <queue-or-subscription>")
	}
	messages, err := client.Receive(ctx, pos[0], mqlite.RecvOpts{Max: *max, Wait: *wait})
	if err != nil {
		return err
	}
	type output struct {
		SequenceNumber int64             `json:"seq_number"`
		Body           string            `json:"body"`
		MessageID      string            `json:"message_id,omitempty"`
		Subject        string            `json:"subject,omitempty"`
		Properties     map[string]string `json:"properties,omitempty"`
		DeliveryCount  int               `json:"delivery_count"`
		LockedUntil    string            `json:"locked_until,omitempty"`
		LockToken      string            `json:"lock_token,omitempty"`
	}
	items := make([]output, len(messages))
	for i, message := range messages {
		items[i] = output{
			SequenceNumber: message.SequenceNumber,
			Body:           string(message.Body),
			MessageID:      message.MessageID,
			Subject:        message.Subject,
			Properties:     message.Properties,
			DeliveryCount:  message.DeliveryCount,
		}
		if !message.LockedUntil.IsZero() {
			items[i].LockedUntil = message.LockedUntil.UTC().Format(time.RFC3339Nano)
		}
		if *noAck {
			items[i].LockToken = message.LockToken()
			continue
		}
		if err := message.Complete(ctx); err != nil {
			return fmt.Errorf("complete sequence %d: %w", message.SequenceNumber, err)
		}
	}
	return writeJSON(struct {
		Messages []output `json:"messages"`
	}{Messages: items})
}

func subscribe(ctx context.Context, client *mqlite.Client, args []string) error {
	fs := flag.NewFlagSet("subscribe", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	expr := fs.String("expr", "", "expr-lang subscription filter")
	ttl := fs.Duration("ttl", 0, "fixed lifetime from first creation")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	pos := fs.Args()
	if len(pos) != 2 {
		return errors.New("usage: mqlitec subscribe [flags] <topic> <subscription>")
	}
	var filter *mqlite.Filter
	if *expr != "" {
		filter = &mqlite.Filter{Expr: *expr}
	}
	if err := client.SubscribeWithOptions(ctx, pos[0], pos[1], filter, mqlite.SubscribeOptions{TTL: *ttl}); err != nil {
		return err
	}
	return writeJSON(struct {
		OK bool `json:"ok"`
	}{OK: true})
}

func deleteSubscription(ctx context.Context, client *mqlite.Client, args []string) error {
	fs := flag.NewFlagSet("delete-subscription", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	force := fs.Bool("force", false, "discard retained messages")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	pos := fs.Args()
	if len(pos) != 2 {
		return errors.New("usage: mqlitec delete-subscription [--force] <topic> <subscription>")
	}
	result, err := client.DeleteSubscriptionWithOptions(ctx, pos[0], pos[1], mqlite.DeleteSubscriptionOpts{Force: *force})
	if err != nil {
		return err
	}
	return writeJSON(result)
}

func writeJSON(value interface{}) error {
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

func usage() {
	fmt.Fprintln(os.Stderr, strings.TrimSpace(`mqlitec `+version.Version+` — remote MQLite diagnostic client

usage: mqlitec [--endpoint URL] [--token TOKEN] <command> [flags]

commands:
  version
  status
  send [--message-id ID] [--subject SUBJECT] [--ttl DURATION] <queue-or-topic> <body>
  receive [--max N] [--wait DURATION] [--no-ack] <queue-or-subscription>
  subscribe [--expr EXPR] [--ttl DURATION] <topic> <subscription>
  delete-subscription [--force] <topic> <subscription>

environment: MQLITE_ENDPOINT, MQLITE_TOKEN

Global flags must appear before the command.`))
}
