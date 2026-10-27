package main

import (
	"context"
	"fmt"
	"os"

	"github.com/mqlitehq/mqlite-go"
)

func main() {
	client, err := mqlite.Open(context.Background(), os.Getenv("MQLITE_ENDPOINT"), mqlite.WithToken(os.Getenv("MQLITE_TOKEN")))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return
	}
	defer client.Close()
	status, err := client.Status(context.Background())
	if err == nil {
		fmt.Println(status.Version)
	}
}
