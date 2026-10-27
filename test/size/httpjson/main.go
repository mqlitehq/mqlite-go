package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
)

func main() {
	endpoint := strings.TrimRight(os.Getenv("MQLITE_ENDPOINT"), "/")
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
		endpoint+"/mqlite.v1.AdminService/Status", bytes.NewBufferString("{}"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if token := os.Getenv("MQLITE_TOKEN"); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return
	}
	var status struct {
		Version string `json:"version"`
	}
	if json.NewDecoder(resp.Body).Decode(&status) == nil {
		fmt.Println(status.Version)
	}
}
