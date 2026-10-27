package wire

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestDeleteResponseValidation(t *testing.T) {
	valid := []struct {
		json    string
		deleted bool
		state   string
	}{
		{`{"deleted":true,"state":"deleted"}`, true, "deleted"},
		{`{"deleted":false,"state":"deleting","future":1}`, false, "deleting"},
	}
	for _, tc := range valid {
		got, err := DecodeDeleteResponse([]byte(tc.json))
		if err != nil || got.Deleted != tc.deleted || got.State != tc.state {
			t.Fatalf("DecodeDeleteResponse(%s) = %+v, %v", tc.json, got, err)
		}
	}

	invalid := []string{
		`{}`, `null`, `[]`, `{"deleted":true}`, `{"state":"deleted"}`,
		`{"deleted":null,"state":"deleted"}`,
		`{"deleted":true,"state":null}`,
		`{"deleted":true,"state":"deleting"}`,
		`{"deleted":false,"state":"deleted"}`,
		`{"deleted":"true","state":"deleted"}`,
	}
	for _, input := range invalid {
		if got, err := DecodeDeleteResponse([]byte(input)); err == nil {
			t.Fatalf("DecodeDeleteResponse(%s) = %+v, want error", input, got)
		}
	}

	if _, err := ReadDeleteResponse(strings.NewReader(strings.Repeat("x", MaxDeleteResponseBytes+1))); err == nil {
		t.Fatal("oversized delete response accepted")
	}
}

func TestSubscriptionPageValidation(t *testing.T) {
	request := ListSubscriptionsPageRequest{Topic: "orders", Limit: 2, IncludeDeleting: true}
	data := []byte(`{
		"subscriptions":[
			{"topic":"orders","name":"a","expr":"","state":"active","created_at_ms":100,"expires_at_ms":0},
			{"topic":"orders","name":"b","expr":"subject == 'x'","state":"deleting","created_at_ms":200,"expires_at_ms":300}
		],
		"total":3,
		"next_after":"cursor-2",
		"future":true
	}`)
	page, err := DecodeListSubscriptionsPageResponse(data, request)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Subscriptions) != 2 || page.Total != 3 || page.NextAfter != "cursor-2" {
		t.Fatalf("unexpected page: %+v", page)
	}
	if page.Subscriptions[1].State != "deleting" || page.Subscriptions[1].ExpiresAtMs != 300 {
		t.Fatalf("unexpected second row: %+v", page.Subscriptions[1])
	}
	empty, err := DecodeListSubscriptionsPageResponse([]byte(`{"subscriptions":[],"total":0}`), ListSubscriptionsPageRequest{})
	if err != nil || empty.Subscriptions == nil || len(empty.Subscriptions) != 0 {
		t.Fatalf("empty page = %+v, %v", empty, err)
	}

	bad := []struct {
		name    string
		request ListSubscriptionsPageRequest
		json    string
	}{
		{"missing subscriptions", request, `{"total":0}`},
		{"null subscriptions", request, `{"subscriptions":null,"total":0}`},
		{"too many", ListSubscriptionsPageRequest{Limit: 1}, `{"subscriptions":[{"topic":"a","name":"a","state":"active","created_at_ms":1,"expires_at_ms":0},{"topic":"a","name":"b","state":"active","created_at_ms":1,"expires_at_ms":0}],"total":2}`},
		{"wrong topic", request, `{"subscriptions":[{"topic":"other","name":"a","state":"active","created_at_ms":1,"expires_at_ms":0}],"total":1}`},
		{"deleting hidden", ListSubscriptionsPageRequest{Limit: 1}, `{"subscriptions":[{"topic":"a","name":"a","state":"deleting","created_at_ms":1,"expires_at_ms":0}],"total":1}`},
		{"duplicate global name", ListSubscriptionsPageRequest{Limit: 2, IncludeDeleting: true}, `{"subscriptions":[{"topic":"a","name":"same","state":"active","created_at_ms":1,"expires_at_ms":0},{"topic":"b","name":"same","state":"active","created_at_ms":1,"expires_at_ms":0}],"total":2}`},
		{"out of order", ListSubscriptionsPageRequest{Limit: 2}, `{"subscriptions":[{"topic":"b","name":"b","state":"active","created_at_ms":1,"expires_at_ms":0},{"topic":"a","name":"a","state":"active","created_at_ms":1,"expires_at_ms":0}],"total":2}`},
		{"invalid lifetime", ListSubscriptionsPageRequest{Limit: 1}, `{"subscriptions":[{"topic":"a","name":"a","state":"active","created_at_ms":10,"expires_at_ms":10}],"total":1}`},
		{"nonprogress cursor", ListSubscriptionsPageRequest{After: "same", Limit: 1}, `{"subscriptions":[{"topic":"a","name":"a","state":"active","created_at_ms":1,"expires_at_ms":0}],"total":2,"next_after":"same"}`},
		{"short cursor page", ListSubscriptionsPageRequest{After: "old", Limit: 2}, `{"subscriptions":[{"topic":"a","name":"a","state":"active","created_at_ms":1,"expires_at_ms":0}],"total":2,"next_after":"next"}`},
	}
	for _, tc := range bad {
		t.Run(tc.name, func(t *testing.T) {
			if got, err := DecodeListSubscriptionsPageResponse([]byte(tc.json), tc.request); err == nil {
				t.Fatalf("accepted invalid page: %+v", got)
			}
		})
	}

	if _, err := ReadListSubscriptionsPageResponse(strings.NewReader(strings.Repeat("x", MaxListSubscriptionsPageResponseBytes+1))); err == nil {
		t.Fatal("oversized subscription page accepted")
	}
	invalidUTF8 := []byte("{\"subscriptions\":[{\"topic\":\"events\",\"name\":\"")
	invalidUTF8 = append(invalidUTF8, 0xff)
	invalidUTF8 = append(invalidUTF8, []byte("\",\"state\":\"active\",\"created_at_ms\":1,\"expires_at_ms\":0}],\"total\":1}")...)
	if _, err := DecodeListSubscriptionsPageResponse(invalidUTF8, ListSubscriptionsPageRequest{}); err == nil {
		t.Fatal("invalid UTF-8 subscription page accepted")
	}
}

func TestWireJSONShape(t *testing.T) {
	req := SubscribeWithOptionsRequest{
		Topic: "events", Name: "worker", Filter: &Filter{Expr: `subject == "paid"`}, TTLMs: 60000,
	}
	data, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range [][]byte{
		[]byte(`"topic":"events"`), []byte(`"name":"worker"`),
		[]byte(`"filter":{"expr":"subject == \"paid\""}`), []byte(`"ttl_ms":60000`),
	} {
		if !bytes.Contains(data, want) {
			t.Fatalf("%s missing %s", data, want)
		}
	}

	mixed := SendWithMessageTTLsRequest{Queue: "q", Messages: []SendMessage{{Message: Message{Body: []byte("x")}, TTLMs: 1}}}
	data, err = json.Marshal(mixed)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(data, []byte(`"body":"eA=="`)) || !bytes.Contains(data, []byte(`"ttl_ms":1`)) {
		t.Fatalf("mixed TTL JSON = %s", data)
	}
}
