package wire

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestMessageResponseSequenceValidation(t *testing.T) {
	for _, body := range []string{
		`{"messages":[null]}`, `{"messages":[{}]}`,
		`{"messages":[{"seq_number":0}]}`, `{"messages":[{"seq_number":-1}]}`,
		`{"messages":[{"seq_number":7},null]}`,
		`{"messages":[{"seq_number":7,"lock_token":"same"},{"seq_number":7,"lock_token":"same"}]}`,
		`{"messages":[{"seq_number":7,"lock_token":"one"},{"seq_number":7,"lock_token":"two"}]}`,
	} {
		t.Run("receive "+body, func(t *testing.T) {
			var response ReceiveResponse
			if err := json.Unmarshal([]byte(body), &response); err == nil {
				t.Fatalf("invalid receive item accepted: %s", body)
			}
		})
	}
	for _, body := range []string{`{"messages":[]}`, `{"messages":[{"seq_number":7}]}`, `{"messages":[{"seq_number":7,"body":""}],"future":true}`} {
		var response ReceiveResponse
		if err := json.Unmarshal([]byte(body), &response); err != nil {
			t.Fatalf("valid empty body/list rejected: %s: %v", body, err)
		}
	}
	for _, body := range []string{`{"seq_numbers":[-1]}`, `{"seq_numbers":[0,-1]}`, `{"seq_numbers":[null]}`, `{"seq_numbers":[7,null]}`} {
		t.Run("send "+body, func(t *testing.T) {
			var response SendResponse
			if err := json.Unmarshal([]byte(body), &response); err == nil {
				t.Fatalf("invalid send sequence accepted: %s", body)
			}
		})
	}
	for _, body := range []string{`{"seq_numbers":[0]}`, `{"seq_numbers":[7,0]}`} {
		var response SendResponse
		if err := json.Unmarshal([]byte(body), &response); err != nil {
			t.Fatalf("valid topic/dedup zero rejected: %s: %v", body, err)
		}
	}
}

func TestErrorBodyResourceIsAdditive(t *testing.T) {
	data, err := json.Marshal(ErrorBody{Code: "not_found", Message: "missing", Resource: "queue"})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(data, []byte(`"resource":"queue"`)) {
		t.Fatalf("error body = %s, want resource discriminator", data)
	}
	var legacy struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(data, &legacy); err != nil || legacy.Code != "not_found" {
		t.Fatalf("legacy decode = %+v, %v", legacy, err)
	}
}

func TestSettleResponseRequiresExplicitOK(t *testing.T) {
	valid := []struct {
		json string
		ok   bool
	}{
		{`{"ok":true}`, true},
		{`{"ok":false,"future":"ignored"}`, false},
	}
	for _, tc := range valid {
		var response SettleResponse
		if err := json.Unmarshal([]byte(tc.json), &response); err != nil || response.Ok != tc.ok {
			t.Fatalf("Unmarshal(%s) = %+v, %v", tc.json, response, err)
		}
	}
	for _, input := range []string{`{}`, `null`, `{"ok":null}`, `{"ok":"true"}`} {
		var response SettleResponse
		if err := json.Unmarshal([]byte(input), &response); err == nil {
			t.Fatalf("Unmarshal(%s) = %+v, want error", input, response)
		}
	}
}

func TestSettleItemResultRequiresExplicitOK(t *testing.T) {
	valid := []struct {
		json string
		ok   bool
	}{
		{`{"seq_number":7,"ok":true,"locked_until_ms":99}`, true},
		{`{"seq_number":7,"ok":false,"future":"ignored"}`, false},
	}
	for _, tc := range valid {
		var result SettleItemResult
		if err := json.Unmarshal([]byte(tc.json), &result); err != nil || result.SeqNumber != 7 || result.Ok != tc.ok {
			t.Fatalf("Unmarshal(%s) = %+v, %v", tc.json, result, err)
		}
	}
	for _, input := range []string{`{}`, `null`, `{"seq_number":7}`, `{"seq_number":7,"ok":null}`, `{"seq_number":7,"ok":0}`} {
		var result SettleItemResult
		if err := json.Unmarshal([]byte(input), &result); err == nil {
			t.Fatalf("Unmarshal(%s) = %+v, want error", input, result)
		}
	}
}

func TestReceiveResponseRequiresNonNullMessages(t *testing.T) {
	for _, input := range []string{`{"messages":[]}`, `{"messages":[],"future":"ignored"}`} {
		var response ReceiveResponse
		if err := json.Unmarshal([]byte(input), &response); err != nil || response.Messages == nil || len(response.Messages) != 0 {
			t.Fatalf("Unmarshal(%s) = %+v, %v", input, response, err)
		}
	}
	for _, input := range []string{`{}`, `null`, `{"messages":null}`, `{"messages":{}}`} {
		var response ReceiveResponse
		if err := json.Unmarshal([]byte(input), &response); err == nil {
			t.Fatalf("Unmarshal(%s) = %+v, want error", input, response)
		}
	}
}

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
