package wire

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"unicode/utf8"
)

var (
	errDeleteResponse           = errors.New("mqlite: invalid deletion response")
	errSubscriptionPageResponse = errors.New("mqlite: invalid subscription inventory response")
)

const (
	MaxDeleteResponseBytes                = 4 << 10
	MaxListSubscriptionsPageResponseBytes = 32 << 20
	maxSubscriptionCursorBytes            = 1 << 20
)

func readBounded(reader io.Reader, limit int64, invalid error) ([]byte, error) {
	data, err := ioutil.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil || int64(len(data)) > limit {
		return nil, invalid
	}
	return data, nil
}

// ReadDeleteResponse reads one complete, bounded lifecycle acknowledgement.
func ReadDeleteResponse(reader io.Reader) ([]byte, error) {
	return readBounded(reader, MaxDeleteResponseBytes, errDeleteResponse)
}

// DecodeDeleteResponse requires one of the only two valid lifecycle outcomes:
// deleted=true/state=deleted or deleted=false/state=deleting.
func DecodeDeleteResponse(data []byte) (DeleteResponse, error) {
	if len(data) > MaxDeleteResponseBytes {
		return DeleteResponse{}, errDeleteResponse
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(data, &fields) != nil || fields == nil {
		return DeleteResponse{}, errDeleteResponse
	}
	deletedJSON, hasDeleted := fields["deleted"]
	stateJSON, hasState := fields["state"]
	if !hasDeleted || !hasState || isNull(deletedJSON) || isNull(stateJSON) {
		return DeleteResponse{}, errDeleteResponse
	}
	var response DeleteResponse
	if json.Unmarshal(deletedJSON, &response.Deleted) != nil ||
		json.Unmarshal(stateJSON, &response.State) != nil ||
		(response.Deleted && response.State != "deleted") ||
		(!response.Deleted && response.State != "deleting") {
		return DeleteResponse{}, errDeleteResponse
	}
	return response, nil
}

// ReadListSubscriptionsPageResponse reads one complete, bounded inventory page.
func ReadListSubscriptionsPageResponse(reader io.Reader) ([]byte, error) {
	return readBounded(reader, MaxListSubscriptionsPageResponseBytes, errSubscriptionPageResponse)
}

func isNull(data []byte) bool {
	return bytes.Equal(bytes.TrimSpace(data), []byte("null"))
}

func requiredObject(data []byte, required ...string) (map[string]json.RawMessage, error) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(data, &fields) != nil || fields == nil {
		return nil, errSubscriptionPageResponse
	}
	for _, name := range required {
		value, ok := fields[name]
		if !ok || isNull(value) {
			return nil, errSubscriptionPageResponse
		}
	}
	return fields, nil
}

// DecodeListSubscriptionsPageResponse validates size, required fields, scope,
// lifecycle values, ordering, uniqueness, cursor progress, and first-page count.
func DecodeListSubscriptionsPageResponse(data []byte, request ListSubscriptionsPageRequest) (ListSubscriptionsPageResponse, error) {
	if len(data) > MaxListSubscriptionsPageResponseBytes || !utf8.Valid(data) || len(request.After) > maxSubscriptionCursorBytes {
		return ListSubscriptionsPageResponse{}, errSubscriptionPageResponse
	}
	limit := request.Limit
	if limit == 0 {
		limit = 100
	}
	if limit < 1 || limit > 1000 {
		return ListSubscriptionsPageResponse{}, errSubscriptionPageResponse
	}

	fields, err := requiredObject(data, "subscriptions", "total")
	if err != nil {
		return ListSubscriptionsPageResponse{}, err
	}
	var rawItems []json.RawMessage
	var total int
	if json.Unmarshal(fields["subscriptions"], &rawItems) != nil || rawItems == nil ||
		json.Unmarshal(fields["total"], &total) != nil || total < 0 ||
		total < len(rawItems) || len(rawItems) > limit {
		return ListSubscriptionsPageResponse{}, errSubscriptionPageResponse
	}

	page := ListSubscriptionsPageResponse{
		Subscriptions: make([]SubscriptionPageJSON, len(rawItems)),
		Total:         total,
	}
	var previousTopic, previousName string
	seenNames := make(map[string]bool, len(rawItems))
	for i, raw := range rawItems {
		itemFields, err := requiredObject(raw, "topic", "name", "state", "created_at_ms", "expires_at_ms")
		if err != nil {
			return ListSubscriptionsPageResponse{}, err
		}
		if expr, ok := itemFields["expr"]; ok && isNull(expr) {
			return ListSubscriptionsPageResponse{}, errSubscriptionPageResponse
		}
		var item SubscriptionPageJSON
		if json.Unmarshal(raw, &item) != nil || item.Topic == "" || item.Name == "" ||
			(item.State != "active" && item.State != "deleting") ||
			item.CreatedAtMs < 0 || item.ExpiresAtMs < 0 ||
			(item.ExpiresAtMs != 0 && item.ExpiresAtMs <= item.CreatedAtMs) ||
			(!request.IncludeDeleting && item.State != "active") ||
			(request.Topic != "" && item.Topic != request.Topic) ||
			seenNames[item.Name] ||
			(i > 0 && (item.Topic < previousTopic || (item.Topic == previousTopic && item.Name <= previousName))) {
			return ListSubscriptionsPageResponse{}, errSubscriptionPageResponse
		}
		page.Subscriptions[i] = item
		seenNames[item.Name] = true
		previousTopic, previousName = item.Topic, item.Name
	}

	if rawNext, ok := fields["next_after"]; ok {
		if isNull(rawNext) || json.Unmarshal(rawNext, &page.NextAfter) != nil {
			return ListSubscriptionsPageResponse{}, errSubscriptionPageResponse
		}
	}
	if page.NextAfter != "" && (len(page.NextAfter) > maxSubscriptionCursorBytes ||
		len(page.Subscriptions) != limit || page.NextAfter == request.After) {
		return ListSubscriptionsPageResponse{}, errSubscriptionPageResponse
	}
	if request.After == "" {
		expected := total
		if expected > limit {
			expected = limit
		}
		if len(page.Subscriptions) != expected || (page.NextAfter != "") != (total > limit) {
			return ListSubscriptionsPageResponse{}, errSubscriptionPageResponse
		}
	}
	return page, nil
}
