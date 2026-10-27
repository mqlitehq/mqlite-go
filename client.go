package mqlite

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"strings"

	"github.com/mqlitehq/mqlite-go/internal/wire"
)

type invalidSuccessResponseError struct{ cause error }

func (e *invalidSuccessResponseError) Error() string {
	return "mqlite: invalid successful response: " + e.cause.Error()
}

func (e *invalidSuccessResponseError) Unwrap() error { return e.cause }

type ambiguousOutcomeError struct {
	operation string
	cause     error
}

func (e *ambiguousOutcomeError) Error() string {
	return fmt.Sprintf("%s: invalid %s acknowledgement: %v", ErrOutcomeUnknown, e.operation, e.cause)
}

func (e *ambiguousOutcomeError) Unwrap() error { return e.cause }

func (e *ambiguousOutcomeError) Is(target error) bool { return target == ErrOutcomeUnknown }

const (
	defaultBrokerPort = "6754"
	// The broker caps the sum of raw message bodies at 32 MiB. JSON base64
	// encoding expands that payload by roughly one third, so the client needs a
	// larger bound to accept a valid maximum-size Receive or Peek response.
	maxResponseBytes = wire.MaxResponseBytes
	maxErrorBytes    = 64 << 10
)

// Client is a remote MQLite JSON-over-HTTP client.
type Client struct {
	endpoint string
	token    string
	http     *http.Client
}

type config struct {
	token      string
	httpClient *http.Client
}

// Option configures Open.
type Option func(*config)

// WithToken sets the Bearer token without putting it in the endpoint URL.
func WithToken(token string) Option { return func(c *config) { c.token = token } }

// WithHTTPClient supplies a custom transport, proxy, or TLS configuration.
// Do not set a global timeout shorter than the longest Receive wait.
// The transport must not retry queue or subscription deletion requests.
func WithHTTPClient(client *http.Client) Option {
	return func(c *config) { c.httpClient = client }
}

// Open creates a remote client. Supported connection strings are http(s) URLs
// and mqlite(s)://token@host[:port]. The context is retained for API parity with
// the main MQLite SDK; individual calls use their own contexts.
func Open(ctx context.Context, dsn string, opts ...Option) (*Client, error) {
	_ = ctx
	endpoint, token, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	cfg := config{}
	for _, option := range opts {
		if option != nil {
			option(&cfg)
		}
	}
	if cfg.token != "" {
		token = cfg.token
	}
	httpClient := cfg.httpClient
	if httpClient == nil {
		// Receive is a long poll. The request context, not a global client timeout,
		// owns its deadline.
		httpClient = &http.Client{}
	}
	return &Client{endpoint: endpoint, token: token, http: httpClient}, nil
}

// Close releases client resources. The standard HTTP client needs no close.
func (c *Client) Close() error { return nil }

// Endpoint returns the credential-free base URL this client dials.
func (c *Client) Endpoint() string { return c.endpoint }

// EndpointIdentity returns the exact credential-free base URL produced from dsn.
func EndpointIdentity(dsn string) (string, error) {
	endpoint, _, err := parseDSN(dsn)
	return endpoint, err
}

func parseDSN(dsn string) (string, string, error) {
	if dsn == "" {
		return "", "", fmt.Errorf("mqlite: empty connection string")
	}
	if strings.HasPrefix(dsn, "http://") || strings.HasPrefix(dsn, "https://") {
		u, err := url.Parse(dsn)
		if err != nil || u.Host == "" {
			return "", "", fmt.Errorf("mqlite: bad connection string")
		}
		if u.User != nil {
			return "", "", fmt.Errorf("mqlite: HTTP endpoint must not contain credentials; use WithToken")
		}
		if u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
			return "", "", fmt.Errorf("mqlite: endpoint must not contain a query or fragment")
		}
		return strings.TrimRight(dsn, "/"), "", nil
	}

	u, err := url.Parse(dsn)
	if err != nil {
		// url.Error includes the original URL. A custom DSN may contain a
		// bearer token in userinfo, so never return the parser's text.
		return "", "", fmt.Errorf("mqlite: bad connection string")
	}
	if u.Scheme != "mqlite" && u.Scheme != "mqlites" {
		return "", "", fmt.Errorf("mqlite: unsupported scheme %q", u.Scheme)
	}
	if u.Host == "" {
		return "", "", fmt.Errorf("mqlite: missing host in connection string")
	}
	if u.Path != "" || u.Fragment != "" {
		return "", "", fmt.Errorf("mqlite: custom connection string must contain only credentials and host")
	}
	if u.ForceQuery {
		return "", "", fmt.Errorf("mqlite: invalid custom connection string query")
	}
	query, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return "", "", fmt.Errorf("mqlite: invalid custom connection string query")
	}
	if len(query) > 1 || (len(query) == 1 && len(query["tls"]) != 1) {
		return "", "", fmt.Errorf("mqlite: custom connection string supports only one tls query parameter")
	}
	tls := false
	if value, ok := query["tls"]; ok {
		if value[0] != "true" && value[0] != "false" {
			return "", "", fmt.Errorf("mqlite: tls must be true or false")
		}
		tls = value[0] == "true"
	}
	token := ""
	if u.User != nil {
		if _, hasPassword := u.User.Password(); hasPassword {
			return "", "", fmt.Errorf("mqlite: custom connection string token must not use a password component")
		}
		token = u.User.Username()
	}
	scheme := "http"
	if u.Scheme == "mqlites" || tls {
		scheme = "https"
	}
	host := u.Host
	if u.Port() == "" {
		host = net.JoinHostPort(u.Hostname(), defaultBrokerPort)
	}
	return scheme + "://" + host, token, nil
}

func isLifecycleDelete(path string) bool {
	return path == wire.PathDeleteQueue || path == wire.PathDeleteSubscription
}

func reconcileHint(path string) string {
	if path == wire.PathDeleteSubscription {
		return "ListSubscriptions with IncludeDeleting"
	}
	return "ListQueues"
}

func readBounded(reader io.Reader, limit int64) ([]byte, error) {
	data, err := ioutil.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("mqlite: response exceeds %d bytes", limit)
	}
	return data, nil
}

func (c *Client) post(ctx context.Context, path string, body interface{}, out interface{}) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint+path, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	httpClient := c.http
	if isLifecycleDelete(path) {
		clone := *c.http
		clone.CheckRedirect = func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		}
		httpClient = &clone
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		if isLifecycleDelete(path) {
			return fmt.Errorf("%w: deletion response unavailable; inspect %s to reconcile: %v", ErrOutcomeUnknown, reconcileHint(path), err)
		}
		return err
	}
	defer resp.Body.Close()

	if isLifecycleDelete(path) && resp.StatusCode/100 == 3 {
		return fmt.Errorf("%w: deletion received HTTP redirect %d; inspect %s to reconcile", ErrOutcomeUnknown, resp.StatusCode, reconcileHint(path))
	}
	if isLifecycleDelete(path) && resp.StatusCode/100 == 5 {
		return fmt.Errorf("%w: deletion received HTTP %d; inspect %s to reconcile", ErrOutcomeUnknown, resp.StatusCode, reconcileHint(path))
	}
	if resp.StatusCode/100 != 2 {
		data, readErr := readBounded(resp.Body, maxErrorBytes)
		var errorBody wire.ErrorBody
		if readErr == nil {
			_ = json.Unmarshal(data, &errorBody)
		}
		if resp.StatusCode == http.StatusNotFound &&
			(errorBody.Code == "" || strings.HasPrefix(errorBody.Message, "no such path")) {
			return fmt.Errorf("%w: %s", ErrUnsupported, path)
		}
		if resp.StatusCode == http.StatusUnauthorized {
			errorBody.Code = "unauthenticated"
		}
		if resp.StatusCode == http.StatusForbidden {
			errorBody.Code = "permission_denied"
		}
		if path == wire.PathDeleteSubscription && errorBody.Code == "not_found" {
			return fmt.Errorf("%w: %s", ErrSubscriptionNotFound, errorBody.Message)
		}
		if path == wire.PathSubscribe && errorBody.Code == "resource_exhausted" {
			return fmt.Errorf("%w: %s", ErrSubscriptionLimit, errorBody.Message)
		}
		return mapError(errorBody)
	}

	if out == nil {
		return nil
	}
	var data []byte
	switch path {
	case wire.PathDeleteQueue, wire.PathDeleteSubscription:
		data, err = wire.ReadDeleteResponse(resp.Body)
	case wire.PathListSubscriptionsPage:
		data, err = wire.ReadListSubscriptionsPageResponse(resp.Body)
	default:
		data, err = readBounded(resp.Body, maxResponseBytes)
	}
	if err != nil {
		if isLifecycleDelete(path) {
			return fmt.Errorf("%w: invalid deletion acknowledgement; inspect %s to reconcile", ErrOutcomeUnknown, reconcileHint(path))
		}
		return &invalidSuccessResponseError{cause: err}
	}
	if raw, ok := out.(*json.RawMessage); ok {
		*raw = append((*raw)[:0], data...)
		return nil
	}
	if err := json.Unmarshal(data, out); err != nil {
		return &invalidSuccessResponseError{cause: err}
	}
	return nil
}

func mapError(body wire.ErrorBody) error {
	var target error
	switch body.Code {
	case "not_found":
		target = ErrNotFound
	case "already_exists":
		target = ErrDedupConflict
	case "queue_not_empty":
		target = ErrQueueNotEmpty
	case "subscription_not_empty":
		target = ErrSubscriptionNotEmpty
	case "name_conflict":
		target = ErrNameConflict
	case "group_required":
		target = ErrGroupRequired
	case "invalid_argument":
		target = ErrInvalidArgument
	case "message_too_large":
		target = ErrMessageTooLarge
	case "lock_lost":
		target = ErrLockLost
	case "unauthenticated":
		target = ErrUnauthenticated
	case "permission_denied":
		target = ErrPermissionDenied
	case "key_conflict":
		target = ErrKeyConflict
	case "outcome_unknown":
		target = ErrOutcomeUnknown
	default:
		if body.Code == "" {
			return fmt.Errorf("mqlite: request failed")
		}
		return fmt.Errorf("mqlite: %s: %s", body.Code, body.Message)
	}
	return fmt.Errorf("%w: %s", target, body.Message)
}
