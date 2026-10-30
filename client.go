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
	"strconv"
	"strings"
	"time"

	"github.com/mqlitehq/mqlite-go/internal/wire"
)

type invalidSuccessResponseError struct{ cause error }

func (e *invalidSuccessResponseError) Error() string {
	return "mqlite: invalid successful response: " + e.cause.Error()
}

func (e *invalidSuccessResponseError) Unwrap() error { return e.cause }

// HTTPError preserves the HTTP status returned by a broker or intermediary.
// Use errors.As to inspect it while errors.Is continues to match any wrapped
// MQLite sentinel.
type HTTPError struct {
	StatusCode   int
	cause        error
	retryAfter   time.Duration
	retryAfterOK bool
}

func (e *HTTPError) Error() string {
	if e.cause == nil {
		return fmt.Sprintf("mqlite: HTTP %d", e.StatusCode)
	}
	return e.cause.Error()
}

func (e *HTTPError) Unwrap() error { return e.cause }

// HTTPStatus returns the response status code.
func (e *HTTPError) HTTPStatus() int { return e.StatusCode }

func httpErrorFromResponse(resp *http.Response, receivedAt time.Time, cause error) *HTTPError {
	retryAfter, ok := parseRetryAfter(resp.Header, receivedAt)
	return &HTTPError{
		StatusCode:   resp.StatusCode,
		cause:        cause,
		retryAfter:   retryAfter,
		retryAfterOK: ok,
	}
}

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
	endpoint  string
	token     string
	http      *http.Client
	ownedHTTP bool
}

type serverClockObservation struct {
	localAt     time.Time
	serverUpper time.Time
	responseRTT time.Duration
	resolution  time.Duration
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
// The transport must honor request contexts and must not retry queue or
// subscription deletion requests. Size MaxIdleConnsPerHost to at least Receiver
// concurrency plus effective prefetch so overlapped settlement reuses connections.
func WithHTTPClient(client *http.Client) Option {
	return func(c *config) { c.httpClient = client }
}

// Open creates a remote client. Supported connection strings are http(s) URLs
// and mqlite(s)://token@host[:port]. The context is retained for API parity with
// the main MQLite SDK; individual calls use their own contexts. Open clones and
// owns the standard *http.Transport. If http.DefaultTransport was replaced by a
// wrapper, Open preserves that wrapper and leaves its pool lifecycle to its owner.
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
	ownedHTTP := false
	if httpClient == nil {
		// Receive is a long poll. The request context, not a global client timeout,
		// owns its deadline. Clone the standard transport so managed receiver
		// concurrency has a useful per-host idle pool and Client.Close can release
		// only this client's connections.
		if base, ok := http.DefaultTransport.(*http.Transport); ok {
			transport := base.Clone()
			transport.MaxIdleConnsPerHost = 64
			if transport.MaxIdleConns < transport.MaxIdleConnsPerHost {
				transport.MaxIdleConns = transport.MaxIdleConnsPerHost
			}
			httpClient = &http.Client{Transport: transport}
			ownedHTTP = true
		} else {
			// Instrumentation and HTTP test packages commonly wrap the process-wide
			// transport. Preserve that behavior instead of making an SDK upgrade a
			// startup failure. The wrapper and its connection pool remain externally
			// owned, exactly as they are for a caller-supplied HTTP client.
			httpClient = &http.Client{Transport: http.DefaultTransport}
		}
	}
	// RPC endpoints never redirect. A 307/308 would replay a POST body and can
	// forward Authorization to another port on the same host. Clone the caller's
	// client so enforcing this policy does not mutate shared configuration.
	clientCopy := *httpClient
	clientCopy.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return &Client{endpoint: endpoint, token: token, http: &clientCopy, ownedHTTP: ownedHTTP}, nil
}

// Close releases idle connections owned by this client. A caller-supplied HTTP
// client and its transport remain under the caller's ownership.
func (c *Client) Close() error {
	if c.ownedHTTP {
		c.http.CloseIdleConnections()
	}
	return nil
}

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

func readResponseBounded(reader io.Reader, contentLength, limit int64) ([]byte, error) {
	if contentLength > limit {
		return nil, fmt.Errorf("mqlite: response exceeds %d bytes", limit)
	}
	return readBounded(reader, limit)
}

func (c *Client) post(ctx context.Context, path string, body interface{}, out interface{}) error {
	return c.postWithObservation(ctx, path, body, out, nil)
}

func (c *Client) postObserved(ctx context.Context, path string, body interface{}, out interface{}) (serverClockObservation, error) {
	var observation serverClockObservation
	err := c.postWithObservation(ctx, path, body, out, &observation)
	return observation, err
}

func (c *Client) postWithObservation(ctx context.Context, path string, body interface{}, out interface{}, observation *serverClockObservation) error {
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

	started := time.Now()
	resp, err := c.http.Do(req)
	if err != nil {
		if isLifecycleDelete(path) {
			return fmt.Errorf("%w: deletion response unavailable; inspect %s to reconcile: %v", ErrOutcomeUnknown, reconcileHint(path), err)
		}
		return err
	}
	receivedAt := time.Now()
	if resp.StatusCode/100 == 2 && observation != nil {
		observed, _ := observeServerTime(resp.Header, started, receivedAt)
		// Even when neither clock header survived an intermediary, retain the
		// response timestamp and measured RTT. localLeaseDeadline still refuses to
		// map without serverUpper, while the Receiver can diagnose and enter its
		// bounded untrusted schedule.
		*observation = observed
	}
	defer resp.Body.Close()

	if isLifecycleDelete(path) && resp.StatusCode/100 == 3 {
		return fmt.Errorf("%w: deletion received HTTP redirect %d; inspect %s to reconcile", ErrOutcomeUnknown, resp.StatusCode, reconcileHint(path))
	}
	if isLifecycleDelete(path) && resp.StatusCode/100 == 5 {
		return fmt.Errorf("%w: deletion received HTTP %d; inspect %s to reconcile", ErrOutcomeUnknown, resp.StatusCode, reconcileHint(path))
	}
	if resp.StatusCode/100 == 3 {
		// Redirect metadata and bodies come from an untrusted intermediary. RPCs
		// never follow redirects, and ordinary calls expose only the status needed
		// for stable classification rather than reflecting Location or body text.
		return httpErrorFromResponse(resp, receivedAt, fmt.Errorf("mqlite: unexpected HTTP redirect %d", resp.StatusCode))
	}
	if resp.StatusCode/100 != 2 {
		data, readErr := readResponseBounded(resp.Body, resp.ContentLength, maxErrorBytes)
		var errorBody wire.ErrorBody
		if readErr == nil {
			_ = json.Unmarshal(data, &errorBody)
		}
		if resp.StatusCode == http.StatusNotFound &&
			(errorBody.Code == "" || strings.HasPrefix(errorBody.Message, "no such path")) {
			return httpErrorFromResponse(resp, receivedAt, fmt.Errorf("%w: %s", ErrUnsupported, path))
		}
		if resp.StatusCode == http.StatusUnauthorized {
			errorBody.Code = "unauthenticated"
		}
		if resp.StatusCode == http.StatusForbidden {
			errorBody.Code = "permission_denied"
		}
		if path == wire.PathDeleteSubscription && errorBody.Code == "not_found" {
			return httpErrorFromResponse(resp, receivedAt, fmt.Errorf("%w: %s", ErrSubscriptionNotFound, errorBody.Message))
		}
		if path == wire.PathSubscribe && errorBody.Code == "resource_exhausted" {
			return httpErrorFromResponse(resp, receivedAt, fmt.Errorf("%w: %s", ErrSubscriptionLimit, errorBody.Message))
		}
		return httpErrorFromResponse(resp, receivedAt, mapError(resp.StatusCode, errorBody))
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
		data, err = readResponseBounded(resp.Body, resp.ContentLength, maxResponseBytes)
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

// parseRetryAfter accepts both Retry-After forms. It stays transport-private:
// only managed Receive uses the parsed value, while exact renewal and settlement
// retry budgets remain unchanged. HTTP-date values are measured from the same
// response's Date header when available, avoiding local clock skew; local
// response receipt time is the fallback.
func parseRetryAfter(header http.Header, receivedAt time.Time) (time.Duration, bool) {
	value := strings.TrimSpace(header.Get("Retry-After"))
	if value == "" {
		return 0, false
	}

	digits := true
	for i := 0; i < len(value); i++ {
		if value[i] < '0' || value[i] > '9' {
			digits = false
			break
		}
	}
	if digits {
		seconds, err := strconv.ParseUint(value, 10, 64)
		const maxDuration = time.Duration(1<<63 - 1)
		if err != nil || seconds > uint64(maxDuration/time.Second) {
			return 0, false
		}
		return time.Duration(seconds) * time.Second, true
	}

	retryAt, err := http.ParseTime(value)
	if err != nil {
		return 0, false
	}
	base := receivedAt
	if responseDate, dateErr := http.ParseTime(header.Get("Date")); dateErr == nil {
		base = responseDate
	}
	if retryAt.Before(base) {
		return 0, false
	}
	seconds := retryAt.Unix() - base.Unix()
	nanos := int64(retryAt.Nanosecond() - base.Nanosecond())
	if nanos < 0 {
		seconds--
		nanos += int64(time.Second)
	}
	if seconds < 0 {
		return 0, false
	}
	const maxDuration = time.Duration(1<<63 - 1)
	maxSeconds := int64(maxDuration / time.Second)
	maxNanos := int64(maxDuration % time.Second)
	if seconds > maxSeconds || (seconds == maxSeconds && nanos > maxNanos) {
		return 0, false
	}
	return time.Duration(seconds)*time.Second + time.Duration(nanos), true
}

// observeServerTime converts broker response time into a conservative upper
// bound for server time when the response reached us. New brokers expose epoch
// milliseconds; older brokers fall back to HTTP's second-resolution Date. The
// full round trip is charged as uncertainty, even though only the response leg
// elapsed after the server generated the header.
func observeServerTime(header http.Header, started, received time.Time) (serverClockObservation, bool) {
	roundTrip := received.Sub(started)
	if roundTrip < 0 {
		roundTrip = 0
	}
	var serverTime time.Time
	resolution := time.Millisecond
	if value := header.Get(wire.ServerTimeHeader); value != "" {
		millis, err := strconv.ParseInt(value, 10, 64)
		if err == nil && millis > 0 {
			serverTime = time.Unix(millis/1000, (millis%1000)*int64(time.Millisecond))
		}
	}
	if serverTime.IsZero() {
		serverDate, err := http.ParseTime(header.Get("Date"))
		if err != nil {
			// Preserve the measured RTT even when an intermediary stripped both
			// clock headers. The managed Receiver cannot map the absolute lease in
			// this mode, but it can still retain transport diagnostics and must not
			// silently treat the raw broker timestamp as a local deadline.
			return serverClockObservation{localAt: received, responseRTT: roundTrip}, false
		}
		serverTime = serverDate
		resolution = time.Second
	}
	return serverClockObservation{
		localAt:     received,
		serverUpper: serverTime.Add(resolution).Add(roundTrip),
		responseRTT: roundTrip,
		resolution:  resolution,
	}, true
}

// localLeaseDeadline translates a broker wall-clock deadline onto the local
// monotonic clock. HTTP Date is available on old brokers, so this is a backward-
// compatible clock-skew fallback while a millisecond protocol field is absent.
func (observation serverClockObservation) localLeaseDeadline(remote time.Time) (time.Time, bool) {
	if remote.IsZero() {
		return time.Time{}, false
	}
	if observation.localAt.IsZero() || observation.serverUpper.IsZero() {
		return time.Time{}, false
	}
	now := time.Now()
	age := now.Sub(observation.localAt)
	if age < 0 {
		return time.Time{}, false
	}
	remaining := remote.Sub(observation.serverUpper.Add(age))
	return now.Add(remaining), true
}

func mapError(statusCode int, body wire.ErrorBody) error {
	var target error
	switch body.Code {
	case "not_found":
		if body.Resource == "queue" {
			target = ErrQueueNotFound
		} else if body.Resource == "subscription" {
			target = ErrSubscriptionNotFound
		} else {
			target = ErrNotFound
		}
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
			return fmt.Errorf("mqlite: request failed: HTTP %d", statusCode)
		}
		return fmt.Errorf("mqlite: %s: %s", body.Code, body.Message)
	}
	return fmt.Errorf("%w: %s", target, body.Message)
}
