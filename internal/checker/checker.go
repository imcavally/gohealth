// Package checker implements the concurrent HTTP health-checking core of
// gohealth: a single-endpoint check with retries/backoff, and a bounded
// worker pool that fans checks out to workers and fans results back in.
package checker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Options configures how each endpoint is checked. The zero value is not
// useful on its own; use sensible defaults from the caller (see cli.Run).
type Options struct {
	// Timeout bounds a single HTTP request (including body read). A value
	// of zero means no per-request timeout beyond the context deadline.
	Timeout time.Duration

	// Retries is the number of *additional* attempts after the first one.
	// Retries=2 means up to 3 attempts total.
	Retries int

	// Backoff is the base delay for exponential backoff between attempts.
	// The nth retry waits Backoff * 2^(n-1), e.g. 200ms, 400ms, 800ms...
	Backoff time.Duration

	// ExpectStatus is the HTTP status code considered healthy. If zero,
	// any 2xx response is considered healthy.
	ExpectStatus int

	// Contains, when non-empty, requires the response body to contain this
	// substring for the endpoint to be considered up.
	Contains string

	// Client is the HTTP client used for requests. If nil, a default
	// client honoring Timeout is created per pool.
	Client *http.Client

	// UserAgent sets the User-Agent header on outgoing requests.
	UserAgent string
}

// Result is the outcome of checking a single endpoint.
type Result struct {
	URL        string        `json:"url"`
	Up         bool          `json:"up"`
	StatusCode int           `json:"status_code,omitempty"`
	Latency    time.Duration `json:"-"`
	LatencyMS  float64       `json:"latency_ms"`
	Attempts   int           `json:"attempts"`
	Error      string        `json:"error,omitempty"`
	CheckedAt  time.Time     `json:"checked_at"`

	// index preserves the input ordering across the worker pool. It is
	// unexported so it never appears in JSON output.
	index int
}

// maxBodyRead caps how much of the response body we read when a substring
// match is requested, to avoid unbounded memory use on huge responses.
const maxBodyRead = 1 << 20 // 1 MiB

// Check performs a single endpoint check, applying retries with exponential
// backoff on transient failures. It respects ctx for cancellation between
// and during attempts.
func Check(ctx context.Context, client *http.Client, url string, opts Options) Result {
	res := Result{URL: url, CheckedAt: time.Now()}

	for attempt := 1; attempt <= opts.Retries+1; attempt++ {
		// Stop early if the caller cancelled between attempts.
		if err := ctx.Err(); err != nil {
			res.Error = err.Error()
			res.Attempts = attempt - 1
			return res
		}

		res.Attempts = attempt
		start := time.Now()
		status, err := doRequest(ctx, client, url, opts)
		res.Latency = time.Since(start)
		res.LatencyMS = float64(res.Latency) / float64(time.Millisecond)
		res.StatusCode = status

		if err == nil {
			res.Up = true
			res.Error = ""
			return res
		}

		res.Up = false
		res.Error = err.Error()

		// Do not keep retrying if the context is done or this was the
		// last attempt.
		if ctx.Err() != nil || attempt == opts.Retries+1 {
			return res
		}

		// Exponential backoff: base * 2^(attempt-1).
		delay := opts.Backoff * time.Duration(1<<(attempt-1))
		if !sleep(ctx, delay) {
			res.Error = ctx.Err().Error()
			return res
		}
	}
	return res
}

// doRequest performs one HTTP GET and validates the response against opts.
// A non-nil error means the endpoint is considered down for this attempt.
func doRequest(ctx context.Context, client *http.Client, url string, opts Options) (int, error) {
	reqCtx := ctx
	if opts.Timeout > 0 {
		var cancel context.CancelFunc
		reqCtx, cancel = context.WithTimeout(ctx, opts.Timeout)
		defer cancel()
	}

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
	if err != nil {
		return 0, fmt.Errorf("build request: %w", err)
	}
	if opts.UserAgent != "" {
		req.Header.Set("User-Agent", opts.UserAgent)
	}

	resp, err := client.Do(req)
	if err != nil {
		return 0, requestError(err)
	}
	defer resp.Body.Close()

	if !statusOK(resp.StatusCode, opts.ExpectStatus) {
		// Drain a little so the connection can be reused.
		_, _ = io.CopyN(io.Discard, resp.Body, 512)
		return resp.StatusCode, fmt.Errorf("unexpected status %d", resp.StatusCode)
	}

	if opts.Contains != "" {
		body, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyRead))
		if err != nil {
			return resp.StatusCode, fmt.Errorf("read body: %w", err)
		}
		if !strings.Contains(string(body), opts.Contains) {
			return resp.StatusCode, fmt.Errorf("body does not contain %q", opts.Contains)
		}
	}

	return resp.StatusCode, nil
}

// statusOK reports whether code satisfies the expectation. When expect is 0
// any 2xx is accepted; otherwise an exact match is required.
func statusOK(code, expect int) bool {
	if expect == 0 {
		return code >= 200 && code < 300
	}
	return code == expect
}

// requestError normalizes transport errors, distinguishing a context
// timeout/cancellation from other failures for clearer reporting.
func requestError(err error) error {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return errors.New("request timeout")
	case errors.Is(err, context.Canceled):
		return errors.New("canceled")
	default:
		return err
	}
}

// sleep waits for d or until ctx is done. It returns true if the full delay
// elapsed, and false if the context was cancelled first.
func sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
