package checker

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func defaultOpts() Options {
	return Options{
		Timeout: 2 * time.Second,
		Retries: 0,
		Backoff: 10 * time.Millisecond,
	}
}

func TestCheck_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "hello world")
	}))
	defer srv.Close()

	r := Check(context.Background(), srv.Client(), srv.URL, defaultOpts())
	if !r.Up {
		t.Fatalf("expected up, got down: %s", r.Error)
	}
	if r.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", r.StatusCode)
	}
	if r.Attempts != 1 {
		t.Fatalf("expected 1 attempt, got %d", r.Attempts)
	}
}

func TestCheck_ExpectStatusMismatch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	opts := defaultOpts()
	opts.ExpectStatus = 200
	r := Check(context.Background(), srv.Client(), srv.URL, opts)
	if r.Up {
		t.Fatal("expected down on status mismatch")
	}
	if r.StatusCode != 500 {
		t.Fatalf("expected code 500 recorded, got %d", r.StatusCode)
	}
}

func TestCheck_ContainsSubstring(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"status":"healthy"}`)
	}))
	defer srv.Close()

	opts := defaultOpts()
	opts.Contains = "healthy"
	if r := Check(context.Background(), srv.Client(), srv.URL, opts); !r.Up {
		t.Fatalf("expected up when body contains substring: %s", r.Error)
	}

	opts.Contains = "not-present"
	if r := Check(context.Background(), srv.Client(), srv.URL, opts); r.Up {
		t.Fatal("expected down when body missing substring")
	}
}

func TestCheck_Timeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(2 * time.Second):
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()

	opts := defaultOpts()
	opts.Timeout = 100 * time.Millisecond
	start := time.Now()
	r := Check(context.Background(), srv.Client(), srv.URL, opts)
	if r.Up {
		t.Fatal("expected down on timeout")
	}
	if time.Since(start) > time.Second {
		t.Fatalf("timeout took too long: %v", time.Since(start))
	}
}

func TestCheck_RetriesThenSucceeds(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Fail the first two attempts, then succeed.
		if atomic.AddInt32(&calls, 1) < 3 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	opts := defaultOpts()
	opts.Retries = 3
	opts.ExpectStatus = 200
	r := Check(context.Background(), srv.Client(), srv.URL, opts)
	if !r.Up {
		t.Fatalf("expected up after retries: %s", r.Error)
	}
	if r.Attempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", r.Attempts)
	}
}

func TestCheck_RetriesExhausted(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	opts := defaultOpts()
	opts.Retries = 2
	opts.ExpectStatus = 200
	r := Check(context.Background(), srv.Client(), srv.URL, opts)
	if r.Up {
		t.Fatal("expected down after exhausting retries")
	}
	if r.Attempts != 3 {
		t.Fatalf("expected 3 attempts (1 + 2 retries), got %d", r.Attempts)
	}
}

func TestCheck_ContextCancellationStops(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled

	opts := defaultOpts()
	opts.Retries = 5
	opts.Backoff = 50 * time.Millisecond
	opts.ExpectStatus = 200
	r := Check(ctx, srv.Client(), srv.URL, opts)
	if r.Up {
		t.Fatal("expected down when context cancelled")
	}
	// With a pre-cancelled context, we should not have hammered the server
	// through all six attempts.
	if got := atomic.LoadInt32(&calls); got > 1 {
		t.Fatalf("expected at most 1 call with cancelled context, got %d", got)
	}
}
