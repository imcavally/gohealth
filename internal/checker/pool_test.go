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

// TestPool_RespectsConcurrencyLimit verifies that no more than the configured
// number of checks run simultaneously, using a server that reports the peak
// number of concurrent in-flight requests it observed.
func TestPool_RespectsConcurrencyLimit(t *testing.T) {
	const limit = 3
	var current, peak int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cur := atomic.AddInt32(&current, 1)
		for {
			old := atomic.LoadInt32(&peak)
			if cur <= old || atomic.CompareAndSwapInt32(&peak, old, cur) {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
		atomic.AddInt32(&current, -1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	urls := make([]string, 20)
	for i := range urls {
		urls[i] = srv.URL
	}

	pool := NewPool(limit, Options{Timeout: 2 * time.Second, Client: srv.Client()})
	results := pool.Run(context.Background(), urls)

	if len(results) != len(urls) {
		t.Fatalf("expected %d results, got %d", len(urls), len(results))
	}
	if got := atomic.LoadInt32(&peak); got > limit {
		t.Fatalf("server observed %d concurrent requests, limit was %d", got, limit)
	}
	if got := pool.MaxObservedConcurrency(); got > limit {
		t.Fatalf("pool observed %d concurrent checks, limit was %d", got, limit)
	}
	if got := pool.MaxObservedConcurrency(); got == 0 {
		t.Fatal("expected non-zero observed concurrency")
	}
}

// TestPool_PreservesOrder ensures results come back in input order regardless
// of completion order.
func TestPool_PreservesOrder(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Respond faster for later paths to shuffle completion order.
		fmt.Fprint(w, r.URL.Path)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	urls := []string{
		srv.URL + "/a",
		srv.URL + "/b",
		srv.URL + "/c",
		srv.URL + "/d",
	}
	pool := NewPool(4, Options{Timeout: 2 * time.Second, Client: srv.Client()})
	results := pool.Run(context.Background(), urls)

	for i, r := range results {
		if r.URL != urls[i] {
			t.Fatalf("result %d out of order: got %s want %s", i, r.URL, urls[i])
		}
	}
}

// TestPool_CancellationStopsWork verifies that cancelling the context stops
// the pool promptly and still returns a result per URL.
func TestPool_CancellationStopsWork(t *testing.T) {
	var served int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&served, 1)
		select {
		case <-time.After(500 * time.Millisecond):
			w.WriteHeader(http.StatusOK)
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()

	urls := make([]string, 50)
	for i := range urls {
		urls[i] = srv.URL
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(80 * time.Millisecond)
		cancel()
	}()

	pool := NewPool(2, Options{Timeout: 5 * time.Second, Client: srv.Client()})
	start := time.Now()
	results := pool.Run(ctx, urls)
	elapsed := time.Since(start)

	if elapsed > 2*time.Second {
		t.Fatalf("cancellation did not stop work promptly: %v", elapsed)
	}
	// The feeder stops handing out jobs, so far fewer than 50 requests run.
	if got := atomic.LoadInt32(&served); got >= int32(len(urls)) {
		t.Fatalf("expected cancellation to prevent serving all URLs, served %d", got)
	}
	if len(results) == 0 {
		t.Fatal("expected at least some results back")
	}
}
