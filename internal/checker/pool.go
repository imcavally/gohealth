package checker

import (
	"context"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Pool checks many URLs concurrently using a bounded number of worker
// goroutines. The concurrency limit is enforced by the fixed size of the
// worker set, not by spawning one goroutine per URL.
type Pool struct {
	concurrency int
	opts        Options
	client      *http.Client

	// inflight tracks the current number of in-progress checks; it is used
	// by tests to assert the concurrency limit is respected, and exposed
	// via MaxObservedConcurrency.
	inflight    int64
	maxInflight int64
}

// NewPool builds a Pool. concurrency is clamped to a minimum of 1. If
// opts.Client is nil, a shared client honoring opts.Timeout is created.
func NewPool(concurrency int, opts Options) *Pool {
	if concurrency < 1 {
		concurrency = 1
	}

	client := opts.Client
	if client == nil {
		client = &http.Client{
			// The per-request timeout is applied via context in
			// doRequest; keep the client itself unbounded so watch
			// mode and cancellation behave predictably.
			Transport: http.DefaultTransport,
		}
	}

	return &Pool{
		concurrency: concurrency,
		opts:        opts,
		client:      client,
	}
}

// Run checks every URL in urls and returns the results sorted by input order.
// It blocks until all checks finish or ctx is cancelled. On cancellation the
// feeder stops handing out new work, so URLs not yet started are omitted;
// checks already in flight are reported as down with a context error.
//
// Design: a classic fan-out / fan-in pipeline.
//
//	urls  ->  jobs channel  ->  [worker 1..N]  ->  results channel  ->  collect
//
// A single feeder goroutine pushes indexed jobs onto a channel; exactly
// `concurrency` workers read from it, so at most N checks run at once. Each
// worker writes to a results channel that a collector drains. sync.WaitGroup
// coordinates worker shutdown so the results channel is closed exactly once.
func (p *Pool) Run(ctx context.Context, urls []string) []Result {
	atomic.StoreInt64(&p.maxInflight, 0)

	type job struct {
		index int
		url   string
	}

	jobs := make(chan job)
	results := make(chan Result)

	// Feeder: fan-out. Stops early if the context is cancelled so workers
	// are not handed new work during shutdown.
	go func() {
		defer close(jobs)
		for i, u := range urls {
			select {
			case <-ctx.Done():
				return
			case jobs <- job{index: i, url: u}:
			}
		}
	}()

	// Workers: a fixed pool of goroutines. This bounded set is what
	// enforces --concurrency.
	var wg sync.WaitGroup
	wg.Add(p.concurrency)
	for w := 0; w < p.concurrency; w++ {
		go func() {
			defer wg.Done()
			for j := range jobs {
				cur := atomic.AddInt64(&p.inflight, 1)
				p.recordMax(cur)

				r := Check(ctx, p.client, j.url, p.opts)
				// Preserve input ordering via the job index.
				r.index = j.index

				atomic.AddInt64(&p.inflight, -1)

				select {
				case <-ctx.Done():
					// Still deliver the (down) result so the
					// collector gets exactly len(urls) items.
					r = downOnCancel(ctx, j.url, r)
					results <- r
				case results <- r:
				}
			}
		}()
	}

	// Closer: fan-in cleanup. Close results once all workers are done.
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collector: drain the fan-in channel.
	collected := make([]Result, 0, len(urls))
	for r := range results {
		collected = append(collected, r)
	}

	sort.SliceStable(collected, func(i, j int) bool {
		if collected[i].index != collected[j].index {
			return collected[i].index < collected[j].index
		}
		return collected[i].URL < collected[j].URL
	})
	return collected
}

// MaxObservedConcurrency returns the peak number of checks that ran
// simultaneously during the last Run. Primarily used in tests.
func (p *Pool) MaxObservedConcurrency() int {
	return int(atomic.LoadInt64(&p.maxInflight))
}

func (p *Pool) recordMax(cur int64) {
	for {
		old := atomic.LoadInt64(&p.maxInflight)
		if cur <= old {
			return
		}
		if atomic.CompareAndSwapInt64(&p.maxInflight, old, cur) {
			return
		}
	}
}

// downOnCancel ensures a result produced during cancellation is marked down
// with a context error, unless the check already succeeded before shutdown.
func downOnCancel(ctx context.Context, url string, r Result) Result {
	if r.Up {
		return r
	}
	if r.Error == "" {
		r.Error = ctx.Err().Error()
	}
	if r.CheckedAt.IsZero() {
		r.CheckedAt = time.Now()
	}
	r.URL = url
	r.Up = false
	return r
}
