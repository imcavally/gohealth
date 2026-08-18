// Package cli wires command-line flags and I/O to the checker package. It is
// kept separate from main so the whole program can be exercised in tests.
package cli

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/imcavally/gohealth/internal/checker"
)

// Exit codes. These are part of the CLI contract with CI systems.
const (
	ExitOK       = 0   // all endpoints up
	ExitDown     = 1   // at least one endpoint down
	ExitUsage    = 2   // bad flags or no URLs
	ExitCanceled = 130 // interrupted (128 + SIGINT)
)

const defaultUserAgent = "gohealth/1.0 (+https://github.com/imcavally/gohealth)"

// config holds the parsed flag values.
type config struct {
	concurrency  int
	timeout      time.Duration
	retries      int
	backoff      time.Duration
	expectStatus int
	contains     string
	interval     time.Duration
	jsonOut      bool
	file         string
}

// Run is the program entry point used by main and by tests. It parses args,
// resolves the URL list, runs one or more check rounds and writes output.
// It returns a process exit code and a non-fatal error for stderr logging.
func Run(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) (int, error) {
	cfg, urls, err := parse(args, stdin, stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return ExitOK, nil
		}
		return ExitUsage, err
	}
	if len(urls) == 0 {
		return ExitUsage, errors.New("no URLs provided (pass as arguments, --file, or stdin)")
	}

	opts := checker.Options{
		Timeout:      cfg.timeout,
		Retries:      cfg.retries,
		Backoff:      cfg.backoff,
		ExpectStatus: cfg.expectStatus,
		Contains:     cfg.contains,
		UserAgent:    defaultUserAgent,
	}
	pool := checker.NewPool(cfg.concurrency, opts)

	// Single-shot mode.
	if cfg.interval <= 0 {
		results := pool.Run(ctx, urls)
		if err := render(stdout, results, cfg.jsonOut); err != nil {
			return ExitUsage, err
		}
		return exitCode(ctx, results), nil
	}

	// Watch mode: re-run on a ticker until the context is cancelled.
	return runWatch(ctx, pool, urls, cfg, stdout)
}

// runWatch repeatedly runs checks every cfg.interval until ctx is cancelled,
// returning the exit code of the most recent round.
func runWatch(ctx context.Context, pool *checker.Pool, urls []string, cfg config, stdout io.Writer) (int, error) {
	ticker := time.NewTicker(cfg.interval)
	defer ticker.Stop()

	round := func() (int, error) {
		results := pool.Run(ctx, urls)
		if ctx.Err() != nil {
			return ExitCanceled, nil
		}
		if !cfg.jsonOut {
			fmt.Fprintf(stdout, "\n== %s ==\n", time.Now().Format("15:04:05"))
		}
		if err := render(stdout, results, cfg.jsonOut); err != nil {
			return ExitUsage, err
		}
		return exitCode(ctx, results), nil
	}

	last, err := round()
	if err != nil {
		return last, err
	}
	for {
		select {
		case <-ctx.Done():
			return last, nil
		case <-ticker.C:
			last, err = round()
			if err != nil {
				return last, err
			}
		}
	}
}

// exitCode maps a set of results to a process exit code.
func exitCode(ctx context.Context, results []checker.Result) int {
	if ctx.Err() != nil {
		return ExitCanceled
	}
	for _, r := range results {
		if !r.Up {
			return ExitDown
		}
	}
	return ExitOK
}

// parse builds a config and the resolved URL list from args and stdin.
func parse(args []string, stdin io.Reader, stderr io.Writer) (config, []string, error) {
	var cfg config

	fs := flag.NewFlagSet("gohealth", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.IntVar(&cfg.concurrency, "concurrency", 8, "maximum number of concurrent checks")
	fs.DurationVar(&cfg.timeout, "timeout", 5*time.Second, "per-request timeout")
	fs.IntVar(&cfg.retries, "retries", 2, "number of retries on transient failure")
	fs.DurationVar(&cfg.backoff, "backoff", 200*time.Millisecond, "base delay for exponential backoff")
	fs.IntVar(&cfg.expectStatus, "expect", 0, "expected HTTP status code (0 = any 2xx)")
	fs.StringVar(&cfg.contains, "contains", "", "require response body to contain this substring")
	fs.DurationVar(&cfg.interval, "interval", 0, "watch mode: re-run checks every interval (e.g. 10s)")
	fs.BoolVar(&cfg.jsonOut, "json", false, "output results as JSON")
	fs.StringVar(&cfg.file, "file", "", "read URLs from a file (one per line)")

	fs.Usage = func() {
		fmt.Fprintf(stderr, "gohealth - concurrent HTTP endpoint health & latency checker\n\n")
		fmt.Fprintf(stderr, "Usage:\n  gohealth [flags] [url ...]\n\n")
		fmt.Fprintf(stderr, "URLs may be passed as arguments, via --file, or on stdin (one per line).\n\n")
		fmt.Fprintf(stderr, "Flags:\n")
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		return cfg, nil, err
	}
	if cfg.concurrency < 1 {
		return cfg, nil, errors.New("--concurrency must be >= 1")
	}

	urls, err := resolveURLs(fs.Args(), cfg.file, stdin)
	if err != nil {
		return cfg, nil, err
	}
	return cfg, urls, nil
}

// resolveURLs gathers URLs from positional args, an optional file and stdin
// (when stdin is not a terminal and no other source provided any URLs). It
// normalizes bare hosts to https:// and de-duplicates while preserving order.
func resolveURLs(args []string, file string, stdin io.Reader) ([]string, error) {
	var raw []string
	raw = append(raw, args...)

	if file != "" {
		f, err := os.Open(file)
		if err != nil {
			return nil, fmt.Errorf("open --file: %w", err)
		}
		defer f.Close()
		lines, err := readLines(f)
		if err != nil {
			return nil, fmt.Errorf("read --file: %w", err)
		}
		raw = append(raw, lines...)
	}

	// Fall back to stdin only if nothing else supplied URLs and stdin is
	// piped (readable). This keeps interactive `gohealth` from hanging.
	if len(raw) == 0 && stdin != nil && isPipe(stdin) {
		lines, err := readLines(stdin)
		if err != nil {
			return nil, fmt.Errorf("read stdin: %w", err)
		}
		raw = append(raw, lines...)
	}

	return normalize(raw), nil
}

// readLines returns non-empty, comment-stripped, trimmed lines from r.
func readLines(r io.Reader) ([]string, error) {
	var out []string
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		out = append(out, line)
	}
	return out, sc.Err()
}

// normalize prefixes bare hosts with https:// and removes duplicates while
// preserving first-seen order.
func normalize(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, u := range in {
		u = strings.TrimSpace(u)
		if u == "" {
			continue
		}
		if !strings.Contains(u, "://") {
			u = "https://" + u
		}
		if _, ok := seen[u]; ok {
			continue
		}
		seen[u] = struct{}{}
		out = append(out, u)
	}
	return out
}

// isPipe reports whether r is an *os.File that is not a character device
// (i.e. it is a pipe or regular file rather than an interactive terminal).
func isPipe(r io.Reader) bool {
	f, ok := r.(*os.File)
	if !ok {
		// Non-file readers (e.g. tests using strings.Reader) are treated
		// as pipes so stdin input can be exercised.
		return true
	}
	info, err := f.Stat()
	if err != nil {
		return false
	}
	return (info.Mode() & os.ModeCharDevice) == 0
}
