// Command gohealth is a concurrent HTTP endpoint health & latency checker.
//
// It reads URLs from positional arguments, a file (--file), or stdin and
// checks them in parallel using a bounded worker pool. For each endpoint it
// reports the status code, latency and an up/down verdict, then prints a
// summary and exits non-zero if any endpoint is down (so it works in CI).
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/imcavally/gohealth/internal/cli"
)

func main() {
	// Cancel in-flight work gracefully on Ctrl-C / SIGTERM.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	code, err := cli.Run(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "gohealth:", err)
	}
	os.Exit(code)
}
