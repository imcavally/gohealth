# gohealth

`gohealth` is a small, dependency-free command-line tool that checks the health
and latency of many HTTP endpoints **concurrently**. Give it a list of URLs and
it reports each one's status code, response time and an up/down verdict, then
prints a summary and **exits non-zero if anything is down** — so it drops
straight into a CI pipeline or a cron job.

It is written in idiomatic Go using only the standard library. The concurrency
is the point: checks run through a **bounded worker pool** (goroutines + channels,
fan-out / fan-in), everything is cancellable via `context.Context`, and Ctrl-C
stops in-flight work gracefully.

## Why

Uptime checkers and readiness gates need to probe a handful — or a few hundred —
endpoints quickly without opening an unbounded number of connections. `gohealth`
does that with a fixed pool of workers, per-request timeouts, and retries with
exponential backoff for flaky endpoints, in a single static binary you can drop
onto any host or CI runner.

## Install

```sh
go install github.com/imcavally/gohealth@latest
```

This puts a `gohealth` binary in `$(go env GOPATH)/bin`. Go 1.22 or newer is
required. You can also build from a checkout:

```sh
git clone https://github.com/imcavally/gohealth
cd gohealth
go build -o gohealth .
```

## Usage

URLs can come from positional arguments, a file (`--file`), or stdin (one per
line; blank lines and `#` comments are ignored). Bare hosts are assumed to be
`https://`.

```sh
# Positional arguments
gohealth https://example.com https://api.example.com/health

# From a file
gohealth --file urls.txt

# From stdin
cat urls.txt | gohealth

# Tune concurrency, timeout and retries; require an exact status
gohealth --concurrency 20 --timeout 3s --retries 3 --expect 200 --file urls.txt

# Require a substring in the body (e.g. a health payload)
gohealth --expect 200 --contains '"status":"ok"' https://api.example.com/health

# Watch mode: re-check every 10 seconds until Ctrl-C
gohealth --interval 10s https://example.com

# Machine-readable output for scripts / CI
gohealth --json --file urls.txt
```

### Sample table output (default)

```text
STATUS  CODE  LATENCY  ATTEMPTS  URL                             DETAIL
UP      200   225ms    1         https://example.com
DOWN    503   1.16s    3         https://httpbin.org/status/503  unexpected status 503
DOWN    -     4.00s    3         https://api.example.com/slow    request timeout

3 checked  |  1 up  |  2 down  |  avg 466ms
```

### Sample JSON output (`--json`)

```json
{
  "checked_at": "2026-08-17T17:42:41.004+07:00",
  "summary": {
    "total": 1,
    "up": 1,
    "down": 0,
    "avg_latency_ms": 301.2261
  },
  "results": [
    {
      "url": "https://example.com",
      "up": true,
      "status_code": 200,
      "latency_ms": 301.2261,
      "attempts": 1,
      "checked_at": "2026-08-17T17:42:40.703+07:00"
    }
  ]
}
```

### Exit codes

| Code | Meaning                                   |
|------|-------------------------------------------|
| `0`  | All endpoints up                          |
| `1`  | At least one endpoint down                |
| `2`  | Usage error (bad flags, or no URLs given) |
| `130`| Interrupted (Ctrl-C / SIGINT)             |

Because a down endpoint yields a non-zero exit code, you can use `gohealth`
directly as a CI gate:

```sh
gohealth --expect 200 --file production-urls.txt || echo "something is down!"
```

## Flags

| Flag            | Default | Description                                              |
|-----------------|---------|----------------------------------------------------------|
| `--concurrency` | `8`     | Maximum number of concurrent checks (worker-pool size)   |
| `--timeout`     | `5s`    | Per-request timeout                                      |
| `--retries`     | `2`     | Retries on transient failure (exponential backoff)       |
| `--backoff`     | `200ms` | Base delay for exponential backoff between attempts      |
| `--expect`      | `0`     | Expected HTTP status code (`0` = accept any 2xx)         |
| `--contains`    | `""`    | Require the response body to contain this substring      |
| `--interval`    | `0`     | Watch mode: re-run checks every interval (e.g. `10s`)    |
| `--json`        | `false` | Emit JSON instead of a table                            |
| `--file`        | `""`    | Read URLs from a file (one per line)                    |

## Concurrency design

The heart of `gohealth` is a classic **fan-out / fan-in** worker pool in
`internal/checker/pool.go`:

```
urls  ->  jobs channel  ->  [ worker 1 .. worker N ]  ->  results channel  ->  collector
          (feeder)            (bounded goroutine set)         (fan-in)
```

- A single **feeder** goroutine pushes indexed jobs onto an unbuffered `jobs`
  channel. It selects on `ctx.Done()` so it stops handing out work the moment a
  cancellation arrives.
- Exactly **N worker goroutines** (`--concurrency`) read from `jobs`. Because the
  worker set is fixed, at most N checks ever run at once — the tool never spawns
  one goroutine per URL. Each worker writes its `Result` to a `results` channel.
- A `sync.WaitGroup` tracks the workers; when they all finish, a closer goroutine
  closes `results` exactly once, which lets the **collector** drain the channel
  and return. Results are re-sorted into the original input order.

Every layer respects **`context.Context`**:

- `--timeout` is applied per request with `context.WithTimeout`, so a single slow
  endpoint can't stall the run.
- `main.go` uses `signal.NotifyContext` so **Ctrl-C / SIGTERM** cancels the shared
  context; in-flight requests are aborted and the feeder stops.
- **Retries** use exponential backoff (`--backoff * 2^(n-1)`) and the backoff
  sleep is itself cancellable, so shutdown is never delayed by a pending retry.

This keeps the code small while making the concurrency correct under `-race`.

## Project layout

```
gohealth/
├── main.go                       # thin entry point: signal handling + cli.Run
├── go.mod
├── internal/
│   ├── checker/                  # concurrency + check logic (unit-tested)
│   │   ├── checker.go            # single-endpoint check, retries, backoff
│   │   ├── pool.go               # bounded worker pool (fan-out / fan-in)
│   │   ├── checker_test.go
│   │   └── pool_test.go
│   └── cli/                      # flag parsing, URL sourcing, output
│       ├── cli.go                # Run(), flags, watch mode, exit codes
│       ├── output.go             # table (text/tabwriter) + JSON rendering
│       └── cli_test.go
├── .github/workflows/ci.yml      # gofmt -l, go vet, go test -race
├── LICENSE                       # MIT
├── .gitignore
└── README.md
```

## Development

```sh
gofmt -l .          # should print nothing
go vet ./...
go test ./...       # add -race where a C compiler (cgo) is available
```

The test suite uses `net/http/httptest` to spin up fake servers and covers:
the pool respecting its concurrency limit, per-request timeouts, retries that
eventually succeed and retries that exhaust, expected-status mismatches, context
cancellation stopping work, URL sourcing (args/file/stdin), exit codes, watch
mode, and the JSON output shape.

## License

MIT — see [LICENSE](LICENSE). Author: Trung P.
