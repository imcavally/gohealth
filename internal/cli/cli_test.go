package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func upServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	}))
}

func downServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
}

func TestRun_AllUp_ExitOK(t *testing.T) {
	srv := upServer(t)
	defer srv.Close()

	var out, errOut bytes.Buffer
	code, err := Run(context.Background(), []string{"--retries", "0", srv.URL}, nil, &out, &errOut)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if code != ExitOK {
		t.Fatalf("expected exit %d, got %d (stderr: %s)", ExitOK, code, errOut.String())
	}
	if !strings.Contains(out.String(), "UP") {
		t.Fatalf("expected table to mention UP, got:\n%s", out.String())
	}
}

func TestRun_OneDown_ExitDown(t *testing.T) {
	up := upServer(t)
	defer up.Close()
	down := downServer(t)
	defer down.Close()

	var out, errOut bytes.Buffer
	args := []string{"--retries", "0", "--expect", "200", up.URL, down.URL}
	code, err := Run(context.Background(), args, nil, &out, &errOut)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if code != ExitDown {
		t.Fatalf("expected exit %d, got %d", ExitDown, code)
	}
}

func TestRun_JSONShape(t *testing.T) {
	srv := upServer(t)
	defer srv.Close()

	var out, errOut bytes.Buffer
	code, err := Run(context.Background(), []string{"--json", "--retries", "0", srv.URL}, nil, &out, &errOut)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if code != ExitOK {
		t.Fatalf("expected exit %d, got %d", ExitOK, code)
	}

	var doc report
	if err := json.Unmarshal(out.Bytes(), &doc); err != nil {
		t.Fatalf("invalid JSON output: %v\n%s", err, out.String())
	}
	if doc.Summary.Total != 1 || doc.Summary.Up != 1 || doc.Summary.Down != 0 {
		t.Fatalf("unexpected summary: %+v", doc.Summary)
	}
	if len(doc.Results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(doc.Results))
	}
	r := doc.Results[0]
	if !r.Up || r.StatusCode != 200 {
		t.Fatalf("unexpected result: %+v", r)
	}
	if r.LatencyMS <= 0 {
		t.Fatalf("expected positive latency_ms, got %v", r.LatencyMS)
	}
}

func TestRun_ReadsFromFile(t *testing.T) {
	srv := upServer(t)
	defer srv.Close()

	dir := t.TempDir()
	path := filepath.Join(dir, "urls.txt")
	content := "# comment line\n" + srv.URL + "\n\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	var out, errOut bytes.Buffer
	code, err := Run(context.Background(), []string{"--json", "--retries", "0", "--file", path}, nil, &out, &errOut)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if code != ExitOK {
		t.Fatalf("expected exit %d, got %d", ExitOK, code)
	}
	var doc report
	if err := json.Unmarshal(out.Bytes(), &doc); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if doc.Summary.Total != 1 {
		t.Fatalf("expected 1 URL from file, got %d", doc.Summary.Total)
	}
}

func TestRun_ReadsFromStdin(t *testing.T) {
	srv := upServer(t)
	defer srv.Close()

	stdin := strings.NewReader(srv.URL + "\n")
	var out, errOut bytes.Buffer
	code, err := Run(context.Background(), []string{"--json", "--retries", "0"}, stdin, &out, &errOut)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if code != ExitOK {
		t.Fatalf("expected exit %d, got %d", ExitOK, code)
	}
}

func TestRun_NoURLs_ExitUsage(t *testing.T) {
	var out, errOut bytes.Buffer
	// nil stdin so no URLs are resolved.
	code, err := Run(context.Background(), []string{"--retries", "0"}, nil, &out, &errOut)
	if err == nil {
		t.Fatal("expected an error when no URLs provided")
	}
	if code != ExitUsage {
		t.Fatalf("expected exit %d, got %d", ExitUsage, code)
	}
}

func TestRun_BadFlag_ExitUsage(t *testing.T) {
	var out, errOut bytes.Buffer
	code, _ := Run(context.Background(), []string{"--nope"}, nil, &out, &errOut)
	if code != ExitUsage {
		t.Fatalf("expected exit %d, got %d", ExitUsage, code)
	}
}

func TestRun_WatchMode_CancelStops(t *testing.T) {
	srv := upServer(t)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	var out, errOut bytes.Buffer
	args := []string{"--interval", "50ms", "--retries", "0", srv.URL}
	done := make(chan int, 1)
	go func() {
		code, _ := Run(ctx, args, nil, &out, &errOut)
		done <- code
	}()

	select {
	case <-done:
		// Returned promptly after ctx timeout - good.
	case <-time.After(2 * time.Second):
		t.Fatal("watch mode did not stop after context cancellation")
	}
	if !strings.Contains(out.String(), "==") {
		t.Fatalf("expected at least one watch round header, got:\n%s", out.String())
	}
}

func TestNormalize_DedupAndScheme(t *testing.T) {
	in := []string{"example.com", "https://example.com", "http://a.test", "a.test", ""}
	got := normalize(in)
	want := []string{"https://example.com", "http://a.test", "https://a.test"}
	if len(got) != len(want) {
		t.Fatalf("expected %d urls, got %d: %v", len(want), len(got), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("normalize[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}
