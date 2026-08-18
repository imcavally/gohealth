package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"text/tabwriter"
	"time"

	"github.com/imcavally/gohealth/internal/checker"
)

// report is the top-level JSON document emitted by --json.
type report struct {
	CheckedAt time.Time        `json:"checked_at"`
	Summary   summary          `json:"summary"`
	Results   []checker.Result `json:"results"`
}

// summary aggregates a set of results.
type summary struct {
	Total    int     `json:"total"`
	Up       int     `json:"up"`
	Down     int     `json:"down"`
	AvgLatMS float64 `json:"avg_latency_ms"`
}

// render writes results either as JSON or as a human-friendly table.
func render(w io.Writer, results []checker.Result, asJSON bool) error {
	if asJSON {
		return renderJSON(w, results)
	}
	return renderTable(w, results)
}

func renderJSON(w io.Writer, results []checker.Result) error {
	// Populate the computed millisecond field for each result.
	for i := range results {
		results[i].LatencyMS = float64(results[i].Latency) / float64(time.Millisecond)
	}
	doc := report{
		CheckedAt: time.Now(),
		Summary:   summarize(results),
		Results:   results,
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(doc)
}

func renderTable(w io.Writer, results []checker.Result) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "STATUS\tCODE\tLATENCY\tATTEMPTS\tURL\tDETAIL")

	for _, r := range results {
		status := "UP"
		if !r.Up {
			status = "DOWN"
		}
		code := "-"
		if r.StatusCode > 0 {
			code = fmt.Sprintf("%d", r.StatusCode)
		}
		detail := r.Error
		fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%s\t%s\n",
			status, code, formatLatency(r.Latency), r.Attempts, r.URL, detail)
	}
	if err := tw.Flush(); err != nil {
		return err
	}

	s := summarize(results)
	fmt.Fprintf(w, "\n%d checked  |  %d up  |  %d down  |  avg %.0fms\n",
		s.Total, s.Up, s.Down, s.AvgLatMS)
	return nil
}

// summarize computes aggregate stats over results.
func summarize(results []checker.Result) summary {
	s := summary{Total: len(results)}
	var totalLat time.Duration
	for _, r := range results {
		if r.Up {
			s.Up++
		} else {
			s.Down++
		}
		totalLat += r.Latency
	}
	if len(results) > 0 {
		s.AvgLatMS = float64(totalLat) / float64(len(results)) / float64(time.Millisecond)
	}
	return s
}

// formatLatency renders a duration compactly (e.g. "12ms", "1.20s").
func formatLatency(d time.Duration) string {
	if d <= 0 {
		return "-"
	}
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	return fmt.Sprintf("%.2fs", d.Seconds())
}
