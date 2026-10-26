package runner

import (
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"os"
	"slices"
	"strconv"
	"strings"
	"text/tabwriter"
)

func ReadSummary(path string) (*Summary, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var s Summary
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return &s, nil
}

// Compare prints runs side by side at each checkpoint they share, settled,
// and at the end.
func Compare(w io.Writer, sums []*Summary) {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	defer func() { _ = tw.Flush() }()
	row := func(cells ...string) { _, _ = fmt.Fprintln(tw, join(cells)) }

	head := []string{"", ""}
	for _, s := range sums {
		head = append(head, s.Run.Isledb.Version)
	}
	row(head...)
	row(append([]string{"run", ""}, mapSums(sums, func(s *Summary) string { return s.Run.Name })...)...)
	row(append([]string{"verification failures", ""}, mapSums(sums, func(s *Summary) string { return strconv.FormatInt(s.Failed, 10) })...)...)
	row(append([]string{"error", ""}, mapSums(sums, func(s *Summary) string { return short(s.Error) })...)...)

	points := checkpoints(sums)
	type at struct {
		label string
		pick  func(*Summary) *Sample
	}
	var ats []at
	for _, p := range points {
		ats = append(ats, at{"at " + Size(p).String() + " written", func(s *Summary) *Sample {
			c, ok := s.Checkpoints[strconv.FormatInt(p, 10)]
			if !ok {
				return nil
			}
			return &c
		}})
	}
	ats = append(ats, at{"settled", func(s *Summary) *Sample { return s.Settled }}, at{"end", func(s *Summary) *Sample { return &s.Final }})
	measures := []struct {
		name string
		get  func(*Sample) string
	}{
		{"written", func(x *Sample) string { return Size(x.Load.Bytes).String() }},
		{"write amplification", func(x *Sample) string { return amp(x, func(a *Amplification) float64 { return a.Write }) }},
		{"  by the writer", func(x *Sample) string { return amp(x, func(a *Amplification) float64 { return a.Writer }) }},
		{"  by compaction", func(x *Sample) string { return amp(x, func(a *Amplification) float64 { return a.Compaction }) }},
		{"commit p50 / p99 ms", func(x *Sample) string { return fmt.Sprintf("%.1f / %.1f", x.Load.Commit.P50, x.Load.Commit.P99) }},
		{"refused writes", func(x *Sample) string { return strconv.FormatInt(x.Load.Refused, 10) }},
		{"get p50 / p99 ms", func(x *Sample) string {
			return readerLatency(x, func(r ReaderSample) (float64, float64) { return r.Get.P50, r.Get.P99 })
		}},
		{"scan p50 / p99 ms", func(x *Sample) string {
			return readerLatency(x, func(r ReaderSample) (float64, float64) { return r.Scan.P50, r.Scan.P99 })
		}},
	}
	for _, a := range ats {
		row("", "")
		row(a.label, "")
		for _, m := range measures {
			cells := []string{"", m.name}
			for _, s := range sums {
				x := a.pick(s)
				if x == nil {
					cells = append(cells, "-")
					continue
				}
				cells = append(cells, m.get(x))
			}
			row(cells...)
		}
	}
}

func join(cells []string) string {
	out := ""
	for i, c := range cells {
		if i > 0 {
			out += "\t"
		}
		out += c
	}
	return out + "\t"
}

func mapSums(sums []*Summary, f func(*Summary) string) []string {
	out := make([]string, len(sums))
	for i, s := range sums {
		out[i] = f(s)
	}
	return out
}

// short is the first line of s, at most 60 characters, or "-".
func short(s string) string {
	if s == "" {
		return "-"
	}
	s, _, _ = strings.Cut(s, "\n")
	if len(s) > 60 {
		s = s[:57] + "..."
	}
	return s
}

func amp(x *Sample, f func(*Amplification) float64) string {
	if x.Amp == nil {
		return "-"
	}
	return fmt.Sprintf("%.2f", f(x.Amp))
}

// readerLatency is the latency of the first reader group, which is what
// most specs have.
func readerLatency(x *Sample, f func(ReaderSample) (float64, float64)) string {
	var names []string
	for n := range x.Readers {
		names = append(names, n)
	}
	if len(names) == 0 {
		return "-"
	}
	slices.Sort(names)
	p50, p99 := f(x.Readers[names[0]])
	return fmt.Sprintf("%.1f / %.1f", p50, p99)
}

// checkpoints returns every checkpoint any run recorded, in order.
func checkpoints(sums []*Summary) []int64 {
	seen := map[int64]bool{}
	for _, s := range sums {
		for k := range s.Checkpoints {
			if v, err := strconv.ParseInt(k, 10, 64); err == nil {
				seen[v] = true
			}
		}
	}
	out := slices.Collect(maps.Keys(seen))
	slices.Sort(out)
	return out
}
