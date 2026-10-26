package runner

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// ProxyStats is a latency proxy's counts by kind of object. It reads the
// counts of every latproxy version: requests and response bytes by kind,
// and, where the proxy has them, by method.
type ProxyStats map[string]struct {
	Requests int64                  `json:"requests"`
	Bytes    int64                  `json:"bytes"`
	Methods  map[string]MethodStats `json:"methods"`
}

type MethodStats struct {
	Requests      int64 `json:"requests"`
	Bytes         int64 `json:"bytes"`
	UploadedBytes int64 `json:"uploaded_bytes"`
	ServerErrors  int64 `json:"server_errors"`
	Injected      int64 `json:"injected"`
	Dropped       int64 `json:"dropped"`
}

var httpc = http.Client{Timeout: 5 * time.Second}

func getJSON(ctx context.Context, url string, into any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := httpc.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: %s", url, resp.Status)
	}
	return json.NewDecoder(resp.Body).Decode(into)
}

func (p *Proxy) Stats(ctx context.Context) (ProxyStats, error) {
	var s ProxyStats
	return s, getJSON(ctx, p.URL+"/_latproxy/stats", &s)
}

// SetFaults replaces the proxy's injected faults with the JSON body.
func (p *Proxy) SetFaults(ctx context.Context, faults string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, p.URL+"/_latproxy/faults", strings.NewReader(faults))
	if err != nil {
		return err
	}
	resp, err := httpc.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("latproxy %s refused faults %s: %s", p.Group, faults, resp.Status)
	}
	return nil
}

// uploaded returns the bytes PUT for objects of kind.
func (s ProxyStats) uploaded(kind string) int64 {
	return s[kind].Methods["PUT"].UploadedBytes
}

// scrapeMetrics returns the isledb metrics a role's admin listener serves,
// each summed over its labels: whatever this version exports, so nothing
// here depends on its names.
func scrapeMetrics(ctx context.Context, admin string) (map[string]float64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+admin+"/metrics", nil)
	if err != nil {
		return nil, err
	}
	resp, err := httpc.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	out := map[string]float64{}
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "isledb_") {
			continue
		}
		name, rest, _ := strings.Cut(line, " ")
		if i := strings.IndexByte(name, '{'); i >= 0 {
			// Labels can hold spaces: the value follows the closing brace.
			j := strings.LastIndexByte(line, '}')
			name, rest = line[:i], strings.TrimSpace(line[j+1:])
		}
		// Histogram buckets would sum to nonsense; their _sum and _count stay.
		if strings.HasSuffix(name, "_bucket") {
			continue
		}
		valueText, _, _ := strings.Cut(rest, " ")
		v, err := strconv.ParseFloat(valueText, 64)
		if err != nil {
			continue
		}
		out[name] += v
	}
	return out, sc.Err()
}
