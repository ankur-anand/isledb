// Command writerbench measures an isledb writer's HTTP API: synchronous and
// asynchronous puts and batches, at rising numbers of concurrent clients. It
// reports requests and writes per second, latency percentiles, the answers by
// status, and, from the writer's metrics, how many writes each commit held.
//
//	writerbench -url http://127.0.0.1:8070 -metrics http://127.0.0.1:8071/metrics
//
// Run the writer against the storage to measure: a local directory for the
// server's own cost, or an S3-compatible server behind latproxy for
// commits that cross a network.
package main

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type scenario struct {
	name  string
	batch int  // ops per request; 0 is a single PUT
	async bool // Prefer: respond-async
}

var scenarios = map[string]scenario{
	"put":        {name: "put"},
	"put-async":  {name: "put-async", async: true},
	"batch-10":   {name: "batch-10", batch: 10},
	"batch-100":  {name: "batch-100", batch: 100},
	"batch-1000": {name: "batch-1000", batch: 1000},
}

type options struct {
	url, metrics, token string
	valueSize           int
	duration            time.Duration
	clients             []int
	scenarios           []string
}

func main() {
	var o options
	var clients, names string
	flag.StringVar(&o.url, "url", "http://127.0.0.1:8070", "writer API")
	flag.StringVar(&o.metrics, "metrics", "http://127.0.0.1:8071/metrics", "writer metrics, for writes per commit; empty to skip")
	flag.StringVar(&o.token, "token", "", "bearer token")
	flag.IntVar(&o.valueSize, "value-size", 256, "bytes per value")
	flag.DurationVar(&o.duration, "duration", 10*time.Second, "length of each run")
	flag.StringVar(&clients, "clients", "1,16,64,256", "concurrent clients, one run each")
	flag.StringVar(&names, "scenarios", "put,put-async,batch-10,batch-100", "scenarios: "+strings.Join(slices.Sorted(maps.Keys(scenarios)), ", "))
	flag.Parse()
	for _, c := range strings.Split(clients, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(c))
		if err != nil || n <= 0 {
			log.Fatalf("clients: %q", c)
		}
		o.clients = append(o.clients, n)
	}
	for _, n := range strings.Split(names, ",") {
		if _, ok := scenarios[strings.TrimSpace(n)]; !ok {
			log.Fatalf("scenario %q", n)
		}
		o.scenarios = append(o.scenarios, strings.TrimSpace(n))
	}

	client := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 1024}}
	const row = "%-10s %7s %9s %9s %9s %9s %9s %9s %13s  %s\n"
	fmt.Printf(row, "scenario", "clients", "req/s", "writes/s", "p50", "p90", "p99", "max", "writes/commit", "answers")
	for _, name := range o.scenarios {
		for _, n := range o.clients {
			r := run(client, o, scenarios[name], n)
			fmt.Printf(row, name, strconv.Itoa(n), fmt.Sprintf("%.0f", r.reqPerSec()), fmt.Sprintf("%.0f", r.writesPerSec()),
				ms(r.pct(50)), ms(r.pct(90)), ms(r.pct(99)), ms(r.pct(100)), r.writesPerCommit(), r.answers())
		}
	}
}

type result struct {
	elapsed   time.Duration
	latencies []time.Duration
	writes    int64
	codes     map[int]int64
	commits   float64 // commits during the run, from the metrics; -1 unknown
	puts      float64
}

func run(client *http.Client, o options, s scenario, clients int) result {
	before := scrape(client, o.metrics)
	value := make([]byte, o.valueSize)
	_, _ = rand.Read(value)
	var (
		mu     sync.Mutex
		res    = result{codes: map[int]int64{}}
		writes atomic.Int64
		wg     sync.WaitGroup
	)
	deadline := time.Now().Add(o.duration)
	start := time.Now()
	for c := range clients {
		wg.Go(func() {
			var lat []time.Duration
			codes := map[int]int64{}
			for i := 0; time.Now().Before(deadline); i++ {
				req := request(o, s, c, i, value)
				t0 := time.Now()
				resp, err := client.Do(req)
				d := time.Since(t0)
				if err != nil {
					codes[0]++
					continue
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
				codes[resp.StatusCode]++
				lat = append(lat, d)
				if resp.StatusCode < 300 {
					writes.Add(int64(max(s.batch, 1)))
				}
			}
			mu.Lock()
			res.latencies = append(res.latencies, lat...)
			for k, v := range codes {
				res.codes[k] += v
			}
			mu.Unlock()
		})
	}
	wg.Wait()
	res.elapsed = time.Since(start)
	res.writes = writes.Load()
	slices.Sort(res.latencies)
	after := scrape(client, o.metrics)
	res.commits, res.puts = -1, -1
	if before != nil && after != nil {
		res.commits = after["isledb_writer_commits_total"] - before["isledb_writer_commits_total"]
		res.puts = after["isledb_writer_puts_total"] - before["isledb_writer_puts_total"]
	}
	return res
}

func request(o options, s scenario, client, i int, value []byte) *http.Request {
	var req *http.Request
	if s.batch == 0 {
		req, _ = http.NewRequest(http.MethodPut, fmt.Sprintf("%s/v1/kv/bench%%2F%d%%2F%d", o.url, client, i), bytes.NewReader(value))
	} else {
		type op struct {
			Op    string `json:"op"`
			Key   []byte `json:"key_base64"`
			Value []byte `json:"value_base64"`
		}
		ops := make([]op, s.batch)
		for j := range ops {
			ops[j] = op{Op: "put", Key: fmt.Appendf(nil, "bench/%d/%d/%d", client, i, j), Value: value}
		}
		body, _ := json.Marshal(map[string]any{"ops": ops})
		req, _ = http.NewRequest(http.MethodPost, o.url+"/v1/kv", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
	}
	if s.async {
		req.Header.Set("Prefer", "respond-async")
	}
	if o.token != "" {
		req.Header.Set("Authorization", "Bearer "+o.token)
	}
	return req
}

// scrape reads the writer's counters, or nil without metrics.
func scrape(client *http.Client, url string) map[string]float64 {
	if url == "" {
		return nil
	}
	resp, err := client.Get(url)
	if err != nil {
		log.Printf("metrics: %v", err)
		return nil
	}
	defer func() { _ = resp.Body.Close() }()
	out := map[string]float64{}
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		name, value, ok := strings.Cut(sc.Text(), " ")
		if !ok || strings.HasPrefix(name, "#") || strings.Contains(name, "{") {
			continue
		}
		if v, err := strconv.ParseFloat(value, 64); err == nil {
			out[name] = v
		}
	}
	return out
}

func (r result) reqPerSec() float64 { return float64(len(r.latencies)) / r.elapsed.Seconds() }

func (r result) writesPerSec() float64 { return float64(r.writes) / r.elapsed.Seconds() }

func (r result) pct(p int) time.Duration {
	if len(r.latencies) == 0 {
		return 0
	}
	return r.latencies[min(len(r.latencies)-1, len(r.latencies)*p/100)]
}

func (r result) writesPerCommit() string {
	if r.commits <= 0 || r.puts < 0 {
		return "-"
	}
	return fmt.Sprintf("%.0f", r.puts/r.commits)
}

func (r result) answers() string {
	var parts []string
	for _, code := range slices.Sorted(maps.Keys(r.codes)) {
		label := strconv.Itoa(code)
		if code == 0 {
			label = "err"
		}
		parts = append(parts, fmt.Sprintf("%s:%d", label, r.codes[code]))
	}
	return strings.Join(parts, " ")
}

func ms(d time.Duration) string { return fmt.Sprintf("%.1fms", float64(d.Microseconds())/1000) }
