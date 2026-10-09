package diskcache

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"slices"
	"sync"
	"testing"
	"time"
)

// TestMeasureRemoveContention measures how deleting many entries slows reads
// of other entries: reads alone, then while RemoveAll deletes dead entries in
// another shard directory, in the readers' own directory, and paced. It runs
// only with ISLEDB_MEASURE set; run it with -v to see the table.
func TestMeasureRemoveContention(t *testing.T) {
	if os.Getenv("ISLEDB_MEASURE") == "" {
		t.Skip("set ISLEDB_MEASURE=1 to run")
	}
	const (
		live       = 1_000
		entryBytes = 4 << 10
		readers    = 8
		batch      = 512 // about one 64 MiB SST's chunks
		baseline   = 3 * time.Second
	)
	c, err := Open(Options{Dir: t.TempDir(), MaxBytes: 1 << 40})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	shard := func(object string) string {
		sum := sha256.Sum256([]byte(object))
		return hex.EncodeToString(sum[:1])
	}
	// An object name whose entries share the shard directory of "live".
	sameDir := ""
	for i := 0; sameDir == ""; i++ {
		if name := fmt.Sprintf("same-%d", i); shard(name) == shard("live") {
			sameDir = name
		}
	}
	key := func(object string, i int) Key {
		return Key{Object: sha256.Sum256([]byte(object)), Kind: KindChunk, Index: uint32(i)}
	}
	data := make([]byte, entryBytes)
	put := func(object string, n int) []Key {
		keys := make([]Key, n)
		for i := range n {
			keys[i] = key(object, i)
			if err := c.Put(keys[i], data); err != nil {
				t.Fatal(err)
			}
		}
		return keys
	}
	put("live", live)

	read := func(stop <-chan struct{}) []time.Duration {
		var mu sync.Mutex
		var all []time.Duration
		var wg sync.WaitGroup
		for w := range readers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				p := make([]byte, entryBytes)
				var mine []time.Duration
				for i := w; ; i++ {
					select {
					case <-stop:
						mu.Lock()
						all = append(all, mine...)
						mu.Unlock()
						return
					default:
					}
					start := time.Now()
					if !c.ReadAt(key("live", i%live), entryBytes, p, 0) {
						t.Error("a live entry was missing")
					}
					mine = append(mine, time.Since(start))
				}
			}()
		}
		<-stop
		wg.Wait()
		slices.Sort(all)
		return all
	}
	// during reads while remove runs, and reports the reads and how long it ran.
	during := func(remove func()) ([]time.Duration, time.Duration) {
		stop := make(chan struct{})
		var took time.Duration
		go func() {
			start := time.Now()
			remove()
			took = time.Since(start)
			close(stop)
		}()
		reads := read(stop)
		return reads, took
	}
	paced := func(keys []Key, perSecond int) func() {
		return func() {
			start := time.Now()
			for i := 0; i < len(keys); i += batch {
				c.RemoveAll(keys[i:min(i+batch, len(keys))])
				due := start.Add(time.Duration(min(i+batch, len(keys))) * time.Second / time.Duration(perSecond))
				time.Sleep(time.Until(due))
			}
		}
	}

	otherKeys := put("dead-other", 40_000)
	sameKeys := put(sameDir, 40_000)
	fast := put("dead-paced-2000", 10_000)
	slow := put("dead-paced-500", 10_000)

	stop := make(chan struct{})
	go func() { time.Sleep(baseline); close(stop) }()
	alone := read(stop)
	other, otherTook := during(func() { c.RemoveAll(otherKeys) })
	same, sameTook := during(func() { c.RemoveAll(sameKeys) })
	pacedFast, fastTook := during(paced(fast, 2000))
	pacedSlow, slowTook := during(paced(slow, 500))

	pct := func(d []time.Duration, p float64) time.Duration {
		return d[int(p*float64(len(d)-1))].Round(time.Microsecond)
	}
	row := func(name string, d []time.Duration, span time.Duration, files int) {
		rate := "–"
		if files > 0 {
			rate = fmt.Sprintf("%.0f", float64(files)/span.Seconds())
		}
		fmt.Printf("| %s | %s | %s | %.0f | %s | %s | %s |\n", name, rate, span.Round(100*time.Millisecond),
			float64(len(d))/span.Seconds(), pct(d, 0.5), pct(d, 0.99), pct(d, 0.999))
	}
	fmt.Printf("\n%d readers on %d live entries in shard %s; deletes with RemoveAll\n\n", readers, live, shard("live"))
	fmt.Println("| phase | files deleted/s | duration | reads/s | p50 | p99 | p99.9 |")
	fmt.Println("| --- | ---: | ---: | ---: | ---: | ---: | ---: |")
	row("reads alone", alone, baseline, 0)
	row("delete 40,000, another directory", other, otherTook, len(otherKeys))
	row("delete 40,000, readers' directory", same, sameTook, len(sameKeys))
	row("delete 10,000 paced at 2,000/s", pacedFast, fastTook, len(fast))
	row("delete 10,000 paced at 500/s", pacedSlow, slowTook, len(slow))
}
