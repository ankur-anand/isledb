package isledb

import (
	"context"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
)

// BenchmarkL0SmallSaves writes tiny saves of random keys over a large L1, as a
// writer saving every 250 ms at a low write rate does, and runs one
// maintenance job per 8 saves, about one 2 s cycle. It reports how many bytes
// maintenance rewrote per byte saved, and how deep L0 got, with the writer
// merging its saves from memory and without. Scaled down: an 8 MiB L1 and
// 1 KiB saves keep L1 about 8,000 saves deep, as a 256 MiB L1 is to 25 KiB
// saves. Without writer merges, saves wait for the age limit, so that mode
// shortens it to see them drain.
//
//	go test -run '^$' -bench BenchmarkL0SmallSaves -benchtime 1x
func BenchmarkL0SmallSaves(b *testing.B) {
	for _, mode := range []struct {
		name         string
		writerMerges bool
	}{
		{"straight-to-L1", false},
		{"writer-merges", true},
	} {
		b.Run(mode.name, func(b *testing.B) {
			for range b.N {
				rewritten, saved, deepest, avg := runSmallSaves(b, mode.writerMerges)
				b.ReportMetric(float64(rewritten)/float64(saved), "rewritten/saved")
				b.ReportMetric(float64(deepest), "max-L0-files")
				b.ReportMetric(avg, "avg-L0-files")
			}
		})
	}
}

func runSmallSaves(b *testing.B, writerMerges bool) (rewritten, saved int64, deepest int, avgDepth float64) {
	ctx := context.Background()
	store := blobstore.NewMemory("l0-small-saves")
	defer store.Close()
	db, err := openDB(ctx, store, dbOpenOptions{sstOutput: testSSTOutput("none", 4096)})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	wopts := DefaultWriterOptions()
	wopts.Flush.Interval = 0
	if writerMerges {
		wopts.L0Merge.TargetBytes = 64 << 10 // 64 saves of 1 KiB, as 64 MiB is to 1 MiB saves
	}
	writer, err := db.OpenWriter(ctx, wopts)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = writer.Close(ctx) }()
	mopts := DefaultMaintenanceOptions()
	mopts.SSTCompaction.BaseLevelBytes = 1 << 60 // keep everything in L1
	mopts.SSTCompaction.TargetSSTBytes = 2 << 20
	if !writerMerges {
		mopts.SSTCompaction.L0MaxAge = time.Nanosecond // flush files are nobody's at once
	}
	maintenance, err := db.OpenMaintenance(ctx, mopts)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = maintenance.Close(ctx) }()

	const keys = 8192
	rng := rand.New(rand.NewPCG(1, 2))
	value := make([]byte, 1000)
	put := func(i int) {
		if _, err := writer.Put(ctx, []byte(fmt.Sprintf("key-%06d", i)), value); err != nil {
			b.Fatal(err)
		}
	}
	// L1: every key, about 8 MiB.
	for i := range keys {
		put(i)
		if i%1024 == 1023 {
			if err := writer.Flush(ctx); err != nil {
				b.Fatal(err)
			}
		}
	}
	driveMaintenanceToIdle(b, ctx, maintenance, writer)

	const saves = 2000
	var depthSum int
	for s := range saves {
		put(rng.IntN(keys))
		if err := writer.Flush(ctx); err != nil {
			b.Fatal(err)
		}
		saved += 1000
		if s%8 == 7 {
			stats, err := maintenance.RunOnce(ctx)
			if err != nil {
				b.Fatal(err)
			}
			rewritten += stats.SSTCompaction.RewrittenBytes
			if err := flushApplyingMaintenance(ctx, writer); err != nil {
				b.Fatal(err)
			}
		}
		depth := replayManifestForTest(b, ctx, store).L0SSTCount()
		depthSum += depth
		deepest = max(deepest, depth)
	}
	return rewritten, saved, deepest, float64(depthSum) / saves
}
