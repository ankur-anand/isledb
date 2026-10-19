package isledb

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// TestL0MergesKeepTheNewestVersion overwrites and deletes the same keys in many
// small saves over a larger L1, so the writer merges its L0 SSTs with each
// other from memory while maintenance runs, and checks every lookup and scan
// still returns the newest version.
func TestL0MergesKeepTheNewestVersion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	store := blobstore.NewMemory("l0-merges")
	defer store.Close()
	db, err := openDB(ctx, store, dbOpenOptions{sstOutput: testSSTOutput("none", 1024)})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	wopts := DefaultWriterOptions()
	wopts.Flush.Interval = 0
	// Small runs, so a 40-save test sees several merges.
	wopts.L0Merge.TargetBytes = 16 << 10
	wopts.Metrics = DefaultWriterMetrics(nil)
	writer, err := db.OpenWriter(ctx, wopts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = writer.Close(ctx) }()
	mopts := DefaultMaintenanceOptions()
	mopts.SSTCompaction.L0TriggerSSTs = 4
	mopts.SSTCompaction.BaseLevelBytes = 1 << 60
	maintenance, err := db.OpenMaintenance(ctx, mopts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = maintenance.Close(ctx) }()

	expected := map[string][]byte{}
	put := func(key, value string) {
		if _, err := writer.Put(ctx, []byte(key), []byte(value)); err != nil {
			t.Fatal(err)
		}
		expected[key] = []byte(value)
	}
	del := func(key string) {
		if _, err := writer.Delete(ctx, []byte(key)); err != nil {
			t.Fatal(err)
		}
		delete(expected, key)
	}

	// A base in L1, much larger than what follows, so later L0 data is not
	// worth rewriting it for.
	for f := 0; f < 4; f++ {
		for i := 0; i < 500; i++ {
			put(fmt.Sprintf("key-%04d", i), fmt.Sprintf("base-%d-%04d-%0100d", f, i, i))
		}
		if err := writer.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	driveMaintenanceToIdle(t, ctx, maintenance, writer)

	// Small saves that overwrite and delete the same keys again and again,
	// across the whole key range.
	for round := 0; round < 40; round++ {
		for _, i := range []int{round % 7, 100 + round%11, 250, 499 - round%5} {
			put(fmt.Sprintf("key-%04d", i), fmt.Sprintf("round-%02d", round))
		}
		del(fmt.Sprintf("key-%04d", 300+round))
		if round%3 == 0 {
			put(fmt.Sprintf("key-%04d", 300+round), fmt.Sprintf("back-%02d", round))
		}
		if err := writer.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		stats := driveMaintenanceToIdle(t, ctx, maintenance, writer)
		if stats.SSTCompaction.L0Merges != 0 {
			t.Fatalf("maintenance merged within L0: %d jobs; that is the writer's", stats.SSTCompaction.L0Merges)
		}
	}
	// One more pass publishes any merge finished after the last save.
	if err := writer.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	m := replayManifestForTest(t, ctx, store)
	if m.L0SSTCount() >= 40 {
		t.Fatalf("L0 holds %d SSTs after 40 saves", m.L0SSTCount())
	}
	// The writer merged; maintenance may since have drained the results into
	// L1, so count merges at the writer, and check the age of whatever merged
	// file is still in L0.
	merges := int(testutil.ToFloat64(wopts.Metrics.L0MergePublished))
	if merges == 0 {
		t.Fatal("no merges published: small saves should have been merged by the writer")
	}
	for _, sst := range m.L0SSTs {
		if !isCompactionSSTID(sst.ID) {
			continue
		}
		if sst.OldestDataAt.IsZero() || sst.OldestDataAt.After(sst.CreatedAt) {
			t.Fatalf("merged SST %s: oldest data %v, created %v", sst.ID, sst.OldestDataAt, sst.CreatedAt)
		}
	}

	reader := openReaderFromDBForTest(t, ctx, store, ReaderOpenOptions{CacheDir: t.TempDir()})
	defer reader.Close()
	assertReaderHasAll(t, ctx, reader, expected)
	for i := 300; i < 340; i++ {
		key := fmt.Sprintf("key-%04d", i)
		if _, want := expected[key]; want {
			continue
		}
		if _, found, err := reader.Get(ctx, []byte(key)); err != nil || found {
			t.Fatalf("deleted %s: found=%v err=%v", key, found, err)
		}
	}
	scanned, err := reader.Scan(ctx, []byte("key-"), []byte("key."))
	if err != nil {
		t.Fatal(err)
	}
	if len(scanned) != len(expected) {
		t.Fatalf("scan returned %d keys, want %d", len(scanned), len(expected))
	}
	for _, kv := range scanned {
		if string(kv.Value) != string(expected[string(kv.Key)]) {
			t.Fatalf("scan %s = %q, want %q", kv.Key, kv.Value, expected[string(kv.Key)])
		}
	}
	t.Logf("40 saves, %d L0 merges, L0 now %d SSTs, L1 %d SSTs", merges, m.L0SSTCount(), len(m.Levels))
}
