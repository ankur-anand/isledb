package isledb

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
)

// TestL0MergesKeepTheNewestVersion overwrites and deletes the same keys in many
// small saves over a larger L1, so maintenance merges L0 SSTs with each other,
// and checks every lookup and scan still returns the newest version.
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
	var merges int
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
		merges += driveMaintenanceToIdle(t, ctx, maintenance, writer).SSTCompaction.L0Merges
	}
	if merges == 0 {
		t.Fatal("no L0 merges: small saves over a large L1 should merge within L0")
	}

	m := replayManifestForTest(t, ctx, store)
	if m.L0SSTCount() >= 40 {
		t.Fatalf("L0 holds %d SSTs after 40 saves and %d merges", m.L0SSTCount(), merges)
	}
	merged := false
	for _, sst := range m.L0SSTs {
		if !sst.OldestDataAt.IsZero() {
			merged = true
			if !sst.OldestDataAt.Before(sst.CreatedAt) && !sst.OldestDataAt.Equal(sst.CreatedAt) {
				t.Fatalf("merged SST %s: oldest data %v after creation %v", sst.ID, sst.OldestDataAt, sst.CreatedAt)
			}
		}
	}
	if !merged {
		t.Fatal("no merged SST in L0 carries OldestDataAt")
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
