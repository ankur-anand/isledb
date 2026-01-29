package isledb

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
)

func TestDB_BasicOperations(t *testing.T) {
	store := blobstore.NewMemory("test")
	ctx := context.Background()

	opts := DefaultDBOptions()
	opts.FlushInterval = 0
	opts.EnableCompaction = false

	db, err := Open(ctx, store, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if err := db.Put([]byte("key1"), []byte("value1")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := db.Put([]byte("key2"), []byte("value2")); err != nil {
		t.Fatalf("put: %v", err)
	}

	if err := db.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	if err := db.Refresh(ctx); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	value, found, err := db.Get(ctx, []byte("key1"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found {
		t.Error("key1 not found")
	}
	if string(value) != "value1" {
		t.Errorf("Get(key1) = %q, want %q", value, "value1")
	}

	if err := db.Delete([]byte("key1")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := db.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := db.Refresh(ctx); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	_, found, err = db.Get(ctx, []byte("key1"))
	if err != nil {
		t.Fatalf("Get after delete: %v", err)
	}
	if found {
		t.Error("key1 should not be found after delete")
	}

	value, found, err = db.Get(ctx, []byte("key2"))
	if err != nil {
		t.Fatalf("Get key2: %v", err)
	}
	if !found {
		t.Error("key2 not found")
	}
	if string(value) != "value2" {
		t.Errorf("Get(key2) = %q, want %q", value, "value2")
	}
}

func TestDB_Scan(t *testing.T) {
	store := blobstore.NewMemory("test")
	ctx := context.Background()

	opts := DefaultDBOptions()
	opts.FlushInterval = 0
	opts.EnableCompaction = false

	db, err := Open(ctx, store, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key%02d", i))
		value := []byte(fmt.Sprintf("value%02d", i))
		if err := db.Put(key, value); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	if err := db.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := db.Refresh(ctx); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	kvs, err := db.Scan(ctx, nil, nil)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(kvs) != 10 {
		t.Errorf("Scan returned %d entries, want 10", len(kvs))
	}

	kvs, err = db.Scan(ctx, []byte("key03"), []byte("key07"))
	if err != nil {
		t.Fatalf("Scan range: %v", err)
	}
	if len(kvs) != 5 {
		t.Errorf("Scan range returned %d entries, want 5", len(kvs))
	}
}

func TestDB_WithCompaction(t *testing.T) {
	store := blobstore.NewMemory("test")
	ctx := context.Background()

	compactionCount := 0
	opts := DefaultDBOptions()
	opts.FlushInterval = 0
	opts.EnableCompaction = true
	opts.L0CompactionThreshold = 4
	opts.CompactionCheckInterval = time.Hour
	opts.OnCompactionEnd = func(job CompactionJob, err error) {
		if err == nil {
			compactionCount++
		}
	}

	db, err := Open(ctx, store, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	for batch := 0; batch < 8; batch++ {
		for i := 0; i < 5; i++ {
			key := []byte{byte('a' + batch), byte('0' + i)}
			value := []byte("value")
			if err := db.Put(key, value); err != nil {
				t.Fatalf("put: %v", err)
			}
		}
		if err := db.Flush(ctx); err != nil {
			t.Fatalf("flush: %v", err)
		}
	}

	if err := db.Compact(ctx); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	if compactionCount == 0 {
		t.Error("no compactions ran")
	}

	if err := db.Refresh(ctx); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	for batch := 0; batch < 8; batch++ {
		for i := 0; i < 5; i++ {
			key := []byte{byte('a' + batch), byte('0' + i)}
			value, found, err := db.Get(ctx, key)
			if err != nil {
				t.Errorf("Get(%q): %v", key, err)
				continue
			}
			if !found {
				t.Errorf("Get(%q): not found", key)
				continue
			}
			if string(value) != "value" {
				t.Errorf("Get(%q) = %q, want %q", key, value, "value")
			}
		}
	}
}

func TestDB_Stats(t *testing.T) {
	store := blobstore.NewMemory("test")
	ctx := context.Background()

	opts := DefaultDBOptions()
	opts.FlushInterval = 0
	opts.EnableCompaction = false

	db, err := Open(ctx, store, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	stats, err := db.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.L0SSTCount != 0 {
		t.Errorf("initial L0SSTCount = %d, want 0", stats.L0SSTCount)
	}

	for i := 0; i < 3; i++ {
		for j := 0; j < 5; j++ {
			if err := db.Put([]byte{byte(i), byte(j)}, []byte("val")); err != nil {
				t.Fatalf("put: %v", err)
			}
		}
		if err := db.Flush(ctx); err != nil {
			t.Fatalf("flush: %v", err)
		}
	}

	if err := db.Refresh(ctx); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	stats, err = db.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.L0SSTCount != 3 {
		t.Errorf("L0SSTCount = %d, want 3", stats.L0SSTCount)
	}
	if stats.TotalSSTSize == 0 {
		t.Error("TotalSSTSize should be > 0")
	}
}

func TestDB_Close(t *testing.T) {
	store := blobstore.NewMemory("test")
	ctx := context.Background()

	opts := DefaultDBOptions()
	opts.FlushInterval = 0
	opts.EnableCompaction = false

	db, err := Open(ctx, store, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	if err := db.Put([]byte("key"), []byte("value")); err != nil {
		t.Fatalf("put: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if err := db.Put([]byte("key2"), []byte("value2")); err == nil {
		t.Error("put after close should fail")
	}

	db2, err := Open(ctx, store, opts)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer db2.Close()

	value, found, err := db2.Get(ctx, []byte("key"))
	if err != nil {
		t.Fatalf("Get after reopen: %v", err)
	}
	if !found {
		t.Error("data lost after close/reopen")
	}
	if string(value) != "value" {
		t.Errorf("Get = %q, want %q", value, "value")
	}
}

func TestDB_WarmCacheOnOpen(t *testing.T) {
	store := blobstore.NewMemory("test")
	ctx := context.Background()

	opts := DefaultDBOptions()
	opts.FlushInterval = 0
	opts.EnableCompaction = false

	db, err := Open(ctx, store, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	for i := 0; i < 5; i++ {
		for j := 0; j < 10; j++ {
			key := []byte(fmt.Sprintf("key-%d-%d", i, j))
			value := []byte(fmt.Sprintf("value-%d-%d", i, j))
			if err := db.Put(key, value); err != nil {
				t.Fatalf("put: %v", err)
			}
		}
		if err := db.Flush(ctx); err != nil {
			t.Fatalf("flush: %v", err)
		}
	}

	db.Close()

	var warmedCount int
	var warmDuration time.Duration

	opts2 := DefaultDBOptions()
	opts2.FlushInterval = 0
	opts2.EnableCompaction = false
	opts2.WarmCacheOnOpen = true
	opts2.WarmCacheTimeout = 10 * time.Second
	opts2.OnCacheWarmDone = func(warmed int, duration time.Duration) {
		warmedCount = warmed
		warmDuration = duration
	}

	db2, err := Open(ctx, store, opts2)
	if err != nil {
		t.Fatalf("Open with warming: %v", err)
	}
	defer db2.Close()

	if warmedCount != 5 {
		t.Errorf("warmed %d SSTs, want 5", warmedCount)
	}
	if warmDuration == 0 {
		t.Error("warmDuration should be > 0")
	}

	stats := db2.reader.SSTCacheStats()
	if stats.EntryCount != 5 {
		t.Errorf("SSTCache.EntryCount = %d, want 5", stats.EntryCount)
	}

	readerStats := db2.reader.SSTReaderCacheStats()
	if readerStats.EntryCount != 5 {
		t.Errorf("SSTReaderCache.EntryCount = %d, want 5", readerStats.EntryCount)
	}

	value, found, err := db2.Get(ctx, []byte("key-0-0"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found {
		t.Error("key-0-0 not found")
	}
	if string(value) != "value-0-0" {
		t.Errorf("Get = %q, want %q", value, "value-0-0")
	}

	readerStatsAfter := db2.reader.SSTReaderCacheStats()
	if readerStatsAfter.Hits == 0 {
		t.Error("expected reader cache hits after Get")
	}
}

func TestDB_BackgroundSync(t *testing.T) {
	store := blobstore.NewMemory("test")
	ctx := context.Background()

	opts := DefaultDBOptions()
	opts.FlushInterval = 0
	opts.EnableCompaction = false
	opts.BackgroundSync = true
	opts.SyncInterval = 50 * time.Millisecond

	db, err := Open(ctx, store, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	for i := 0; i < 5; i++ {
		if err := db.Put([]byte(fmt.Sprintf("key-%d", i)), []byte("value")); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	if err := db.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	value, found, err := db.Get(ctx, []byte("key-0"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found {

		t.Error("key-0 not found after background sync")
	}
	if string(value) != "value" {
		t.Errorf("Get = %q, want %q", value, "value")
	}
}

func TestDB_InProcessOptions(t *testing.T) {
	opts := InProcessDBOptions()

	if !opts.WarmCacheOnOpen {
		t.Error("WarmCacheOnOpen should be true for in-process")
	}
	if !opts.BackgroundSync {
		t.Error("BackgroundSync should be true for in-process")
	}
	if opts.SSTCacheSize != 256*1024*1024 {
		t.Errorf("SSTCacheSize = %d, want 256MB", opts.SSTCacheSize)
	}
	if opts.SSTReaderCacheSize != 100 {
		t.Errorf("SSTReaderCacheSize = %d, want 100", opts.SSTReaderCacheSize)
	}
}
