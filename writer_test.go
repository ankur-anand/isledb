package isledb

import (
	"context"
	"testing"

	"github.com/ankur-anand/isledb/blobstore"
)

func TestWriter_FlushCreatesManifestAndFiles(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("writer-test")
	defer store.Close()

	w, err := NewWriter(ctx, store, WriterOptions{
		MemtableSize:    1 << 20,
		FlushInterval:   0,
		BloomBitsPerKey: 0,
		BlockSize:       4096,
		Compression:     "none",
	})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	if err := w.Put([]byte("a"), []byte("value-1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := w.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	manifestKey := store.ManifestPath()
	if _, _, err := store.Read(ctx, manifestKey); err != nil {
		t.Fatalf("manifest CURRENT missing: %v", err)
	}

	logs, err := store.ListManifestLogs(ctx)
	if err != nil {
		t.Fatalf("ListManifestLogs: %v", err)
	}
	if len(logs) == 0 {
		t.Fatalf("expected manifest log entries")
	}

	ssts, err := store.List(ctx, blobstore.ListOptions{Prefix: "sstable/"})
	if err != nil {
		t.Fatalf("List sstable: %v", err)
	}
	if len(ssts.Objects) == 0 {
		t.Fatalf("expected at least one sstable object")
	}
}

func TestWriter_ReplaySeedsEpoch(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("writer-replay")
	defer store.Close()

	w, err := NewWriter(ctx, store, WriterOptions{
		MemtableSize:  1 << 20,
		FlushInterval: 0,
		BlockSize:     4096,
		Compression:   "none",
	})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.Put([]byte("a"), []byte("v1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := w.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	w2, err := NewWriter(ctx, store, WriterOptions{
		MemtableSize:  1 << 20,
		FlushInterval: 0,
		BlockSize:     4096,
		Compression:   "none",
	})
	if err != nil {
		t.Fatalf("NewWriter(2): %v", err)
	}
	defer w2.Close()

	if err := w2.Put([]byte("b"), []byte("v2")); err != nil {
		t.Fatalf("Put2: %v", err)
	}
	if err := w2.Flush(ctx); err != nil {
		t.Fatalf("Flush2: %v", err)
	}

	logs, err := store.ListManifestLogs(ctx)
	if err != nil {
		t.Fatalf("ListManifestLogs: %v", err)
	}
	if len(logs) < 2 {
		t.Fatalf("expected at least 2 manifest log entries, got %d", len(logs))
	}
}
