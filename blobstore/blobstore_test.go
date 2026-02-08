package blobstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"gocloud.dev/blob"
)

type storeHarness struct {
	store   *Store
	cleanup func()
	reopen  func(t *testing.T) *Store
}

type storeFactory struct {
	name string
	new  func(t *testing.T, prefix string) storeHarness
}

func storeFactories() []storeFactory {
	return []storeFactory{
		{name: "memory", new: newMemoryHarness},
		{name: "file", new: newFileHarness},
	}
}

func forEachStore(t *testing.T, prefix string, fn func(t *testing.T, h storeHarness)) {
	t.Helper()
	for _, factory := range storeFactories() {
		factory := factory
		t.Run(factory.name, func(t *testing.T) {
			h := factory.new(t, prefix)
			if h.cleanup != nil {
				t.Cleanup(h.cleanup)
			}
			fn(t, h)
		})
	}
}

func newMemoryHarness(t *testing.T, prefix string) storeHarness {
	t.Helper()
	store := NewMemory(prefix)
	return storeHarness{
		store: store,
		cleanup: func() {
			_ = store.Close()
		},
		reopen: func(t *testing.T) *Store {
			t.Helper()
			return newMemoryFromBucket(store.Bucket(), prefix)
		},
	}
}

func newFileHarness(t *testing.T, prefix string) storeHarness {
	t.Helper()
	store, dir, err := newFileTemp(prefix)
	if err != nil {
		t.Fatalf("newFileTemp: %v", err)
	}
	return storeHarness{
		store: store,
		cleanup: func() {
			_ = store.Close()
			_ = os.RemoveAll(dir)
		},
		reopen: func(t *testing.T) *Store {
			t.Helper()
			reopened, err := openFile(context.Background(), dir, prefix)
			if err != nil {
				t.Fatalf("openFile: %v", err)
			}
			return reopened
		},
	}
}

func TestWriteIfMatch_CreateOnly(t *testing.T) {
	forEachStore(t, "test", func(t *testing.T, h storeHarness) {
		ctx := context.Background()
		store := h.store

		key := store.ManifestPath()
		data := []byte(`{"version": 1}`)

		attr, err := store.WriteIfMatch(ctx, key, data, "")
		if err != nil {
			t.Fatalf("first write failed: %v", err)
		}
		if attr.ETag == "" {
			t.Error("expected non-empty ETag after write")
		}

		_, err = store.WriteIfMatch(ctx, key, []byte(`{"version": 2}`), "")
		if !errors.Is(err, ErrPreconditionFailed) {
			t.Errorf("expected ErrPreconditionFailed, got %v", err)
		}
	})
}

func TestWriteIfMatch_UpdateWithETag(t *testing.T) {
	forEachStore(t, "test", func(t *testing.T, h storeHarness) {
		ctx := context.Background()
		store := h.store

		key := store.ManifestPath()
		data1 := []byte(`{"version": 1}`)
		data2 := []byte(`{"version": 2}`)

		attr1, err := store.Write(ctx, key, data1)
		if err != nil {
			t.Fatalf("initial write failed: %v", err)
		}

		attr2, err := store.WriteIfMatch(ctx, key, data2, attr1.ETag)
		if err != nil {
			t.Fatalf("update with correct ETag failed: %v", err)
		}
		if attr2.ETag == attr1.ETag {
			t.Error("expected ETag to change after update")
		}

		content, _, err := store.Read(ctx, key)
		if err != nil {
			t.Fatalf("read failed: %v", err)
		}
		if string(content) != string(data2) {
			t.Errorf("content mismatch: got %q, want %q", content, data2)
		}
	})
}

func TestWriteIfMatch_UpdateWithWrongETag(t *testing.T) {
	forEachStore(t, "test", func(t *testing.T, h storeHarness) {
		ctx := context.Background()
		store := h.store

		key := store.ManifestPath()
		data := []byte(`{"version": 1}`)

		_, err := store.Write(ctx, key, data)
		if err != nil {
			t.Fatalf("initial write failed: %v", err)
		}

		_, err = store.WriteIfMatch(ctx, key, []byte(`{"version": 2}`), "wrong-etag")
		if !errors.Is(err, ErrPreconditionFailed) {
			t.Errorf("expected ErrPreconditionFailed, got %v", err)
		}

		content, _, err := store.Read(ctx, key)
		if err != nil {
			t.Fatalf("read failed: %v", err)
		}
		if string(content) != string(data) {
			t.Errorf("content was modified: got %q, want %q", content, data)
		}
	})
}

func TestWriteIfMatch_UpdateNonExistent(t *testing.T) {
	forEachStore(t, "test", func(t *testing.T, h storeHarness) {
		ctx := context.Background()
		store := h.store

		key := store.ManifestPath()

		_, err := store.WriteIfMatch(ctx, key, []byte(`{"version": 1}`), "some-etag")
		if !errors.Is(err, ErrPreconditionFailed) {
			t.Errorf("expected ErrPreconditionFailed, got %v", err)
		}
	})
}

func TestWriteIfMatch_ConcurrentWrites(t *testing.T) {
	forEachStore(t, "test", func(t *testing.T, h storeHarness) {
		ctx := context.Background()
		store := h.store

		key := store.ManifestPath()
		data := []byte(`{"version": 1}`)

		attr, err := store.Write(ctx, key, data)
		if err != nil {
			t.Fatalf("initial write failed: %v", err)
		}

		etag := attr.ETag

		_, err = store.WriteIfMatch(ctx, key, []byte(`{"version": 2}`), etag)
		if err != nil {
			t.Fatalf("first concurrent write failed: %v", err)
		}

		_, err = store.WriteIfMatch(ctx, key, []byte(`{"version": 3}`), etag)
		if !errors.Is(err, ErrPreconditionFailed) {
			t.Errorf("expected ErrPreconditionFailed for second writer, got %v", err)
		}
	})
}

func TestPathHelpers(t *testing.T) {
	forEachStore(t, "tenant-1", func(t *testing.T, h storeHarness) {
		store := h.store

		tests := []struct {
			name string
			got  string
			want string
		}{
			{"SSTPath", store.SSTPath("seg1.sst"), "sstable/seg1.sst"},
			{"VLogPath", store.VLogPath("abc123"), "vlogs/abc123.vlog"},
			{"ManifestPath", store.ManifestPath(), "manifest/CURRENT"},
			{"ManifestLogPath", store.ManifestLogPath("001"), "manifest/log/001.json"},
			{"ManifestSnapshotPath", store.ManifestSnapshotPath("001"), "manifest/snapshots/001.manifest"},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if tt.got != "tenant-1/"+tt.want {
					t.Errorf("%s = %q, want %q", tt.name, tt.got, tt.want)
				}
			})
		}
	})
}

func TestBasicOperations(t *testing.T) {
	forEachStore(t, "test", func(t *testing.T, h storeHarness) {
		ctx := context.Background()
		store := h.store

		key := "test/file.txt"
		data := []byte("hello world")

		attr, err := store.Write(ctx, key, data)
		if err != nil {
			t.Fatalf("Write failed: %v", err)
		}
		if attr.Size != int64(len(data)) {
			t.Errorf("Size: got %d, want %d", attr.Size, len(data))
		}

		exists, err := store.Exists(ctx, key)
		if err != nil {
			t.Fatalf("Exists failed: %v", err)
		}
		if !exists {
			t.Error("Exists: expected true")
		}

		content, readAttr, err := store.Read(ctx, key)
		if err != nil {
			t.Fatalf("Read failed: %v", err)
		}
		if string(content) != string(data) {
			t.Errorf("Read content: got %q, want %q", content, data)
		}
		if readAttr.Size != int64(len(data)) {
			t.Errorf("Read size: got %d, want %d", readAttr.Size, len(data))
		}
		if readAttr.ETag != "" {
			t.Errorf("Read ETag should be empty, got %q", readAttr.ETag)
		}
		attrs, err := store.Attributes(ctx, key)
		if err != nil {
			t.Fatalf("Attributes failed: %v", err)
		}
		if attrs.ETag != attr.ETag {
			t.Errorf("ETag mismatch: got %q, want %q", attrs.ETag, attr.ETag)
		}

		if err := store.Delete(ctx, key); err != nil {
			t.Fatalf("delete failed: %v", err)
		}

		exists, err = store.Exists(ctx, key)
		if err != nil {
			t.Fatalf("Exists after delete failed: %v", err)
		}
		if exists {
			t.Error("file still exists after delete")
		}

		if err := store.Delete(ctx, key); err != nil {
			t.Errorf("delete non-existent failed: %v", err)
		}
	})
}

func TestReadNotFound(t *testing.T) {
	forEachStore(t, "test", func(t *testing.T, h storeHarness) {
		ctx := context.Background()
		store := h.store

		_, _, err := store.Read(ctx, "nonexistent")
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("expected ErrNotFound, got %v", err)
		}
	})
}

func TestStore_ReopenReadsExistingObject(t *testing.T) {
	forEachStore(t, "test-tenant", func(t *testing.T, h storeHarness) {
		if h.reopen == nil {
			t.Skip("reopen not supported")
		}

		ctx := context.Background()
		store := h.store

		key := store.ManifestPath()
		data := []byte(`{"version": 1}`)

		_, err := store.Write(ctx, key, data)
		if err != nil {
			t.Fatalf("Write: %v", err)
		}

		reopened := h.reopen(t)
		t.Cleanup(func() {
			_ = reopened.Close()
		})

		content, _, err := reopened.Read(ctx, key)
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		if string(content) != string(data) {
			t.Errorf("content: got %q, want %q", content, data)
		}
	})
}

func TestDebugString(t *testing.T) {
	forEachStore(t, "debug", func(t *testing.T, h storeHarness) {
		ctx := context.Background()
		store := h.store

		out := store.DebugString()
		if !strings.Contains(out, "Objects:\n") {
			t.Fatalf("expected DebugString header, got %q", out)
		}

		key1 := store.ManifestPath()
		data1 := []byte(`{"version": 1}`)
		_, err := store.Write(ctx, key1, data1)
		if err != nil {
			t.Fatalf("Write key1: %v", err)
		}

		key2 := store.SSTPath("seg1.sst")
		data2 := []byte("abc")
		_, err = store.Write(ctx, key2, data2)
		if err != nil {
			t.Fatalf("Write key2: %v", err)
		}

		out = store.DebugString()
		want1 := fmt.Sprintf("%s (%d bytes)", key1, len(data1))
		if !strings.Contains(out, want1) {
			t.Errorf("expected DebugString to include %q, got %q", want1, out)
		}
		want2 := fmt.Sprintf("%s (%d bytes)", key2, len(data2))
		if !strings.Contains(out, want2) {
			t.Errorf("expected DebugString to include %q, got %q", want2, out)
		}
	})
}

func TestStoreAccessors(t *testing.T) {
	forEachStore(t, "access", func(t *testing.T, h storeHarness) {
		store := h.store
		if store.Bucket() == nil {
			t.Fatal("expected non-nil bucket")
		}
		if store.Prefix() != "access" {
			t.Errorf("Prefix = %q, want %q", store.Prefix(), "access")
		}
	})
}

func TestWriteReaderAndAttributes(t *testing.T) {
	forEachStore(t, "attrs", func(t *testing.T, h storeHarness) {
		ctx := context.Background()
		store := h.store

		key := store.ManifestPath()
		data := "hello reader"

		attr, err := store.WriteReader(ctx, key, strings.NewReader(data), &blob.WriterOptions{
			ContentType: "text/plain",
		})
		if err != nil {
			t.Fatalf("WriteReader failed: %v", err)
		}
		if attr.Size != int64(len(data)) {
			t.Errorf("Size: got %d, want %d", attr.Size, len(data))
		}

		got, err := store.Attributes(ctx, key)
		if err != nil {
			t.Fatalf("Attributes failed: %v", err)
		}
		if got.Size != attr.Size {
			t.Errorf("Attributes size: got %d, want %d", got.Size, attr.Size)
		}
		if got.ETag == "" {
			t.Error("expected non-empty ETag after Attributes")
		}

		_, err = store.Attributes(ctx, "missing")
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("expected ErrNotFound, got %v", err)
		}
	})
}

func TestReadStream(t *testing.T) {
	forEachStore(t, "stream", func(t *testing.T, h storeHarness) {
		ctx := context.Background()
		store := h.store

		key := "stream/data.txt"
		data := []byte("streaming data")
		_, err := store.Write(ctx, key, data)
		if err != nil {
			t.Fatalf("Write failed: %v", err)
		}

		r, err := store.ReadStream(ctx, key)
		if err != nil {
			t.Fatalf("ReadStream failed: %v", err)
		}
		defer r.Close()

		got, err := io.ReadAll(r)
		if err != nil {
			t.Fatalf("read stream: %v", err)
		}
		if string(got) != string(data) {
			t.Errorf("content mismatch: got %q, want %q", got, data)
		}

		_, err = store.ReadStream(ctx, "missing")
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("expected ErrNotFound, got %v", err)
		}
	})
}

func TestListFunctions(t *testing.T) {
	forEachStore(t, "list", func(t *testing.T, h storeHarness) {
		ctx := context.Background()
		store := h.store

		expected := map[string]int{
			store.ManifestLogPath("001"): 2,
			store.ManifestLogPath("002"): 3,
			store.SSTPath("seg1.sst"):    4,
			store.VLogPath("abc123"):     5,
		}

		payloads := map[string][]byte{
			store.ManifestLogPath("001"): []byte("m1"),
			store.ManifestLogPath("002"): []byte("log"),
			store.SSTPath("seg1.sst"):    []byte("data"),
			store.VLogPath("abc123"):     []byte("vlog!"),
		}

		for key, data := range payloads {
			if _, err := store.Write(ctx, key, data); err != nil {
				t.Fatalf("Write %s failed: %v", key, err)
			}
		}

		listAll, err := store.List(ctx, ListOptions{})
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}
		got := make(map[string]int64, len(listAll.Objects))
		for _, obj := range listAll.Objects {
			got[obj.Key] = obj.Size
		}
		if len(got) != len(expected) {
			t.Fatalf("List count: got %d, want %d", len(got), len(expected))
		}
		for key, size := range expected {
			gotSize, ok := got[key]
			if !ok {
				t.Errorf("List missing key %q", key)
				continue
			}
			if gotSize != int64(size) {
				t.Errorf("List size for %q: got %d, want %d", key, gotSize, size)
			}
		}

		manifestOnly, err := store.List(ctx, ListOptions{Prefix: "manifest/"})
		if err != nil {
			t.Fatalf("List with prefix failed: %v", err)
		}
		if len(manifestOnly.Objects) != 2 {
			t.Fatalf("List manifest count: got %d, want %d", len(manifestOnly.Objects), 2)
		}

		sst, err := store.ListSSTFiles(ctx)
		if err != nil {
			t.Fatalf("ListSSTFiles failed: %v", err)
		}
		if len(sst) != 1 || sst[0].Key != store.SSTPath("seg1.sst") {
			t.Errorf("ListSSTFiles = %#v", sst)
		}

		vlogs, err := store.ListVLogFiles(ctx)
		if err != nil {
			t.Fatalf("ListVLogFiles failed: %v", err)
		}
		if len(vlogs) != 1 || vlogs[0].Key != store.VLogPath("abc123") {
			t.Errorf("ListVLogFiles = %#v", vlogs)
		}

		logs, err := store.ListManifestLogs(ctx)
		if err != nil {
			t.Fatalf("ListManifestLogs failed: %v", err)
		}
		if len(logs) != 2 {
			t.Fatalf("ListManifestLogs count: got %d, want %d", len(logs), 2)
		}
	})
}

func TestWriteIfNotExist_Success(t *testing.T) {
	forEachStore(t, "test", func(t *testing.T, h storeHarness) {
		ctx := context.Background()
		store := h.store

		key := store.ManifestLogPath("00000000000000000001")
		data := []byte(`{"op": "add_sst", "id": "sst-1"}`)

		_, err := store.WriteIfNotExist(ctx, key, data)
		if err != nil {
			t.Fatalf("WriteIfNotExist failed: %v", err)
		}

		content, _, err := store.Read(ctx, key)
		if err != nil {
			t.Fatalf("read failed: %v", err)
		}
		if string(content) != string(data) {
			t.Errorf("content mismatch: got %q, want %q", content, data)
		}
	})
}

func TestWriteIfNotExist_AlreadyExists(t *testing.T) {
	forEachStore(t, "test", func(t *testing.T, h storeHarness) {
		ctx := context.Background()
		store := h.store

		key := store.ManifestLogPath("00000000000000000001")
		data1 := []byte(`{"op": "add_sst", "id": "sst-1"}`)
		data2 := []byte(`{"op": "add_sst", "id": "sst-2"}`)

		_, err := store.WriteIfNotExist(ctx, key, data1)
		if err != nil {
			t.Fatalf("first WriteIfNotExist failed: %v", err)
		}

		_, err = store.WriteIfNotExist(ctx, key, data2)
		if !errors.Is(err, ErrPreconditionFailed) {
			t.Errorf("expected ErrPreconditionFailed, got %v", err)
		}

		content, _, err := store.Read(ctx, key)
		if err != nil {
			t.Fatalf("read failed: %v", err)
		}
		if string(content) != string(data1) {
			t.Errorf("content should be unchanged: got %q, want %q", content, data1)
		}
	})
}

func TestBatchDelete(t *testing.T) {
	forEachStore(t, "test", func(t *testing.T, h storeHarness) {
		ctx := context.Background()
		store := h.store

		keys := []string{
			"batch/a.txt",
			"batch/b.txt",
			"batch/missing.txt",
		}

		if _, err := store.Write(ctx, keys[0], []byte("a")); err != nil {
			t.Fatalf("write key %q failed: %v", keys[0], err)
		}
		if _, err := store.Write(ctx, keys[1], []byte("b")); err != nil {
			t.Fatalf("write key %q failed: %v", keys[1], err)
		}

		if err := store.BatchDelete(ctx, []string{keys[0], keys[1], keys[2], keys[0], ""}); err != nil {
			t.Fatalf("batch delete failed: %v", err)
		}

		for _, key := range keys {
			exists, err := store.Exists(ctx, key)
			if err != nil {
				t.Fatalf("exists(%q) failed: %v", key, err)
			}
			if exists {
				t.Fatalf("expected %q to be deleted", key)
			}
		}

		if err := store.BatchDelete(ctx, keys); err != nil {
			t.Fatalf("batch delete idempotency failed: %v", err)
		}
	})
}
