package manifest

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

type etagStorage struct {
	mu          sync.Mutex
	current     []byte
	etagCounter int

	casExpected []string
	readCount   int
}

func newETagStorage() *etagStorage {
	return &etagStorage{}
}

func (s *etagStorage) ReadCurrent(ctx context.Context) ([]byte, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.readCount++
	if s.current == nil {
		return nil, "", ErrNotFound
	}
	return append([]byte(nil), s.current...), fmt.Sprintf("e%d", s.etagCounter), nil
}

func (s *etagStorage) WriteCurrentCAS(ctx context.Context, data []byte, expectedETag string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.casExpected = append(s.casExpected, expectedETag)

	if s.current == nil {
		if expectedETag != "" {
			return "", ErrPreconditionFailed
		}
	} else {
		currentETag := fmt.Sprintf("e%d", s.etagCounter)
		if expectedETag != currentETag {
			return "", ErrPreconditionFailed
		}
	}

	s.current = append([]byte(nil), data...)
	s.etagCounter++
	return fmt.Sprintf("e%d", s.etagCounter), nil
}

func (s *etagStorage) ReadSnapshot(ctx context.Context, path string) ([]byte, error) {
	return nil, ErrNotFound
}

func (s *etagStorage) WriteSnapshot(ctx context.Context, id string, data []byte) (string, error) {
	return "snapshots/" + id, nil
}

func (s *etagStorage) resetCounts() {
	s.mu.Lock()
	s.readCount = 0
	s.casExpected = nil
	s.mu.Unlock()
}

func (s *etagStorage) snapshotCounts() (int, []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cas := append([]string(nil), s.casExpected...)
	return s.readCount, cas
}

func TestAppendEntry_UsesCachedETagAndSingleRead(t *testing.T) {
	ctx := context.Background()
	storage := newETagStorage()
	ms := NewStoreWithStorage(storage)

	if _, err := ms.Replay(ctx); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if _, err := ms.ClaimWriter(ctx, "writer-1"); err != nil {
		t.Fatalf("claim writer: %v", err)
	}

	storage.mu.Lock()
	expectedETag := fmt.Sprintf("e%d", storage.etagCounter)
	storage.mu.Unlock()

	storage.resetCounts()

	if _, err := ms.AppendAddSSTableWithFence(ctx, SSTMeta{ID: "a.sst", Epoch: 1, Level: 0}); err != nil {
		t.Fatalf("append add sstable: %v", err)
	}

	readCount, casExpected := storage.snapshotCounts()
	if readCount != 0 {
		t.Fatalf("expected 0 ReadCurrent after reset, got %d", readCount)
	}
	if len(casExpected) != 1 || casExpected[0] != expectedETag {
		t.Fatalf("expected CAS with %s, got %v", expectedETag, casExpected)
	}
}

func TestAppendEntry_UsesUpdatedETagAcrossAppends(t *testing.T) {
	ctx := context.Background()
	storage := newETagStorage()
	ms := NewStoreWithStorage(storage)

	if _, err := ms.Replay(ctx); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if _, err := ms.ClaimWriter(ctx, "writer-1"); err != nil {
		t.Fatalf("claim writer: %v", err)
	}

	storage.mu.Lock()
	startETag := fmt.Sprintf("e%d", storage.etagCounter)
	nextETag := fmt.Sprintf("e%d", storage.etagCounter+1)
	storage.mu.Unlock()

	storage.resetCounts()

	if _, err := ms.AppendAddSSTableWithFence(ctx, SSTMeta{ID: "a.sst", Epoch: 1, Level: 0}); err != nil {
		t.Fatalf("append add sstable: %v", err)
	}
	if _, err := ms.AppendAddSSTableWithFence(ctx, SSTMeta{ID: "b.sst", Epoch: 2, Level: 0}); err != nil {
		t.Fatalf("append add sstable: %v", err)
	}

	_, casExpected := storage.snapshotCounts()
	if len(casExpected) != 2 || casExpected[0] != startETag || casExpected[1] != nextETag {
		t.Fatalf("expected CAS [%s %s], got %v", startETag, nextETag, casExpected)
	}
}

func TestReadCurrentWithETag_ClearsCacheOnNotFound(t *testing.T) {
	ctx := context.Background()
	storage := newETagStorage()
	currentData, err := EncodeCurrent(&Current{NextEpoch: 1, NextSeq: 1})
	if err != nil {
		t.Fatalf("encode current: %v", err)
	}
	storage.current = currentData

	ms := NewStoreWithStorage(storage)
	if current, _, err := ms.readCurrentWithETag(ctx); err != nil {
		t.Fatalf("read current: %v", err)
	} else if current == nil {
		t.Fatal("expected current")
	}
	if ms.current == nil || ms.currentETag == "" {
		t.Fatal("expected current cache to be populated")
	}

	storage.mu.Lock()
	storage.current = nil
	storage.mu.Unlock()

	if current, etag, err := ms.readCurrentWithETag(ctx); err != nil {
		t.Fatalf("read missing current: %v", err)
	} else if current != nil || etag != "" {
		t.Fatalf("expected missing current, got current=%v etag=%q", current, etag)
	}
	if ms.current != nil || ms.currentETag != "" {
		t.Fatalf("expected current cache to be cleared, current=%v etag=%q", ms.current, ms.currentETag)
	}
}
