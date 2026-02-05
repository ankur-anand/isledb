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

	logs        map[string][]byte
	casExpected []string
	readCount   int
}

func newETagStorage() *etagStorage {
	return &etagStorage{
		logs: make(map[string][]byte),
	}
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

func (s *etagStorage) ReadLog(ctx context.Context, path string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.logs[path]
	if !ok {
		return nil, ErrNotFound
	}
	return append([]byte(nil), data...), nil
}

func (s *etagStorage) WriteLog(ctx context.Context, name string, data []byte) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.logs[name]; exists {
		return "", ErrPreconditionFailed
	}
	s.logs[name] = append([]byte(nil), data...)
	return name, nil
}

func (s *etagStorage) ListLogs(ctx context.Context) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	paths := make([]string, 0, len(s.logs))
	for path := range s.logs {
		paths = append(paths, path)
	}
	return paths, nil
}

func (s *etagStorage) LogPath(name string) string {
	return name
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

func TestAppendCurrent_UsesCachedETagAndSingleRead(t *testing.T) {
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

func TestAppendCurrent_UsesUpdatedETagAcrossAppends(t *testing.T) {
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
