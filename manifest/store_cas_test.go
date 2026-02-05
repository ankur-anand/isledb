package manifest

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
)

type casInjectStorage struct {
	base Storage

	mu              sync.Mutex
	failNextCAS     bool
	bumpWriterFence bool
}

func (s *casInjectStorage) ReadCurrent(ctx context.Context) ([]byte, string, error) {
	return s.base.ReadCurrent(ctx)
}

func (s *casInjectStorage) WriteCurrentCAS(ctx context.Context, data []byte, expectedETag string) (string, error) {
	s.mu.Lock()
	fail := s.failNextCAS
	bump := s.bumpWriterFence
	if fail {
		s.failNextCAS = false
	}
	if bump {
		s.bumpWriterFence = false
	}
	s.mu.Unlock()

	if fail {
		if bump {
			_ = s.bumpWriterFenceEpoch(ctx)
		}
		return "", ErrPreconditionFailed
	}

	return s.base.WriteCurrentCAS(ctx, data, expectedETag)
}

func (s *casInjectStorage) ReadSnapshot(ctx context.Context, path string) ([]byte, error) {
	return s.base.ReadSnapshot(ctx, path)
}

func (s *casInjectStorage) WriteSnapshot(ctx context.Context, id string, data []byte) (string, error) {
	return s.base.WriteSnapshot(ctx, id, data)
}

func (s *casInjectStorage) ReadLog(ctx context.Context, path string) ([]byte, error) {
	return s.base.ReadLog(ctx, path)
}

func (s *casInjectStorage) WriteLog(ctx context.Context, name string, data []byte) (string, error) {
	return s.base.WriteLog(ctx, name, data)
}

func (s *casInjectStorage) ListLogs(ctx context.Context) ([]string, error) {
	return s.base.ListLogs(ctx)
}

func (s *casInjectStorage) LogPath(name string) string {
	return s.base.LogPath(name)
}

func (s *casInjectStorage) bumpWriterFenceEpoch(ctx context.Context) error {
	data, etag, err := s.base.ReadCurrent(ctx)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return err
	}
	if errors.Is(err, ErrNotFound) {
		etag = ""
	}

	var current *Current
	if len(data) > 0 {
		decoded, err := DecodeCurrent(data)
		if err != nil {
			return err
		}
		current = decoded
	}
	if current == nil {
		current = &Current{NextEpoch: 1}
	}

	newEpoch := uint64(1)
	if current.WriterFence != nil {
		newEpoch = current.WriterFence.Epoch + 1
	} else if current.NextEpoch > 0 {
		newEpoch = current.NextEpoch
	}

	current.WriterFence = &FenceToken{
		Epoch:     newEpoch,
		Owner:     "bumped-writer",
		ClaimedAt: time.Now().UTC(),
	}
	if current.NextEpoch <= newEpoch {
		current.NextEpoch = newEpoch + 1
	}

	encoded, err := EncodeCurrent(current)
	if err != nil {
		return err
	}
	_, err = s.base.WriteCurrentCAS(ctx, encoded, etag)
	return err
}

func TestAppendCurrent_CASRetry_SucceedsWhenFenceValid(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("cas-retry")
	defer store.Close()

	base := NewBlobStoreBackend(store)
	inject := &casInjectStorage{base: base}
	ms := NewStoreWithStorage(inject)

	if _, err := ms.Replay(ctx); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if _, err := ms.ClaimWriter(ctx, "writer-1"); err != nil {
		t.Fatalf("claim writer: %v", err)
	}

	inject.mu.Lock()
	inject.failNextCAS = true
	inject.mu.Unlock()

	if _, err := ms.AppendAddSSTableWithFence(ctx, SSTMeta{ID: "a.sst", Epoch: 1, Level: 0}); err != nil {
		t.Fatalf("append add sstable: %v", err)
	}
}

func TestAppendCurrent_CASFencesWhenEpochAdvanced(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("cas-fence")
	defer store.Close()

	base := NewBlobStoreBackend(store)
	inject := &casInjectStorage{base: base}
	ms := NewStoreWithStorage(inject)

	if _, err := ms.Replay(ctx); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if _, err := ms.ClaimWriter(ctx, "writer-1"); err != nil {
		t.Fatalf("claim writer: %v", err)
	}

	inject.mu.Lock()
	inject.failNextCAS = true
	inject.bumpWriterFence = true
	inject.mu.Unlock()

	_, err := ms.AppendAddSSTableWithFence(ctx, SSTMeta{ID: "b.sst", Epoch: 1, Level: 0})
	if !errors.Is(err, ErrFenced) {
		t.Fatalf("expected ErrFenced, got %v", err)
	}
}
