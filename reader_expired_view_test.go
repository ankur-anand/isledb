package isledb

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal"
	"github.com/ankur-anand/isledb/internal/manifest"
)

// frozenCurrentStorage answers CURRENT reads with frozen once it is set, as a
// store that went back would.
type frozenCurrentStorage struct {
	pagedManifestStorage
	mu     sync.Mutex
	frozen []byte
}

func (s *frozenCurrentStorage) ReadCurrent(ctx context.Context) ([]byte, string, error) {
	s.mu.Lock()
	frozen := s.frozen
	s.mu.Unlock()
	if frozen != nil {
		return frozen, "frozen", nil
	}
	return s.pagedManifestStorage.ReadCurrent(ctx)
}

func (s *frozenCurrentStorage) freeze(data []byte) {
	s.mu.Lock()
	s.frozen = data
	s.mu.Unlock()
}

// TestReadOnAnExpiredViewFailsWhenNoRefreshRenewsIt: CURRENT goes back
// behind the view the reader holds, and the view expires. A refresh then
// succeeds without renewing it; the read must fail rather than answer from
// the expired view, and succeed again once CURRENT is back.
func TestReadOnAnExpiredViewFailsWhenNoRefreshRenewsIt(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("reader-expired-view")
	t.Cleanup(func() { _ = store.Close() })
	ms := manifest.NewStore(store)
	writeTestSST(t, ctx, store, ms, []internal.MemEntry{
		{Key: []byte("a"), Seq: 1, Kind: internal.OpPut, Value: []byte("1")},
	}, 0, 1)
	backend := manifest.NewBlobStoreBackend(store)
	older, _, err := backend.ReadCurrent(ctx)
	if err != nil {
		t.Fatal(err)
	}
	storage := &frozenCurrentStorage{pagedManifestStorage: backend}
	reader, err := newReader(ctx, store, readerOptions{
		CacheDir: t.TempDir(), ManifestStorage: storage, DisableManifestPageCache: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reader.Close() })
	commitKeyB(t, ctx, store, ms)
	if err := reader.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if _, found, err := reader.Get(ctx, []byte("a")); err != nil || !found {
		t.Fatalf("warm read: %v %v", found, err)
	}

	storage.freeze(older)
	reader.mu.Lock()
	reader.viewExpiresAt = time.Now().Add(-time.Second)
	reader.mu.Unlock()
	if _, _, err := reader.Get(ctx, []byte("a")); !errors.Is(err, ErrReadViewExpired) {
		t.Fatalf("read on an expired view no refresh renewed: %v, want %v", err, ErrReadViewExpired)
	}

	storage.freeze(nil)
	if v, found, err := reader.Get(ctx, []byte("a")); err != nil || !found || string(v) != "1" {
		t.Fatalf("read once CURRENT is back: %q %v %v", v, found, err)
	}
}
