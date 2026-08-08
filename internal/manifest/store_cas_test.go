package manifest

import (
	"context"
	"errors"
	"fmt"
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

type cancelCASStorage struct {
	base   Storage
	cancel context.CancelFunc

	mu    sync.Mutex
	calls int
}

type appliedThenErrorStorage struct {
	Storage

	mu       sync.Mutex
	failNext bool
	failErr  error
}

type trackingCASStorage struct {
	Storage

	mu           sync.Mutex
	activeWrites int
	maxActive    int
	conflicts    int
}

func (s *trackingCASStorage) WriteCurrentCAS(ctx context.Context, data []byte, expectedETag string) (string, error) {
	s.mu.Lock()
	if s.conflicts > 0 {
		s.conflicts--
		s.mu.Unlock()
		return "", ErrPreconditionFailed
	}
	s.activeWrites++
	if s.activeWrites > s.maxActive {
		s.maxActive = s.activeWrites
	}
	s.mu.Unlock()

	time.Sleep(2 * time.Millisecond)
	etag, err := s.Storage.WriteCurrentCAS(ctx, data, expectedETag)

	s.mu.Lock()
	s.activeWrites--
	s.mu.Unlock()
	return etag, err
}

func (s *trackingCASStorage) armConflicts(count int) {
	s.mu.Lock()
	s.conflicts = count
	s.mu.Unlock()
}

func (s *trackingCASStorage) maximumActiveWrites() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.maxActive
}

func (s *appliedThenErrorStorage) arm(err error) {
	s.mu.Lock()
	s.failNext = true
	s.failErr = err
	s.mu.Unlock()
}

func (s *appliedThenErrorStorage) WriteCurrentCAS(ctx context.Context, data []byte, expectedETag string) (string, error) {
	etag, err := s.Storage.WriteCurrentCAS(ctx, data, expectedETag)
	if err != nil {
		return "", err
	}

	s.mu.Lock()
	fail := s.failNext
	failErr := s.failErr
	if fail {
		s.failNext = false
	}
	s.mu.Unlock()
	if fail {
		return "", failErr
	}
	return etag, nil
}

func (s *casInjectStorage) ReadCurrent(ctx context.Context) ([]byte, string, error) {
	return s.base.ReadCurrent(ctx)
}

func (s *casInjectStorage) ReadMaintenanceHead(ctx context.Context) ([]byte, string, error) {
	return s.base.ReadMaintenanceHead(ctx)
}

func (s *casInjectStorage) WriteMaintenanceHeadCAS(ctx context.Context, data []byte, expectedETag string) (string, error) {
	return s.base.WriteMaintenanceHeadCAS(ctx, data, expectedETag)
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

func (s *cancelCASStorage) ReadCurrent(ctx context.Context) ([]byte, string, error) {
	return s.base.ReadCurrent(ctx)
}

func (s *cancelCASStorage) ReadMaintenanceHead(ctx context.Context) ([]byte, string, error) {
	return s.base.ReadMaintenanceHead(ctx)
}

func (s *cancelCASStorage) WriteMaintenanceHeadCAS(ctx context.Context, data []byte, expectedETag string) (string, error) {
	return s.base.WriteMaintenanceHeadCAS(ctx, data, expectedETag)
}

func (s *cancelCASStorage) WriteCurrentCAS(ctx context.Context, data []byte, expectedETag string) (string, error) {
	s.mu.Lock()
	s.calls++
	first := s.calls == 1
	cancel := s.cancel
	s.mu.Unlock()

	if first && cancel != nil {
		cancel()
	}
	return "", ErrPreconditionFailed
}

func (s *cancelCASStorage) ReadSnapshot(ctx context.Context, path string) ([]byte, error) {
	return s.base.ReadSnapshot(ctx, path)
}

func (s *cancelCASStorage) WriteSnapshot(ctx context.Context, id string, data []byte) (string, error) {
	return s.base.WriteSnapshot(ctx, id, data)
}

func TestAppendWriterCommitReconcilesAppliedCASAfterLostResponse(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("writer-commit-lost-response")
	defer store.Close()

	base := NewBlobStoreBackend(store)
	storage := &appliedThenErrorStorage{Storage: base}
	manifestStore := NewStoreWithStorage(storage)
	if _, err := manifestStore.ClaimWriter(ctx, "writer-1"); err != nil {
		t.Fatalf("ClaimWriter: %v", err)
	}

	commit := WriterCommit{
		ID: "commit-1",
		SSTable: SSTMeta{
			ID:    "sst-1",
			SeqLo: 10,
			SeqHi: 20,
			Level: 0,
		},
		ChangeBatch: &ChangeBatchMeta{
			ID:      "change-1",
			Path:    "changes/change-1",
			SeqLo:   10,
			SeqHi:   20,
			Payload: ChangeFeedPayloadFullValues,
		},
	}
	lostResponse := errors.New("lost CURRENT response")
	storage.arm(lostResponse)
	if _, err := manifestStore.AppendWriterCommit(ctx, commit); !errors.Is(err, lostResponse) {
		t.Fatalf("first AppendWriterCommit error=%v, want %v", err, lostResponse)
	}

	// A successor may claim the writer fence after the commit reached CURRENT
	// but before the original writer receives the response. The old writer must
	// still recognize its completed commit instead of reporting a false failure.
	successor := NewStoreWithStorage(base)
	if _, err := successor.ClaimWriter(ctx, "writer-2"); err != nil {
		t.Fatalf("successor ClaimWriter: %v", err)
	}

	entry, err := manifestStore.AppendWriterCommit(ctx, commit)
	if err != nil {
		t.Fatalf("retry AppendWriterCommit: %v", err)
	}
	if entry.CommitID != commit.ID || entry.SSTable == nil || entry.SSTable.ID != commit.SSTable.ID {
		t.Fatalf("reconciled entry=%+v", entry)
	}

	data, _, err := base.ReadCurrent(ctx)
	if err != nil {
		t.Fatalf("ReadCurrent: %v", err)
	}
	current, err := DecodeCurrent(data)
	if err != nil {
		t.Fatalf("DecodeCurrent: %v", err)
	}
	marker := current.LastWriterCommit
	if marker == nil || marker.CommitID != commit.ID || marker.Fingerprint == "" ||
		marker.SeqLo != 10 || marker.SeqHi != 20 {
		t.Fatalf("last writer commit=%+v", marker)
	}

	seqs, err := manifestStore.ListEntries(ctx)
	if err != nil {
		t.Fatalf("ListEntries: %v", err)
	}
	commits := 0
	for _, seq := range seqs {
		entry, err := manifestStore.ReadEntry(ctx, seq)
		if err != nil {
			t.Fatalf("ReadEntry(%d): %v", seq, err)
		}
		if entry.CommitID == commit.ID {
			commits++
		}
	}
	if commits != 1 {
		t.Fatalf("committed entries with commit_id=%q: got=%d want=1", commit.ID, commits)
	}

	conflict := commit
	conflict.SSTable.Checksum = "sha256:different"
	if _, err := manifestStore.AppendWriterCommit(ctx, conflict); !errors.Is(err, ErrWriterCommitConflict) {
		t.Fatalf("conflicting retry error=%v, want %v", err, ErrWriterCommitConflict)
	}
}

func TestAppendWriterCommitRejectsInvalidIdentity(t *testing.T) {
	store := blobstore.NewMemory("writer-commit-invalid")
	defer store.Close()
	manifestStore := NewStoreWithStorage(NewBlobStoreBackend(store))

	tests := []WriterCommit{
		{SSTable: SSTMeta{ID: "sst", SeqLo: 1, SeqHi: 1}},
		{ID: string(make([]byte, maxWriterCommitIDBytes+1)), SSTable: SSTMeta{ID: "sst", SeqLo: 1, SeqHi: 1}},
		{ID: "commit", SSTable: SSTMeta{SeqLo: 1, SeqHi: 1}},
		{ID: "commit", SSTable: SSTMeta{ID: "sst", SeqLo: 2, SeqHi: 1}},
		{
			ID:      "commit",
			SSTable: SSTMeta{ID: "sst", SeqLo: 1, SeqHi: 2},
			ChangeBatch: &ChangeBatchMeta{
				ID: "change", Path: "changes/change", SeqLo: 1, SeqHi: 3,
				Payload: ChangeFeedPayloadFullValues,
			},
		},
	}
	for i, commit := range tests {
		if _, err := manifestStore.AppendWriterCommit(context.Background(), commit); !errors.Is(err, ErrInvalidWriterCommit) {
			t.Fatalf("case %d error=%v, want %v", i, err, ErrInvalidWriterCommit)
		}
	}
}

func (s *cancelCASStorage) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func TestAppendEntry_CASRetry_SucceedsWhenFenceValid(t *testing.T) {
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

func TestAppendEntry_CASRetry_SurvivesTransientConflictBurst(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("cas-retry-burst")
	defer store.Close()

	tracked := &trackingCASStorage{Storage: NewBlobStoreBackend(store)}
	ms := NewStoreWithStorage(tracked)
	if _, err := ms.Replay(ctx); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if _, err := ms.ClaimWriter(ctx, "writer-1"); err != nil {
		t.Fatalf("claim writer: %v", err)
	}

	tracked.armConflicts(4)
	if _, err := ms.AppendAddSSTableWithFence(ctx, SSTMeta{ID: "burst.sst", Epoch: 1, Level: 0}); err != nil {
		t.Fatalf("append after transient conflict burst: %v", err)
	}
}

func TestStoreSerializesLocalWriterAndCompactorCAS(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("cas-local-roles")
	defer store.Close()

	tracked := &trackingCASStorage{Storage: NewBlobStoreBackend(store)}
	ms := NewStoreWithStorage(tracked)
	if _, err := ms.Replay(ctx); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if _, err := ms.ClaimWriter(ctx, "writer-1"); err != nil {
		t.Fatalf("claim writer: %v", err)
	}
	compactorFence, err := ms.ClaimCompactor(ctx, "compactor-1")
	if err != nil {
		t.Fatalf("claim compactor: %v", err)
	}

	const commitsPerRole = 12
	start := make(chan struct{})
	errs := make(chan error, 2)
	go func() {
		<-start
		for i := 0; i < commitsPerRole; i++ {
			_, err := ms.AppendAddSSTableWithFence(ctx, SSTMeta{
				ID:    fmt.Sprintf("writer-%02d.sst", i),
				Epoch: 1,
				Level: 0,
			})
			if err != nil {
				errs <- err
				return
			}
		}
		errs <- nil
	}()
	go func() {
		<-start
		for i := 0; i < commitsPerRole; i++ {
			entry := &ManifestLogEntry{
				Op: LogOpFenceClaim,
				FenceClaim: &FenceClaimPayload{
					Role:      FenceRoleCompactor,
					Epoch:     compactorFence.Epoch,
					Owner:     compactorFence.Owner,
					ClaimedAt: compactorFence.ClaimedAt,
				},
			}
			if err := ms.AppendWithCompactorFence(ctx, entry); err != nil {
				errs <- err
				return
			}
		}
		errs <- nil
	}()
	close(start)

	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("concurrent manifest mutation: %v", err)
		}
	}
	if got := tracked.maximumActiveWrites(); got != 1 {
		t.Fatalf("maximum concurrent CURRENT writes=%d, want 1", got)
	}

	replayed, err := ms.Replay(ctx)
	if err != nil {
		t.Fatalf("replay committed entries: %v", err)
	}
	if got := replayed.L0SSTCount(); got != commitsPerRole {
		t.Fatalf("L0 SST count=%d, want %d", got, commitsPerRole)
	}
}

func TestAppendEntry_CASFencesWhenEpochAdvanced(t *testing.T) {
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

func TestClaimFence_StopsRetryWhenContextCanceledDuringBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := blobstore.NewMemory("cas-claim-cancel")
	defer store.Close()

	base := NewBlobStoreBackend(store)
	inject := &cancelCASStorage{base: base, cancel: cancel}
	ms := NewStoreWithStorage(inject)

	_, err := ms.ClaimWriter(ctx, "writer-1")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", err)
	}
	if calls := inject.callCount(); calls != 1 {
		t.Fatalf("expected a single CAS attempt before cancellation, got %d", calls)
	}
}
