package manifest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/segmentio/ksuid"
)

var (
	ErrFenced                    = errors.New("fenced: epoch superseded by newer owner")
	ErrFenceConflict             = errors.New("fence conflict: concurrent claim detected")
	ErrCurrentTooLarge           = errors.New("manifest CURRENT exceeds size limit")
	ErrInvalidWriterCommit       = errors.New("invalid writer commit")
	ErrWriterCommitConflict      = errors.New("writer commit identity conflict")
	ErrCommitIndeterminate       = errors.New("writer commit outcome indeterminate")
	ErrChangeFeedRequired        = errors.New("change feed batch required")
	ErrInvalidChangeFeedPayload  = errors.New("invalid change feed payload")
	ErrChangeFeedPayloadMismatch = errors.New("change feed payload mismatch")
	ErrChangeFeedHistory         = errors.New("change feed history unavailable")
	ErrChangeFeedPosition        = errors.New("invalid change feed position")
	ErrInvalidRetirement         = errors.New("invalid retired object batch")
	ErrInvalidManifest           = errors.New("invalid manifest topology")
	ErrStorePolicyMismatch       = errors.New("store policy mismatch")
	ErrCurrentUnavailable        = errors.New("manifest CURRENT is unavailable")
)

const (
	defaultActiveEntryLimit = 64
	defaultPageFanout       = 32
	defaultMaxCurrentBytes  = 64 << 10
	currentCASMaxRetries    = 8
	maxWriterCommitIDBytes  = 128
)

type FenceRole int

const (
	FenceRoleWriter FenceRole = iota
	FenceRoleCompactor
)

type replayCache struct {
	manifest *Manifest

	// snapshot and logSeqStart identify the base; if either changes we
	// must fall back to a full replay.
	snapshot    *ObjectRef
	logSeqStart uint64

	// nextSeq is the CURRENT.NextSeq at the time of the last replay.
	// On an incremental replay we only read entries [nextSeq, current.NextSeq).
	nextSeq uint64

	// Cached active fence epochs so we can continue fence filtering
	// correctly for the delta entries.
	activeWriterEpoch    uint64
	activeCompactorEpoch uint64

	// Fence epochs from CURRENT used to build this cache. If CURRENT fence
	// epochs change, incremental replay must fall back to full replay.
	writerFenceEpoch    uint64
	compactorFenceEpoch uint64
}

type Store struct {
	storage Storage

	// commitMu serializes read-modify-CAS operations issued through this Store.
	// Writer and maintenance are independent fenced roles, but they share one
	// CURRENT object and must not make each other exhaust optimistic retries.
	commitMu sync.Mutex
	mu       sync.Mutex
	nextSeq  uint64

	// current is the latest CURRENT observed by any read and backs
	// CurrentData. commitCurrent/currentETag are a matched read-modify-CAS
	// pair. Read-only replay must never replace that pair: some storage
	// adapters obtain object bytes and the match token with separate requests,
	// so a concurrent publication can otherwise produce old bytes paired with
	// a new token and let the next append overwrite a committed entry.
	current       *Current
	commitCurrent *Current
	currentETag   string
	// currentObserved is monotonic. Once this Store has read or published a
	// valid CURRENT, a later missing head is an availability failure rather than
	// permission to reinterpret the database as empty.
	currentObserved bool

	writerFence    *FenceToken
	compactorFence *FenceToken

	// pendingWriterCommit records the exact immutable manifest position used
	// by a writer CAS whose outcome was not acknowledged. commitMu protects it.
	// A Store can have only one such append in flight because appends are
	// serialized by commitMu.
	pendingWriterCommit *WriterCommitMarker

	rcache *replayCache

	activeEntryLimit int
	pageFanout       int
	maxCurrentBytes  int
}

func NewStore(store *blobstore.Store) *Store {
	return NewStoreWithStorage(NewBlobStoreBackend(store))
}

func NewStoreWithStorage(storage Storage) *Store {
	return &Store{
		storage:          storage,
		activeEntryLimit: defaultActiveEntryLimit,
		pageFanout:       defaultPageFanout,
		maxCurrentBytes:  defaultMaxCurrentBytes,
	}
}

func (s *Store) Storage() Storage {
	return s.storage
}

// CurrentData returns the last decoded CURRENT snapshot cached by the store.
// It does not perform I/O.
func (s *Store) CurrentData() *Current {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.current.Clone()
}

func (s *Store) ClaimWriter(ctx context.Context, ownerID string) (*FenceToken, error) {
	return s.claimWriter(ctx, ownerID, nil)
}

// ClaimWriterWithPolicy claims the writer fence and establishes the store's
// immutable pinned-view lifetime. Later writer claims must use the same value.
func (s *Store) ClaimWriterWithPolicy(ctx context.Context, ownerID string, maxPinnedViewAge time.Duration) (*FenceToken, error) {
	if maxPinnedViewAge <= 0 {
		return nil, fmt.Errorf("%w: max_pinned_view_age=%s", ErrStorePolicyMismatch, maxPinnedViewAge)
	}
	return s.claimWriter(ctx, ownerID, &maxPinnedViewAge)
}

func (s *Store) claimWriter(ctx context.Context, ownerID string, maxPinnedViewAge *time.Duration) (*FenceToken, error) {
	token, err := s.claimFence(ctx, FenceRoleWriter, ownerID, maxPinnedViewAge)
	if err != nil {
		return nil, err
	}

	if err := s.writeFenceClaimEntry(ctx, FenceRoleWriter, token); err != nil {
		return nil, fmt.Errorf("write fence claim entry: %w", err)
	}

	return token, nil
}

func (s *Store) ClaimCompactor(ctx context.Context, ownerID string) (*FenceToken, error) {
	token, err := s.claimFence(ctx, FenceRoleCompactor, ownerID, nil)
	if err != nil {
		return nil, err
	}

	if err := s.writeFenceClaimEntry(ctx, FenceRoleCompactor, token); err != nil {
		return nil, fmt.Errorf("write fence claim entry: %w", err)
	}

	return token, nil
}

func (s *Store) claimFence(ctx context.Context, role FenceRole, ownerID string, maxPinnedViewAge *time.Duration) (*FenceToken, error) {
	s.commitMu.Lock()
	defer s.commitMu.Unlock()

	const maxRetries = 5

	for attempt := 0; attempt < maxRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		current, etag, err := s.readCurrentWithETag(ctx)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return nil, err
		}
		if current == nil {
			current = &Current{NextEpoch: 1}
		}
		normalizeCurrent(current)

		if role == FenceRoleWriter && maxPinnedViewAge != nil {
			if current.WriterFence == nil {
				current.MaxPinnedViewAge = *maxPinnedViewAge
			} else if current.MaxPinnedViewAge != *maxPinnedViewAge {
				return nil, fmt.Errorf("%w: stored=%s requested=%s",
					ErrStorePolicyMismatch, current.MaxPinnedViewAge, *maxPinnedViewAge)
			}
		}

		var existingFence *FenceToken
		switch role {
		case FenceRoleWriter:
			existingFence = current.WriterFence
		case FenceRoleCompactor:
			existingFence = current.CompactorFence
		}

		var newEpoch uint64 = 1
		if existingFence != nil {
			newEpoch = existingFence.Epoch + 1
		}

		newFence := &FenceToken{
			Epoch:     newEpoch,
			Owner:     ownerID,
			ClaimedAt: time.Now().UTC(),
		}

		switch role {
		case FenceRoleWriter:
			current.WriterFence = newFence
		case FenceRoleCompactor:
			current.CompactorFence = newFence
		}

		if err := s.writeCurrentWithCAS(ctx, current, etag); err != nil {
			if errors.Is(err, ErrPreconditionFailed) {
				backoff := time.Millisecond * 10 * time.Duration(attempt+1)
				if err := sleepWithContext(ctx, backoff); err != nil {
					return nil, err
				}
				continue
			}
			return nil, err
		}

		s.mu.Lock()
		switch role {
		case FenceRoleWriter:
			s.writerFence = newFence
		case FenceRoleCompactor:
			s.compactorFence = newFence
		}
		s.mu.Unlock()

		return newFence, nil
	}

	return nil, ErrFenceConflict
}

func sleepWithContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}

	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func sleepBeforeCurrentCASRetry(ctx context.Context, attempt int) error {
	shift := attempt
	if shift > 5 {
		shift = 5
	}
	delay := 2 * time.Millisecond * time.Duration(1<<shift)
	jitterWindow := delay / 2
	jitter := time.Duration(time.Now().UnixNano() % int64(jitterWindow+1))
	return sleepWithContext(ctx, delay/2+jitter)
}

func (s *Store) writeFenceClaimEntry(ctx context.Context, role FenceRole, token *FenceToken) error {
	entry := &ManifestLogEntry{
		Role:  role,
		Epoch: token.Epoch,
		Op:    LogOpFenceClaim,
		FenceClaim: &FenceClaimPayload{
			Role:      role,
			Epoch:     token.Epoch,
			Owner:     token.Owner,
			ClaimedAt: token.ClaimedAt,
		},
	}
	return s.appendInternal(ctx, entry, role)
}

func (s *Store) CheckWriterFence(ctx context.Context) error {
	return s.checkFence(ctx, FenceRoleWriter)
}

func (s *Store) CheckCompactorFence(ctx context.Context) error {
	return s.checkFence(ctx, FenceRoleCompactor)
}

// CheckCompactorFenceToken verifies that token is still the current compactor
// fence without relying on mutable process-local Store state.
func (s *Store) CheckCompactorFenceToken(ctx context.Context, token *FenceToken) error {
	current, err := s.readCurrentData(ctx)
	if err != nil {
		return err
	}
	if current == nil {
		return ErrFenced
	}
	return checkFenceToken(token, current.CompactorFence)
}

func (s *Store) checkFence(ctx context.Context, role FenceRole) error {
	s.mu.Lock()
	var localFence *FenceToken
	switch role {
	case FenceRoleWriter:
		localFence = s.writerFence
	case FenceRoleCompactor:
		localFence = s.compactorFence
	}
	s.mu.Unlock()

	if localFence == nil {
		return ErrFenced
	}

	current, err := s.readCurrentData(ctx)
	if err != nil {
		return err
	}
	if current == nil {
		return ErrFenced
	}

	var remoteFence *FenceToken
	switch role {
	case FenceRoleWriter:
		remoteFence = current.WriterFence
	default:
		remoteFence = current.CompactorFence
	}

	return checkFenceToken(localFence, remoteFence)
}

func (s *Store) WriterEpoch() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.writerFence == nil {
		return 0
	}
	return s.writerFence.Epoch
}

// WriterFenceObservedActive reports whether token still matches the writer
// fence retained by this Store. It performs no I/O: false means this Store has
// observed the token being superseded, while true does not replace a remote
// fence check before publication.
func (s *Store) WriterFenceObservedActive(token *FenceToken) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return checkFenceToken(token, s.writerFence) == nil
}

func (s *Store) CompactorEpoch() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.compactorFence == nil {
		return 0
	}
	return s.compactorFence.Epoch
}

func (s *Store) AppendWithWriterFence(ctx context.Context, entry *ManifestLogEntry) error {
	if err := s.checkLocalFence(FenceRoleWriter); err != nil {
		return err
	}
	return s.appendInternal(ctx, entry, FenceRoleWriter)
}

func (s *Store) AppendWithCompactorFence(ctx context.Context, entry *ManifestLogEntry) error {
	if err := s.checkLocalFence(FenceRoleCompactor); err != nil {
		return err
	}
	return s.appendInternal(ctx, entry, FenceRoleCompactor)
}

func (s *Store) checkLocalFence(role FenceRole) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch role {
	case FenceRoleWriter:
		if s.writerFence == nil {
			return ErrFenced
		}
	case FenceRoleCompactor:
		if s.compactorFence == nil {
			return ErrFenced
		}
	default:
		return ErrFenced
	}
	return nil
}

func (s *Store) appendInternal(ctx context.Context, entry *ManifestLogEntry, role FenceRole) error {
	s.commitMu.Lock()
	defer s.commitMu.Unlock()

	for attempt := 0; attempt < currentCASMaxRetries; attempt++ {
		var current *Current
		var etag string
		var err error
		if attempt == 0 {
			s.mu.Lock()
			current = s.commitCurrent.Clone()
			etag = s.currentETag
			s.mu.Unlock()
		}
		if current == nil {
			current, etag, err = s.readCurrentWithETag(ctx)
			if err != nil {
				return err
			}
		}
		if current == nil {
			current = &Current{NextEpoch: 1}
		}
		normalizeCurrent(current)
		// Reconciliation below may prove that an earlier writer commit
		// succeeded and return before the ordinary fence check. Remember any
		// ownership change now so callers can report that old commit as
		// successful while still treating the local session as terminal.
		s.observeFenceWithCurrent(role, current)

		if role == FenceRoleWriter && entry.CommitID != "" && entry.Op == LogOpAddSSTable {
			if applied, err := s.reconcilePendingWriterCommit(ctx, current, entry); applied || err != nil {
				return err
			}
		}
		if applied, err := reconcileWriterCommit(current, entry); applied || err != nil {
			return err
		}
		if role == FenceRoleWriter && current.ChangeFeedEnabled && entry.Op == LogOpAddSSTable {
			if entry.ChangeBatch == nil {
				return ErrChangeFeedRequired
			}
			if entry.ChangeBatch.Payload != current.ChangeFeedPayload {
				return fmt.Errorf("%w: batch=%q configured=%q",
					ErrChangeFeedPayloadMismatch, entry.ChangeBatch.Payload, current.ChangeFeedPayload)
			}
		}

		if err := s.checkFenceWithCurrent(role, current); err != nil {
			return err
		}

		nextEntry := *entry
		nextEntry.Seq = current.NextSeq
		switch role {
		case FenceRoleWriter:
			nextEntry.Role = FenceRoleWriter
			if current.WriterFence != nil {
				nextEntry.Epoch = current.WriterFence.Epoch
			}
		case FenceRoleCompactor:
			nextEntry.Role = FenceRoleCompactor
			if current.CompactorFence != nil {
				nextEntry.Epoch = current.CompactorFence.Epoch
			}
		}
		if nextEntry.ID.IsNil() {
			nextEntry.ID = ksuid.New()
		}
		if nextEntry.Timestamp.IsZero() {
			nextEntry.Timestamp = time.Now().UTC()
		}
		if err := validateRetiredObjects(&nextEntry); err != nil {
			return err
		}

		updated := current.Clone()
		if updated == nil {
			updated = &Current{NextEpoch: 1}
		}
		normalizeCurrent(updated)
		if len(updated.ActiveEntries) >= s.activeLimit() {
			if err := s.rotateActiveEntries(ctx, updated); err != nil {
				return err
			}
		}
		if updated.LogSeqStart == updated.NextSeq {
			updated.LogSeqStart = nextEntry.Seq
		}
		if !updated.ChangeFeedEnabled && updated.ChangeFeedLogStart == 0 {
			updated.ChangeFeedLogStart = updated.LogSeqStart
		}
		updated.ActiveEntries = append(updated.ActiveEntries, nextEntry)
		updated.NextSeq = nextEntry.Seq + 1
		updated.NextEpoch = nextEpochFromEntry(updated.NextEpoch, &nextEntry)
		var writerMarker *WriterCommitMarker
		if nextEntry.Role == FenceRoleWriter && nextEntry.Op == LogOpAddSSTable && nextEntry.CommitID != "" {
			writerMarker, err = writerCommitMarker(&nextEntry)
			if err != nil {
				return err
			}
			updated.LastWriterCommit = writerMarker
		}
		data, err := s.encodeCurrentForCASWithRotation(ctx, updated)
		if err != nil {
			return err
		}
		if writerMarker != nil {
			s.pendingWriterCommit = writerMarker.Clone()
		}
		if err := s.writeEncodedCurrentWithCAS(ctx, updated, data, etag); err != nil {
			if errors.Is(err, ErrPreconditionFailed) {
				s.pendingWriterCommit = nil
				if attempt+1 < currentCASMaxRetries {
					if err := sleepBeforeCurrentCASRetry(ctx, attempt); err != nil {
						return err
					}
				}
				continue
			}
			return err
		}
		s.pendingWriterCommit = nil
		*entry = nextEntry

		s.mu.Lock()
		s.nextSeq = updated.NextSeq
		s.mu.Unlock()
		return nil
	}

	return ErrFenceConflict
}

func (s *Store) AppendAddSSTableWithFence(ctx context.Context, sst SSTMeta) (*ManifestLogEntry, error) {
	return s.AppendAddSSTableWithChangeBatchWithFence(ctx, sst, nil)
}

func (s *Store) AppendAddSSTableWithChangeBatchWithFence(ctx context.Context, sst SSTMeta, changeBatch *ChangeBatchMeta) (*ManifestLogEntry, error) {
	return s.AppendWriterCommit(ctx, WriterCommit{
		ID:          writerCommitIDForSST(sst.ID),
		SSTable:     sst,
		ChangeBatch: changeBatch,
	})
}

// AppendWriterCommit idempotently publishes one uploaded memtable. Reusing the
// same commit ID after an uncertain CAS result cannot append the SST twice.
func (s *Store) AppendWriterCommit(ctx context.Context, commit WriterCommit) (*ManifestLogEntry, error) {
	if err := validateWriterCommit(commit); err != nil {
		return nil, err
	}
	sst := commit.SSTable
	var changeBatch *ChangeBatchMeta
	if commit.ChangeBatch != nil {
		copy := *commit.ChangeBatch
		changeBatch = &copy
	}
	entry := &ManifestLogEntry{
		CommitID:    commit.ID,
		Op:          LogOpAddSSTable,
		SSTable:     &sst,
		ChangeBatch: changeBatch,
	}
	if err := s.AppendWithWriterFence(ctx, entry); err != nil {
		return nil, err
	}
	return entry, nil
}

func validateWriterCommit(commit WriterCommit) error {
	if commit.ID == "" {
		return fmt.Errorf("%w: empty commit_id", ErrInvalidWriterCommit)
	}
	if len(commit.ID) > maxWriterCommitIDBytes {
		return fmt.Errorf("%w: commit_id bytes=%d max=%d", ErrInvalidWriterCommit,
			len(commit.ID), maxWriterCommitIDBytes)
	}
	if commit.SSTable.ID == "" {
		return fmt.Errorf("%w: empty sstable_id", ErrInvalidWriterCommit)
	}
	if commit.SSTable.Level != 0 {
		return fmt.Errorf("%w: writer SST level=%d, want L0", ErrInvalidWriterCommit, commit.SSTable.Level)
	}
	if commit.SSTable.SeqHi < commit.SSTable.SeqLo {
		return fmt.Errorf("%w: invalid sequence range %d-%d", ErrInvalidWriterCommit,
			commit.SSTable.SeqLo, commit.SSTable.SeqHi)
	}
	if change := commit.ChangeBatch; change != nil {
		if change.ID == "" || change.Path == "" {
			return fmt.Errorf("%w: incomplete change batch", ErrInvalidWriterCommit)
		}
		if !change.Payload.Valid() {
			return fmt.Errorf("%w: change batch payload=%q", ErrInvalidWriterCommit, change.Payload)
		}
		if change.Epoch != commit.SSTable.Epoch {
			return fmt.Errorf("%w: change batch epoch=%d does not match sstable epoch=%d",
				ErrInvalidWriterCommit, change.Epoch, commit.SSTable.Epoch)
		}
		if change.SeqLo != commit.SSTable.SeqLo || change.SeqHi != commit.SSTable.SeqHi {
			return fmt.Errorf("%w: change batch range %d-%d does not match sstable %d-%d",
				ErrInvalidWriterCommit, change.SeqLo, change.SeqHi,
				commit.SSTable.SeqLo, commit.SSTable.SeqHi)
		}
	}
	return nil
}

func reconcileWriterCommit(current *Current, entry *ManifestLogEntry) (bool, error) {
	if current == nil || entry == nil || entry.CommitID == "" ||
		entry.Role == FenceRoleCompactor || entry.Op != LogOpAddSSTable {
		return false, nil
	}
	marker := current.LastWriterCommit
	if marker == nil || marker.CommitID != entry.CommitID {
		return false, nil
	}
	return reconcileWriterCommitMarker(marker, entry)
}

// reconcilePendingWriterCommit resolves an unacknowledged writer CAS from the
// immutable manifest entry at the exact sequence used by that CAS. The
// sequence locates the committed position; equality of the full marker proves
// that the position contains this writer's exact commit.
func (s *Store) reconcilePendingWriterCommit(
	ctx context.Context,
	current *Current,
	entry *ManifestLogEntry,
) (bool, error) {
	marker := s.pendingWriterCommit
	if marker == nil {
		return false, nil
	}
	if entry == nil || entry.SSTable == nil || entry.CommitID != marker.CommitID {
		return true, fmt.Errorf("%w: pending commit_id=%q retry commit_id=%q",
			ErrWriterCommitConflict, marker.CommitID, entry.CommitID)
	}
	fingerprint, err := writerCommitFingerprint(entry.SSTable, entry.ChangeBatch)
	if err != nil {
		return true, err
	}
	if fingerprint != marker.Fingerprint {
		return true, fmt.Errorf("%w: commit_id=%q metadata mismatch",
			ErrWriterCommitConflict, entry.CommitID)
	}

	retainedFrom := retainedEntryFloor(current)
	if marker.ManifestSeq < retainedFrom {
		return true, fmt.Errorf("%w: commit_id=%q manifest_seq=%d retained_from=%d",
			ErrCommitIndeterminate, marker.CommitID, marker.ManifestSeq, retainedFrom)
	}
	if current == nil || marker.ManifestSeq >= current.NextSeq {
		// The attempted position has not been committed. A later CAS may safely
		// reuse the logical commit at the current head.
		s.pendingWriterCommit = nil
		return false, nil
	}

	entries, err := s.entriesInRange(ctx, current, marker.ManifestSeq, marker.ManifestSeq+1)
	if err != nil {
		return true, fmt.Errorf("read uncertain writer commit at manifest seq=%d: %w",
			marker.ManifestSeq, err)
	}
	committed := entries[0]
	if committed.Role != FenceRoleWriter || committed.Op != LogOpAddSSTable || committed.CommitID == "" {
		s.pendingWriterCommit = nil
		return false, nil
	}
	committedMarker, err := writerCommitMarker(committed)
	if err != nil {
		return true, err
	}
	if !writerCommitMarkersEqual(committedMarker, marker) {
		// A different immutable entry owns the attempted sequence, proving this
		// CAS did not publish. Normal fence validation decides whether retrying
		// at the new head is still allowed.
		s.pendingWriterCommit = nil
		return false, nil
	}

	*entry = *committed
	s.pendingWriterCommit = nil
	return true, nil
}

func writerCommitMarkersEqual(a, b *WriterCommitMarker) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.CommitID == b.CommitID &&
		a.Fingerprint == b.Fingerprint &&
		a.EntryID == b.EntryID &&
		a.ManifestSeq == b.ManifestSeq &&
		a.WriterEpoch == b.WriterEpoch &&
		a.SeqLo == b.SeqLo &&
		a.SeqHi == b.SeqHi &&
		a.CommittedAt.Equal(b.CommittedAt)
}

func reconcileWriterCommitMarker(marker *WriterCommitMarker, entry *ManifestLogEntry) (bool, error) {
	if entry.SSTable == nil {
		return true, fmt.Errorf("%w: commit_id=%q", ErrWriterCommitConflict, entry.CommitID)
	}
	fingerprint, err := writerCommitFingerprint(entry.SSTable, entry.ChangeBatch)
	if err != nil {
		return true, err
	}
	if marker.Fingerprint != fingerprint {
		return true, fmt.Errorf("%w: commit_id=%q metadata mismatch", ErrWriterCommitConflict, entry.CommitID)
	}
	entry.ID = marker.EntryID
	entry.Seq = marker.ManifestSeq
	entry.Role = FenceRoleWriter
	entry.Epoch = marker.WriterEpoch
	entry.Timestamp = marker.CommittedAt
	return true, nil
}

func writerCommitMarker(entry *ManifestLogEntry) (*WriterCommitMarker, error) {
	if entry == nil || entry.SSTable == nil {
		return nil, fmt.Errorf("%w: missing sstable metadata", ErrInvalidWriterCommit)
	}
	fingerprint, err := writerCommitFingerprint(entry.SSTable, entry.ChangeBatch)
	if err != nil {
		return nil, err
	}
	return &WriterCommitMarker{
		CommitID:    entry.CommitID,
		Fingerprint: fingerprint,
		EntryID:     entry.ID,
		ManifestSeq: entry.Seq,
		WriterEpoch: entry.Epoch,
		SeqLo:       entry.SSTable.SeqLo,
		SeqHi:       entry.SSTable.SeqHi,
		CommittedAt: entry.Timestamp,
	}, nil
}

func writerCommitIDForSST(sstableID string) string {
	sum := sha256.Sum256([]byte(sstableID))
	return fmt.Sprintf("sst:%x", sum[:])
}

func writerCommitFingerprint(sstable *SSTMeta, changeBatch *ChangeBatchMeta) (string, error) {
	payload, err := json.Marshal(struct {
		SSTable     *SSTMeta         `json:"sstable"`
		ChangeBatch *ChangeBatchMeta `json:"change_batch,omitempty"`
	}{
		SSTable:     sstable,
		ChangeBatch: changeBatch,
	})
	if err != nil {
		return "", fmt.Errorf("%w: encode metadata fingerprint: %v", ErrInvalidWriterCommit, err)
	}
	sum := sha256.Sum256(payload)
	return fmt.Sprintf("sha256:%x", sum[:]), nil
}

func (s *Store) AppendRemoveSSTablesWithFence(ctx context.Context, sstableIDs []string, retired []RetiredObject) (*ManifestLogEntry, error) {
	entry := &ManifestLogEntry{
		Op:               LogOpRemoveSSTable,
		RemoveSSTableIDs: sstableIDs,
		RetiredObjects:   retired,
	}
	if err := s.AppendWithCompactorFence(ctx, entry); err != nil {
		return nil, err
	}
	return entry, nil
}

func (s *Store) AppendCompactionWithFence(ctx context.Context, payload CompactionLogPayload, retired []RetiredObject) (*ManifestLogEntry, error) {
	if err := validateCompactionPayload(payload); err != nil {
		return nil, err
	}
	entry := &ManifestLogEntry{
		Op:             LogOpCompaction,
		Compaction:     &payload,
		RetiredObjects: retired,
	}
	if err := s.AppendWithCompactorFence(ctx, entry); err != nil {
		return nil, err
	}
	return entry, nil
}

func validateCompactionPayload(payload CompactionLogPayload) error {
	if payload.SourceLevel == ^uint32(0) || payload.DestinationLevel != payload.SourceLevel+1 {
		return fmt.Errorf("%w: compaction source=L%d destination=L%d must target the adjacent level",
			ErrInvalidManifest, payload.SourceLevel, payload.DestinationLevel)
	}
	if len(payload.RemoveSSTableIDs) == 0 {
		return fmt.Errorf("%w: compaction has no inputs", ErrInvalidManifest)
	}
	if len(payload.RemoveSSTableIDs) > MaxRetiredObjectsPerEntry || len(payload.AddSSTables) > MaxRetiredObjectsPerEntry {
		return fmt.Errorf("%w: compaction inputs=%d outputs=%d max=%d", ErrInvalidManifest,
			len(payload.RemoveSSTableIDs), len(payload.AddSSTables), MaxRetiredObjectsPerEntry)
	}
	seen := make(map[string]struct{}, len(payload.RemoveSSTableIDs))
	for _, id := range payload.RemoveSSTableIDs {
		if id == "" {
			return fmt.Errorf("%w: empty compaction input id", ErrInvalidManifest)
		}
		if _, exists := seen[id]; exists {
			return fmt.Errorf("%w: duplicate compaction input id=%q", ErrInvalidManifest, id)
		}
		seen[id] = struct{}{}
	}
	outputs := append([]SSTMeta(nil), payload.AddSSTables...)
	for _, sst := range outputs {
		if sst.ID == "" || sst.Level != payload.DestinationLevel {
			return fmt.Errorf("%w: output id=%q level=%d destination=%d", ErrInvalidManifest,
				sst.ID, sst.Level, payload.DestinationLevel)
		}
	}
	sort.Slice(outputs, func(i, j int) bool {
		return bytes.Compare(outputs[i].MinKey, outputs[j].MinKey) < 0
	})
	for i := 1; i < len(outputs); i++ {
		if bytes.Compare(outputs[i-1].MaxKey, outputs[i].MinKey) >= 0 {
			return fmt.Errorf("%w: overlapping compaction outputs %q and %q", ErrInvalidManifest,
				outputs[i-1].ID, outputs[i].ID)
		}
	}
	return nil
}

func validateReplayEntry(entry *ManifestLogEntry) error {
	if entry == nil || entry.Op != LogOpCompaction {
		return nil
	}
	if entry.Compaction == nil {
		return fmt.Errorf("%w: compaction entry seq=%d has no payload", ErrInvalidManifest, entry.Seq)
	}
	if err := validateCompactionPayload(*entry.Compaction); err != nil {
		return fmt.Errorf("compaction entry seq=%d: %w", entry.Seq, err)
	}
	return nil
}

func validateRetiredObjects(entry *ManifestLogEntry) error {
	if entry == nil {
		return fmt.Errorf("%w: nil manifest entry", ErrInvalidRetirement)
	}
	if len(entry.RetiredObjects) > MaxRetiredObjectsPerEntry {
		return fmt.Errorf("%w: count=%d max=%d", ErrInvalidRetirement, len(entry.RetiredObjects), MaxRetiredObjectsPerEntry)
	}

	removed := make(map[string]struct{})
	switch entry.Op {
	case LogOpRemoveSSTable:
		for _, id := range entry.RemoveSSTableIDs {
			removed[id] = struct{}{}
		}
	case LogOpCompaction:
		if entry.Compaction != nil {
			added := make(map[string]struct{}, len(entry.Compaction.AddSSTables))
			for _, sst := range entry.Compaction.AddSSTables {
				added[sst.ID] = struct{}{}
			}
			for _, id := range entry.Compaction.RemoveSSTableIDs {
				if _, stillLive := added[id]; !stillLive {
					removed[id] = struct{}{}
				}
			}
		}
	}

	retiredSSTs := make(map[string]struct{}, len(entry.RetiredObjects))
	seenKeys := make(map[string]struct{}, len(entry.RetiredObjects))
	for _, retired := range entry.RetiredObjects {
		if retired.Kind != RetiredObjectSST {
			return fmt.Errorf("%w: unsupported kind=%q", ErrInvalidRetirement, retired.Kind)
		}
		if retired.ID == "" || retired.Key == "" || retired.Size < 0 {
			return fmt.Errorf("%w: incomplete object kind=%q id=%q key=%q", ErrInvalidRetirement, retired.Kind, retired.ID, retired.Key)
		}
		if _, exists := seenKeys[retired.Key]; exists {
			return fmt.Errorf("%w: duplicate key=%q", ErrInvalidRetirement, retired.Key)
		}
		seenKeys[retired.Key] = struct{}{}
		if _, exists := removed[retired.ID]; !exists {
			return fmt.Errorf("%w: sst id=%q is not removed by entry", ErrInvalidRetirement, retired.ID)
		}
		retiredSSTs[retired.ID] = struct{}{}
	}

	for id := range removed {
		if _, exists := retiredSSTs[id]; !exists {
			return fmt.Errorf("%w: removed sst id=%q has no retirement record", ErrInvalidRetirement, id)
		}
	}
	return nil
}

func (s *Store) activeLimit() int {
	if s.activeEntryLimit <= 0 {
		return defaultActiveEntryLimit
	}
	return s.activeEntryLimit
}

func (s *Store) frontierFanout() int {
	if s.pageFanout <= 1 {
		return defaultPageFanout
	}
	return s.pageFanout
}

func (s *Store) currentByteLimit() int {
	if s.maxCurrentBytes <= 0 {
		return defaultMaxCurrentBytes
	}
	return s.maxCurrentBytes
}

// encodeCurrentForCASWithRotation encodes CURRENT once for both its byte-limit
// check and the CAS write. If it is oversized, the active tail is rotated into
// the immutable page tree and only the resulting CURRENT is encoded again.
// Entry count remains the normal rollover trigger; this guards variable-sized
// manifest entries.
func (s *Store) encodeCurrentForCASWithRotation(ctx context.Context, current *Current) ([]byte, error) {
	data, err := s.encodeCurrentForCAS(current)
	if err == nil || !errors.Is(err, ErrCurrentTooLarge) || current == nil || len(current.ActiveEntries) == 0 {
		return data, err
	}
	if err := s.rotateActiveEntries(ctx, current); err != nil {
		return nil, err
	}
	return s.encodeCurrentForCAS(current)
}

func (s *Store) rotateActiveEntries(ctx context.Context, current *Current) error {
	if current == nil || len(current.ActiveEntries) == 0 {
		return nil
	}
	ref, err := s.writeEntryPage(ctx, current.ActiveEntries)
	if err != nil {
		return err
	}
	current.ActiveEntries = nil
	return s.addPageRef(ctx, current, ref)
}

func (s *Store) addPageRef(ctx context.Context, current *Current, ref PageRef) error {
	if err := addStateReplayPage(current, ref); err != nil {
		return err
	}
	current.IndexFrontier = append(current.IndexFrontier, ref)
	for {
		level := ref.Level
		var same []PageRef
		var keep []PageRef
		for _, existing := range current.IndexFrontier {
			if existing.Level == level {
				same = append(same, existing)
			} else {
				keep = append(keep, existing)
			}
		}
		if len(same) < s.frontierFanout() {
			sort.Slice(current.IndexFrontier, func(i, j int) bool {
				return current.IndexFrontier[i].SeqLo < current.IndexFrontier[j].SeqLo
			})
			return nil
		}
		sort.Slice(same, func(i, j int) bool {
			return same[i].SeqLo < same[j].SeqLo
		})
		indexRef, err := s.writeIndexPage(ctx, same)
		if err != nil {
			return err
		}
		if err := addStateReplayPage(current, indexRef); err != nil {
			return err
		}
		current.IndexFrontier = append(keep, indexRef)
		ref = indexRef
	}
}

func addStateReplayPage(current *Current, ref PageRef) error {
	if current == nil {
		return errors.New("nil current")
	}
	if ref.EncodedBytes == 0 {
		return fmt.Errorf("%w: page %q has zero encoded bytes", ErrInvalidManifest, ref.Path)
	}
	if current.StateReplayPages == math.MaxUint64 {
		return fmt.Errorf("%w: state replay page count overflow", ErrInvalidManifest)
	}
	if ref.EncodedBytes > math.MaxUint64-current.StateReplayBytes {
		return fmt.Errorf("%w: state replay byte count overflow", ErrInvalidManifest)
	}
	current.StateReplayPages++
	current.StateReplayBytes += ref.EncodedBytes
	if ref.Level > current.ManifestPageMaxLevel {
		current.ManifestPageMaxLevel = ref.Level
	}
	return nil
}

func (s *Store) writeEntryPage(ctx context.Context, entries []ManifestLogEntry) (PageRef, error) {
	if len(entries) == 0 {
		return PageRef{}, errors.New("empty commit page")
	}
	copied := make([]ManifestLogEntry, len(entries))
	copy(copied, entries)
	sort.Slice(copied, func(i, j int) bool {
		return copied[i].Seq < copied[j].Seq
	})
	now := time.Now().UTC()
	page := &CommitPage{
		LayoutVersion: LayoutVersion,
		PageType:      CommitPageTypeLeaf,
		Level:         0,
		SeqLo:         copied[0].Seq,
		SeqHi:         copied[len(copied)-1].Seq,
		Count:         uint32(len(copied)),
		Entries:       copied,
		CreatedAt:     now,
	}
	return s.writeCommitPage(ctx, page)
}

func (s *Store) writeIndexPage(ctx context.Context, children []PageRef) (PageRef, error) {
	if len(children) == 0 {
		return PageRef{}, errors.New("empty index page")
	}
	copied := make([]PageRef, len(children))
	copy(copied, children)
	sort.Slice(copied, func(i, j int) bool {
		return copied[i].SeqLo < copied[j].SeqLo
	})
	now := time.Now().UTC()
	page := &CommitPage{
		LayoutVersion: LayoutVersion,
		PageType:      CommitPageTypeIndex,
		Level:         copied[0].Level + 1,
		SeqLo:         copied[0].SeqLo,
		SeqHi:         copied[len(copied)-1].SeqHi,
		Count:         uint32(len(copied)),
		Children:      copied,
		CreatedAt:     now,
	}
	return s.writeCommitPage(ctx, page)
}

func (s *Store) writeCommitPage(ctx context.Context, page *CommitPage) (PageRef, error) {
	pages, ok := s.storage.(PageStorage)
	if !ok {
		return PageRef{}, errors.New("manifest page storage unsupported")
	}
	data, err := EncodeCommitPage(page)
	if err != nil {
		return PageRef{}, err
	}
	// Page reclamation is ordered by the complete-page death predicate
	// SeqHi < retained floor. Readers navigate pages through exact PageRefs and
	// never depend on listing order, so put SeqHi first and label both fields to
	// make lifecycle scans cheap without making the filename authoritative.
	id := fmt.Sprintf("h%020d-l%020d-%s", page.SeqHi, page.SeqLo, ksuid.New().String())
	path, err := pages.WritePage(ctx, page.Level, id, data)
	if err != nil {
		return PageRef{}, err
	}
	objectRef, err := newManifestObjectRef(path, data, manifestObjectKindPage, page.CreatedAt)
	if err != nil {
		return PageRef{}, err
	}
	return PageRef{
		ObjectRef: objectRef,
		Level:     page.Level,
		SeqLo:     page.SeqLo,
		SeqHi:     page.SeqHi,
		Count:     page.Count,
	}, nil
}

func (s *Store) Replay(ctx context.Context) (*Manifest, error) {
	current, err := s.readCurrent(ctx)
	if err != nil {
		return nil, err
	}
	return s.replayCurrent(ctx, current, false)
}

// ReplayWithArtifactValidation replays the manifest while validating the SST
// and Bloom metadata required by Reader cache and origin paths. Artifact
// validation is folded into the existing level-validation traversal.
func (s *Store) ReplayWithArtifactValidation(ctx context.Context) (*Manifest, error) {
	current, err := s.readCurrent(ctx)
	if err != nil {
		return nil, err
	}
	return s.replayCurrent(ctx, current, true)
}

// ReplayWithCurrent returns a manifest and the exact CURRENT value used to
// build it. Maintenance uses this to calculate scheduling pressure without a
// second object-store read that could observe a different generation.
func (s *Store) ReplayWithCurrent(ctx context.Context) (*Manifest, *Current, error) {
	current, err := s.readCurrent(ctx)
	if err != nil {
		return nil, nil, err
	}
	m, err := s.replayCurrent(ctx, current, false)
	if err != nil {
		return nil, nil, err
	}
	return m, current, nil
}

func (s *Store) replayCurrent(
	ctx context.Context,
	current *Current,
	validateArtifacts bool,
) (*Manifest, error) {

	// Attempt incremental replay: if the snapshot and log window base haven't
	// changed, we only need to read the new delta entries.
	if m, ok := s.tryIncrementalReplay(ctx, current); ok {
		if err := m.validateLevels(validateArtifacts); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrInvalidManifest, err)
		}
		return m, nil
	}
	m, err := s.fullReplay(ctx, current)
	if err != nil {
		return nil, err
	}
	if err := m.validateLevels(validateArtifacts); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidManifest, err)
	}
	return m, nil
}

// tryIncrementalReplay checks whether we can avoid a full replay by applying
// only the log entries that appeared since the last successful Replay call.
// It returns (manifest, true) on success, or (nil, false) when a full replay
// is required.
func (s *Store) tryIncrementalReplay(ctx context.Context, current *Current) (*Manifest, bool) {
	s.mu.Lock()
	rc := s.rcache
	s.mu.Unlock()

	if rc == nil || current == nil {
		return nil, false
	}

	// If the snapshot or log window base changed, we must do a full replay.
	if !objectRefsEqual(current.Snapshot, rc.snapshot) || current.LogSeqStart != rc.logSeqStart {
		return nil, false
	}

	if tokenEpoch(current.WriterFence) != rc.writerFenceEpoch ||
		tokenEpoch(current.CompactorFence) != rc.compactorFenceEpoch {
		return nil, false
	}

	// No new entries since last replay — return cached manifest directly.
	// If NextSeq regresses, we must fall back to full replay.
	if current.NextSeq == rc.nextSeq {
		m := rc.manifest.Clone()
		if current.NextEpoch > m.NextEpoch {
			m.NextEpoch = current.NextEpoch
		}
		s.mu.Lock()
		if current.NextSeq > 0 {
			s.nextSeq = current.NextSeq
		}
		s.mu.Unlock()
		return m, true
	}
	if current.NextSeq < rc.nextSeq {
		return nil, false
	}

	// Read only the delta entries: [rc.nextSeq, current.NextSeq)
	m := rc.manifest.Clone()
	activeWriterEpoch := rc.activeWriterEpoch
	activeCompactorEpoch := rc.activeCompactorEpoch
	var maxSeq uint64

	entries, err := s.entriesInRange(ctx, current, rc.nextSeq, current.NextSeq)
	if err != nil {
		return nil, false
	}
	for _, entry := range entries {
		if entry.Seq > maxSeq {
			maxSeq = entry.Seq
		}

		if entry.Op == LogOpFenceClaim {
			if entry.Role == FenceRoleWriter && entry.Epoch > activeWriterEpoch {
				activeWriterEpoch = entry.Epoch
			} else if entry.Role == FenceRoleCompactor && entry.Epoch > activeCompactorEpoch {
				activeCompactorEpoch = entry.Epoch
			}
			continue
		}

		if entry.Role == FenceRoleWriter && entry.Epoch < activeWriterEpoch {
			continue
		}
		if entry.Role == FenceRoleCompactor && entry.Epoch < activeCompactorEpoch {
			continue
		}

		if err := validateReplayEntry(entry); err != nil {
			return nil, false
		}
		m = ApplyLogEntry(m, entry)
	}

	// epoch and sequence bookkeeping.
	maxEpoch := activeWriterEpoch
	if activeCompactorEpoch > maxEpoch {
		maxEpoch = activeCompactorEpoch
	}
	if maxEpoch >= m.NextEpoch {
		m.NextEpoch = maxEpoch + 1
	}
	if current.NextEpoch > m.NextEpoch {
		m.NextEpoch = current.NextEpoch
	}
	if current.NextSeq > 0 {
		m.LogSeq = current.NextSeq - 1
	} else if maxSeq > m.LogSeq {
		m.LogSeq = maxSeq
	}

	s.mu.Lock()
	if current.NextSeq > 0 {
		s.nextSeq = current.NextSeq
	} else {
		s.nextSeq = maxSeq + 1
	}
	// Update the cache with the new state.
	s.rcache = &replayCache{
		manifest:             m.Clone(),
		snapshot:             current.Snapshot.Clone(),
		logSeqStart:          current.LogSeqStart,
		nextSeq:              current.NextSeq,
		activeWriterEpoch:    activeWriterEpoch,
		activeCompactorEpoch: activeCompactorEpoch,
		writerFenceEpoch:     tokenEpoch(current.WriterFence),
		compactorFenceEpoch:  tokenEpoch(current.CompactorFence),
	}
	s.mu.Unlock()

	return m, true
}

// fullReplay performs the original full manifest replay from snapshot + all log
// entries. It updates the replay cache on success.
func (s *Store) fullReplay(ctx context.Context, current *Current) (*Manifest, error) {
	var m *Manifest
	var err error

	if current != nil && current.Snapshot != nil {
		if err := validateManifestObjectRef(*current.Snapshot, manifestObjectKindSnapshot); err != nil {
			return nil, err
		}
		data, err := s.storage.ReadSnapshot(ctx, current.Snapshot.Path)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				return nil, fmt.Errorf("read snapshot %q: %w", current.Snapshot.Path, err)
			}
			return nil, err
		}
		if len(data) > 0 {
			if err := verifyManifestObjectRef(data, *current.Snapshot, manifestObjectKindSnapshot); err != nil {
				return nil, err
			}
			snap, err := DecodeSnapshot(data)
			if err != nil {
				return nil, err
			}
			if current.LogSeqStart > 0 && snap.LogSeq != current.LogSeqStart-1 {
				return nil, fmt.Errorf("%w: snapshot log_seq=%d want=%d path=%q",
					ErrInvalidManifest, snap.LogSeq, current.LogSeqStart-1, current.Snapshot.Path)
			}
			m = snap
		} else {
			return nil, fmt.Errorf("snapshot %q is empty", current.Snapshot.Path)
		}
	}
	if m == nil {
		m = &Manifest{Version: 2, NextEpoch: 1}
	}

	var entries []*ManifestLogEntry
	if current != nil {
		entries, err = s.entriesInRange(ctx, current, currentLogStart(current), currentNextSeq(current))
		if err != nil {
			return nil, err
		}
	}
	var maxSeq uint64
	var maxWriterFenceClaimEpoch uint64
	var maxCompactorFenceClaimEpoch uint64
	var maxWriterEntryEpoch uint64
	var maxCompactorEntryEpoch uint64
	for _, entry := range entries {
		if entry.Seq > maxSeq {
			maxSeq = entry.Seq
		}
		if entry.Op == LogOpFenceClaim {
			if entry.Role == FenceRoleWriter && entry.Epoch > maxWriterFenceClaimEpoch {
				maxWriterFenceClaimEpoch = entry.Epoch
			} else if entry.Role == FenceRoleCompactor && entry.Epoch > maxCompactorFenceClaimEpoch {
				maxCompactorFenceClaimEpoch = entry.Epoch
			}
		}
		if entry.Role == FenceRoleWriter && entry.Epoch > maxWriterEntryEpoch {
			maxWriterEntryEpoch = entry.Epoch
		} else if entry.Role == FenceRoleCompactor && entry.Epoch > maxCompactorEntryEpoch {
			maxCompactorEntryEpoch = entry.Epoch
		}
	}

	// track the active epochs per role. Seed from CURRENT only if:
	// 1. No fence-claim exists in the log window (truncated logs), OR
	// 2. The log contains entries at CURRENT's epoch but NO fence-claim for that epoch
	// (writer/compactor crashed after updating CURRENT but before writing fence-claim log)
	// Otherwise, we start from zero and let fence-claim logs advance epochs in order,
	// preserving entries that appeared before the fence-claim.
	var activeWriterEpoch uint64
	var activeCompactorEpoch uint64
	if current != nil {
		if current.WriterFence != nil &&
			(maxWriterFenceClaimEpoch == 0 ||
				(maxWriterEntryEpoch >= current.WriterFence.Epoch && maxWriterFenceClaimEpoch < current.WriterFence.Epoch)) {
			activeWriterEpoch = current.WriterFence.Epoch
		}
		if current.CompactorFence != nil &&
			(maxCompactorFenceClaimEpoch == 0 ||
				(maxCompactorEntryEpoch >= current.CompactorFence.Epoch && maxCompactorFenceClaimEpoch < current.CompactorFence.Epoch)) {
			activeCompactorEpoch = current.CompactorFence.Epoch
		}
	}

	for _, entry := range entries {
		// we will handle fence claim entries - they update the active epoch (monotonically)
		// We only increase epochs to prevent downgrades from stale fence-claim logs
		// that may appear after CURRENT was updated by a newer writer/compactor.
		if entry.Op == LogOpFenceClaim {
			if entry.Role == FenceRoleWriter && entry.Epoch > activeWriterEpoch {
				activeWriterEpoch = entry.Epoch
			} else if entry.Role == FenceRoleCompactor && entry.Epoch > activeCompactorEpoch {
				activeCompactorEpoch = entry.Epoch
			}
			// this fence claimed so we don't modify manifest state
			continue
		}

		// skip all the entries below fence.
		if entry.Role == FenceRoleWriter && entry.Epoch < activeWriterEpoch {
			continue
		}
		if entry.Role == FenceRoleCompactor && entry.Epoch < activeCompactorEpoch {
			continue
		}

		if err := validateReplayEntry(entry); err != nil {
			return nil, err
		}
		m = ApplyLogEntry(m, entry)
	}

	// THIS is IMP: never reuse the same epoch so Set NextEpoch
	maxEpoch := activeWriterEpoch
	if activeCompactorEpoch > maxEpoch {
		maxEpoch = activeCompactorEpoch
	}
	if maxEpoch >= m.NextEpoch {
		m.NextEpoch = maxEpoch + 1
	}

	if current != nil && current.NextEpoch > m.NextEpoch {
		m.NextEpoch = current.NextEpoch
	}
	if current != nil && current.NextSeq > 0 {
		m.LogSeq = current.NextSeq - 1
	} else if maxSeq > m.LogSeq {
		m.LogSeq = maxSeq
	}

	s.mu.Lock()
	if current != nil && current.NextSeq > 0 {
		s.nextSeq = current.NextSeq
	} else {
		s.nextSeq = maxSeq + 1
	}

	if current != nil {
		s.rcache = &replayCache{
			manifest:             m.Clone(),
			snapshot:             current.Snapshot.Clone(),
			logSeqStart:          current.LogSeqStart,
			nextSeq:              current.NextSeq,
			activeWriterEpoch:    activeWriterEpoch,
			activeCompactorEpoch: activeCompactorEpoch,
			writerFenceEpoch:     tokenEpoch(current.WriterFence),
			compactorFenceEpoch:  tokenEpoch(current.CompactorFence),
		}
	}
	s.mu.Unlock()

	return m, nil
}

func (s *Store) ListEntries(ctx context.Context) ([]uint64, error) {
	current, err := s.readCurrent(ctx)
	if err != nil {
		return nil, err
	}
	if current == nil || current.NextSeq <= current.ChangeFeedLogStart {
		return []uint64{}, nil
	}
	seqs := make([]uint64, 0, current.NextSeq-current.ChangeFeedLogStart)
	for seq := current.ChangeFeedLogStart; seq < current.NextSeq; seq++ {
		seqs = append(seqs, seq)
	}
	return seqs, nil
}

// EnableChangeFeed permanently enables change batches with one payload policy
// for subsequent writer commits. Existing history is excluded by advancing
// the retained floor to the current manifest head.
func (s *Store) EnableChangeFeed(ctx context.Context, payload ChangeFeedPayload) error {
	if !payload.Valid() {
		return fmt.Errorf("%w: %q", ErrInvalidChangeFeedPayload, payload)
	}
	s.commitMu.Lock()
	defer s.commitMu.Unlock()

	for attempt := 0; attempt < currentCASMaxRetries; attempt++ {
		current, etag, err := s.readCurrentWithETag(ctx)
		if err != nil {
			return err
		}
		if current == nil {
			current = &Current{NextEpoch: 1}
		}
		normalizeCurrent(current)
		if current.ChangeFeedEnabled {
			if !current.ChangeFeedPayload.Valid() {
				return fmt.Errorf("%w: change feed payload=%q", ErrInvalidManifest, current.ChangeFeedPayload)
			}
			if current.ChangeFeedPayload != payload {
				return fmt.Errorf("%w: configured=%q requested=%q",
					ErrChangeFeedPayloadMismatch, current.ChangeFeedPayload, payload)
			}
			return nil
		}

		updated := current.Clone()
		updated.ChangeFeedEnabled = true
		updated.ChangeFeedPayload = payload
		updated.ChangeFeedLogStart = updated.NextSeq
		if err := s.writeCurrentWithCAS(ctx, updated, etag); err != nil {
			if errors.Is(err, ErrPreconditionFailed) && attempt+1 < currentCASMaxRetries {
				if err := sleepBeforeCurrentCASRetry(ctx, attempt); err != nil {
					return err
				}
				continue
			}
			return err
		}
		return nil
	}
	return ErrFenceConflict
}

// ChangeFeedView is an immutable view of the manifest metadata required to
// locate retained change-feed entries.
type ChangeFeedView struct {
	current   *Current
	expiresAt time.Time
}

func (v *ChangeFeedView) Enabled() bool {
	return v != nil && v.current != nil && v.current.ChangeFeedEnabled
}

func (v *ChangeFeedView) Payload() ChangeFeedPayload {
	if v == nil || v.current == nil || !v.current.ChangeFeedEnabled {
		return ""
	}
	return v.current.ChangeFeedPayload
}

func (v *ChangeFeedView) RetainedFrom() uint64 {
	if v == nil || v.current == nil {
		return 0
	}
	return v.current.ChangeFeedLogStart
}

func (v *ChangeFeedView) Head() uint64 {
	if v == nil || v.current == nil {
		return 0
	}
	return v.current.NextSeq
}

// ExpiredAt reports whether this immutable view has reached the maximum
// lifetime published in the CURRENT object from which it was loaded. Callers
// must load a fresh view before starting more work once this returns true.
func (v *ChangeFeedView) ExpiredAt(now time.Time) bool {
	return v == nil || v.expiresAt.IsZero() || !now.Before(v.expiresAt)
}

// LoadChangeFeedView reads CURRENT once and returns an immutable change-feed
// view that can serve multiple bounded reads until CURRENT.MaxPinnedViewAge.
func (s *Store) LoadChangeFeedView(ctx context.Context) (*ChangeFeedView, error) {
	loadedAt := time.Now()
	current, err := s.readCurrent(ctx)
	if err != nil {
		return nil, err
	}
	if current != nil && current.ChangeFeedEnabled && !current.ChangeFeedPayload.Valid() {
		return nil, fmt.Errorf("%w: change feed payload=%q", ErrInvalidManifest, current.ChangeFeedPayload)
	}
	return &ChangeFeedView{
		current:   current,
		expiresAt: loadedAt.Add(current.PinnedViewAge()),
	}, nil
}

// ChangeFeedBounds returns whether the feed is enabled, the oldest retained
// manifest sequence, and the first uncommitted manifest sequence.
func (s *Store) ChangeFeedBounds(ctx context.Context) (enabled bool, retainedFrom, head uint64, err error) {
	view, err := s.LoadChangeFeedView(ctx)
	if err != nil {
		return false, 0, 0, err
	}
	return view.Enabled(), view.RetainedFrom(), view.Head(), nil
}

// ReadChangeEntries returns a bounded contiguous manifest range. When
// fromOldest is true, start is ignored and reading begins at the retained
// change-feed floor.
func (s *Store) ReadChangeEntries(
	ctx context.Context,
	start uint64,
	fromOldest bool,
	limit int,
) (entries []*ManifestLogEntry, enabled bool, retainedFrom, head uint64, err error) {
	view, err := s.LoadChangeFeedView(ctx)
	if err != nil {
		return nil, false, 0, 0, err
	}
	entries, err = s.ReadChangeEntriesFromView(ctx, view, start, fromOldest, limit)
	return entries, view.Enabled(), view.RetainedFrom(), view.Head(), err
}

// ReadChangeEntriesFromView returns a bounded contiguous manifest range from
// an immutable view without reading CURRENT again.
func (s *Store) ReadChangeEntriesFromView(
	ctx context.Context,
	view *ChangeFeedView,
	start uint64,
	fromOldest bool,
	limit int,
) ([]*ManifestLogEntry, error) {
	if limit <= 0 {
		limit = 128
	}
	if limit > 1024 {
		limit = 1024
	}

	if view == nil || view.current == nil {
		return nil, nil
	}
	current := view.current
	retainedFrom := view.RetainedFrom()
	head := view.Head()
	if fromOldest {
		start = retainedFrom
	}
	if start < retainedFrom {
		return nil, fmt.Errorf(
			"%w: start=%d retained_from=%d", ErrChangeFeedHistory, start, retainedFrom)
	}
	if start > head {
		return nil, fmt.Errorf(
			"%w: start=%d head=%d", ErrChangeFeedPosition, start, head)
	}
	if start == head {
		return nil, nil
	}

	end := start + uint64(limit)
	if end < start || end > head {
		end = head
	}
	entries, err := s.entriesInRange(ctx, current, start, end)
	if err != nil {
		return nil, err
	}
	return entries, nil
}

func (s *Store) ReadEntry(ctx context.Context, seq uint64) (*ManifestLogEntry, error) {
	current, err := s.readCurrent(ctx)
	if err != nil {
		return nil, err
	}
	if current == nil || seq < current.ChangeFeedLogStart || seq >= current.NextSeq {
		return nil, fmt.Errorf("manifest entry seq=%d not retained", seq)
	}
	entries, err := s.entriesInRange(ctx, current, seq, seq+1)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, fmt.Errorf("manifest entry seq=%d not found", seq)
	}
	return entries[0], nil
}

func (s *Store) writeSnapshotObject(ctx context.Context, m *Manifest) (ObjectRef, error) {
	data, err := EncodeSnapshot(m)
	if err != nil {
		return ObjectRef{}, err
	}
	createdAt := time.Now().UTC()
	path, err := s.storage.WriteSnapshot(ctx, ksuid.New().String(), data)
	if err != nil {
		return ObjectRef{}, err
	}
	return newManifestObjectRef(path, data, manifestObjectKindSnapshot, createdAt)
}

// PrepareCheckpoint writes an immutable snapshot candidate and returns the
// command needed to publish it. It does not mutate CURRENT.
func (s *Store) PrepareCheckpoint(ctx context.Context) (CheckpointCommand, error) {
	current, err := s.readCurrentData(ctx)
	if err != nil {
		return CheckpointCommand{}, err
	}
	if current == nil {
		return CheckpointCommand{}, ErrInvalidManifest
	}

	state, err := s.fullReplay(ctx, current)
	if err != nil {
		return CheckpointCommand{}, err
	}
	if err := state.ValidateLevels(); err != nil {
		return CheckpointCommand{}, fmt.Errorf("%w: %v", ErrInvalidManifest, err)
	}
	state.WriterFence = current.WriterFence.Clone()
	state.CompactorFence = current.CompactorFence.Clone()
	if current.NextSeq > 0 {
		state.LogSeq = current.NextSeq - 1
	}

	snapshot, err := s.writeSnapshotObject(ctx, state)
	if err != nil {
		return CheckpointCommand{}, err
	}
	return CheckpointCommand{
		Snapshot:                 snapshot,
		BaseSnapshot:             current.Snapshot.Clone(),
		BaseLogSeqStart:          current.LogSeqStart,
		SnapshotNextSeq:          current.NextSeq,
		FoldedReplayPages:        current.StateReplayPages,
		FoldedReplayBytes:        current.StateReplayBytes,
		FoldedReplayMaxPageLevel: maxManifestPageLevel(current.IndexFrontier),
	}, nil
}

func maxManifestPageLevel(refs []PageRef) uint8 {
	var maximum uint8
	for i := range refs {
		if refs[i].Level > maximum {
			maximum = refs[i].Level
		}
	}
	return maximum
}

func filterEntriesAtOrAfter(entries []ManifestLogEntry, floor uint64) []ManifestLogEntry {
	if floor == 0 || len(entries) == 0 {
		return entries
	}
	kept := entries[:0]
	for _, entry := range entries {
		if entry.Seq >= floor {
			kept = append(kept, entry)
		}
	}
	return kept
}

func filterPageRefsAtOrAfter(refs []PageRef, floor uint64) []PageRef {
	if floor == 0 || len(refs) == 0 {
		return refs
	}
	kept := refs[:0]
	for _, ref := range refs {
		if ref.SeqHi >= floor {
			kept = append(kept, ref)
		}
	}
	return kept
}

func currentLogStart(current *Current) uint64 {
	if current == nil {
		return 0
	}
	return current.LogSeqStart
}

func retainedEntryFloor(current *Current) uint64 {
	if current == nil {
		return 0
	}
	floor := current.LogSeqStart
	if current.ChangeFeedLogStart < floor {
		floor = current.ChangeFeedLogStart
	}
	return floor
}

func currentNextSeq(current *Current) uint64 {
	if current == nil {
		return 0
	}
	return current.NextSeq
}

func (s *Store) entriesInRange(ctx context.Context, current *Current, start, end uint64) ([]*ManifestLogEntry, error) {
	if current == nil || end <= start {
		return nil, nil
	}
	var entries []*ManifestLogEntry
	for _, ref := range current.IndexFrontier {
		if ref.SeqHi < start || ref.SeqLo >= end {
			continue
		}
		pageEntries, err := s.entriesFromPageRef(ctx, ref, start, end)
		if err != nil {
			return nil, err
		}
		entries = append(entries, pageEntries...)
	}
	for i := range current.ActiveEntries {
		entry := current.ActiveEntries[i]
		if entry.Seq >= start && entry.Seq < end {
			e := entry
			entries = append(entries, &e)
		}
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Seq < entries[j].Seq
	})
	if err := validateEntryCoverage(entries, start, end); err != nil {
		return nil, err
	}
	return entries, nil
}

func validateEntryCoverage(entries []*ManifestLogEntry, start, end uint64) error {
	if end <= start {
		return nil
	}
	expectedCount := end - start
	if uint64(len(entries)) != expectedCount {
		return fmt.Errorf("manifest entry range incomplete: start=%d end=%d got=%d want=%d", start, end, len(entries), expectedCount)
	}
	expected := start
	for _, entry := range entries {
		if entry == nil {
			return fmt.Errorf("manifest entry range contains nil entry at seq=%d", expected)
		}
		if entry.Seq != expected {
			return fmt.Errorf("manifest entry sequence mismatch: got=%d want=%d range=[%d,%d)", entry.Seq, expected, start, end)
		}
		expected++
	}
	return nil
}

func (s *Store) entriesFromPageRef(ctx context.Context, ref PageRef, start, end uint64) ([]*ManifestLogEntry, error) {
	pages, ok := s.storage.(PageStorage)
	if !ok {
		return nil, errors.New("manifest page storage unsupported")
	}
	if err := validatePageRef(ref); err != nil {
		return nil, err
	}
	data, err := pages.ReadPage(ctx, ref.Path)
	if err != nil {
		return nil, err
	}
	if err := verifyManifestObjectRef(data, ref.ObjectRef, manifestObjectKindPage); err != nil {
		return nil, err
	}
	page, err := DecodeCommitPage(data)
	if err != nil {
		return nil, err
	}
	if page.Level != ref.Level || page.SeqLo != ref.SeqLo || page.SeqHi != ref.SeqHi {
		return nil, fmt.Errorf("manifest page ref mismatch path=%q", ref.Path)
	}
	if err := validateCommitPage(page, ref.Path); err != nil {
		return nil, err
	}
	if page.SeqHi < start || page.SeqLo >= end {
		return nil, nil
	}
	if page.Level == 0 {
		entries := make([]*ManifestLogEntry, 0, len(page.Entries))
		for i := range page.Entries {
			entry := page.Entries[i]
			if entry.Seq >= start && entry.Seq < end {
				e := entry
				entries = append(entries, &e)
			}
		}
		return entries, nil
	}
	var entries []*ManifestLogEntry
	for _, child := range page.Children {
		if child.SeqHi < start || child.SeqLo >= end {
			continue
		}
		childEntries, err := s.entriesFromPageRef(ctx, child, start, end)
		if err != nil {
			return nil, err
		}
		entries = append(entries, childEntries...)
	}
	return entries, nil
}

func validateCommitPage(page *CommitPage, path string) error {
	if page == nil {
		return fmt.Errorf("manifest page is nil path=%q", path)
	}
	if page.LayoutVersion != LayoutVersion {
		return fmt.Errorf("manifest page layout version mismatch path=%q version=%d", path, page.LayoutVersion)
	}
	if page.CreatedAt.IsZero() {
		return fmt.Errorf("manifest page creation time is missing path=%q", path)
	}
	if page.SeqHi < page.SeqLo {
		return fmt.Errorf("manifest page invalid range path=%q seq_lo=%d seq_hi=%d", path, page.SeqLo, page.SeqHi)
	}
	switch page.Level {
	case 0:
		if page.PageType != CommitPageTypeLeaf {
			return fmt.Errorf("manifest leaf page type mismatch path=%q type=%q", path, page.PageType)
		}
		if page.Count != uint32(len(page.Entries)) {
			return fmt.Errorf("manifest leaf page count mismatch path=%q count=%d entries=%d", path, page.Count, len(page.Entries))
		}
		if len(page.Children) != 0 {
			return fmt.Errorf("manifest leaf page has children path=%q", path)
		}
		if len(page.Entries) == 0 {
			return fmt.Errorf("manifest leaf page empty path=%q", path)
		}
		entries := make([]*ManifestLogEntry, 0, len(page.Entries))
		for i := range page.Entries {
			entry := page.Entries[i]
			e := entry
			entries = append(entries, &e)
		}
		if page.Entries[0].Seq != page.SeqLo || page.Entries[len(page.Entries)-1].Seq != page.SeqHi {
			return fmt.Errorf("manifest leaf page range mismatch path=%q", path)
		}
		return validateEntryCoverage(entries, page.SeqLo, page.SeqHi+1)
	default:
		if page.PageType != CommitPageTypeIndex {
			return fmt.Errorf("manifest index page type mismatch path=%q type=%q", path, page.PageType)
		}
		if page.Count != uint32(len(page.Children)) {
			return fmt.Errorf("manifest index page count mismatch path=%q count=%d children=%d", path, page.Count, len(page.Children))
		}
		if len(page.Entries) != 0 {
			return fmt.Errorf("manifest index page has entries path=%q", path)
		}
		if len(page.Children) == 0 {
			return fmt.Errorf("manifest index page empty path=%q", path)
		}
		sort.Slice(page.Children, func(i, j int) bool {
			return page.Children[i].SeqLo < page.Children[j].SeqLo
		})
		if page.Children[0].SeqLo != page.SeqLo || page.Children[len(page.Children)-1].SeqHi != page.SeqHi {
			return fmt.Errorf("manifest index page range mismatch path=%q", path)
		}
		expected := page.SeqLo
		for _, child := range page.Children {
			if err := validatePageRef(child); err != nil {
				return err
			}
			if child.Level+1 != page.Level {
				return fmt.Errorf("manifest index child level mismatch path=%q child_level=%d page_level=%d", path, child.Level, page.Level)
			}
			if child.SeqLo != expected {
				return fmt.Errorf("manifest index child sequence gap path=%q got=%d want=%d", path, child.SeqLo, expected)
			}
			if child.SeqHi < child.SeqLo {
				return fmt.Errorf("manifest index child invalid range path=%q seq_lo=%d seq_hi=%d", path, child.SeqLo, child.SeqHi)
			}
			expected = child.SeqHi + 1
		}
		if expected != page.SeqHi+1 {
			return fmt.Errorf("manifest index page sequence gap path=%q got_end=%d want_end=%d", path, expected, page.SeqHi+1)
		}
		return nil
	}
}

func (s *Store) readCurrent(ctx context.Context) (*Current, error) {
	return s.readCurrentData(ctx)
}

// ReadCurrentData reads and decodes CURRENT using the most direct storage path available.
func (s *Store) ReadCurrentData(ctx context.Context) (*Current, error) {
	return s.readCurrentData(ctx)
}

func (s *Store) readCurrentData(ctx context.Context) (*Current, error) {
	data, _, err := s.storage.ReadCurrent(ctx)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			if s.hasObservedCurrent() {
				return nil, ErrCurrentUnavailable
			}
			return nil, nil
		}
		return nil, err
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("%w: CURRENT object is empty", ErrCurrentUnavailable)
	}
	current, err := DecodeCurrent(data)
	if err != nil {
		return nil, err
	}
	normalizeCurrent(current)
	s.mu.Lock()
	s.current = current.Clone()
	s.currentObserved = true
	s.mu.Unlock()
	return current, nil
}

func (s *Store) readCurrentWithETag(ctx context.Context) (*Current, string, error) {
	data, etag, err := s.storage.ReadCurrent(ctx)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			if s.hasObservedCurrent() {
				return nil, "", ErrCurrentUnavailable
			}
			return nil, "", nil
		}
		return nil, "", err
	}
	if len(data) == 0 {
		return nil, "", fmt.Errorf("%w: CURRENT object is empty", ErrCurrentUnavailable)
	}
	current, err := DecodeCurrent(data)
	if err != nil {
		return nil, "", err
	}
	normalizeCurrent(current)
	s.mu.Lock()
	s.current = current.Clone()
	s.commitCurrent = current.Clone()
	s.currentETag = etag
	s.currentObserved = true
	s.mu.Unlock()
	return current, etag, nil
}

func (s *Store) clearCurrentCache() {
	s.mu.Lock()
	s.current = nil
	s.commitCurrent = nil
	s.currentETag = ""
	s.mu.Unlock()
}

func (s *Store) hasObservedCurrent() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.currentObserved
}

func (s *Store) writeCurrentWithCAS(ctx context.Context, current *Current, etag string) error {
	data, err := s.encodeCurrentForCAS(current)
	if err != nil {
		return err
	}
	return s.writeEncodedCurrentWithCAS(ctx, current, data, etag)
}

// encodeCurrentForCAS completes every local operation that can fail before a
// request is issued. Writer reconciliation records an uncertain attempt only
// after this succeeds, so a local encoding or size error cannot be mistaken
// for an ambiguous provider response.
func (s *Store) encodeCurrentForCAS(current *Current) ([]byte, error) {
	normalizeCurrent(current)
	data, err := EncodeCurrent(current)
	if err != nil {
		return nil, err
	}
	if limit := s.currentByteLimit(); len(data) > limit {
		return nil, fmt.Errorf("%w: size=%d limit=%d", ErrCurrentTooLarge, len(data), limit)
	}
	return data, nil
}

func (s *Store) writeEncodedCurrentWithCAS(
	ctx context.Context,
	current *Current,
	data []byte,
	etag string,
) error {
	newETag, err := s.storage.WriteCurrentCAS(ctx, data, etag)
	if err != nil {
		s.clearCurrentCache()
		return err
	}
	s.mu.Lock()
	s.current = current.Clone()
	s.commitCurrent = current.Clone()
	s.currentETag = newETag
	s.currentObserved = true
	s.mu.Unlock()
	return nil
}

// AdvanceChangeFeedLogStart advances CURRENT.change_feed_log_start and prunes
// retained manifest entry refs below the new floor. The floor is clamped to
// [current.change_feed_log_start, current.next_seq].
func (s *Store) AdvanceChangeFeedLogStart(ctx context.Context, floor uint64, token *FenceToken) (*Current, error) {
	s.commitMu.Lock()
	defer s.commitMu.Unlock()

	for attempt := 0; attempt < currentCASMaxRetries; attempt++ {
		current, etag, err := s.readCurrentWithETag(ctx)
		if err != nil {
			return nil, err
		}
		if current == nil {
			return nil, nil
		}
		if err := checkFenceToken(token, current.CompactorFence); err != nil {
			return nil, err
		}

		updated := current.Clone()
		if updated == nil {
			return nil, nil
		}
		normalizeCurrent(updated)
		if floor < updated.ChangeFeedLogStart {
			floor = updated.ChangeFeedLogStart
		}
		if floor > updated.NextSeq {
			floor = updated.NextSeq
		}
		if floor == updated.ChangeFeedLogStart {
			return updated, nil
		}

		updated.ChangeFeedLogStart = floor
		retainedFrom := retainedEntryFloor(updated)
		updated.ActiveEntries = filterEntriesAtOrAfter(updated.ActiveEntries, retainedFrom)
		updated.IndexFrontier = filterPageRefsAtOrAfter(updated.IndexFrontier, retainedFrom)

		if err := s.writeCurrentWithCAS(ctx, updated, etag); err != nil {
			if errors.Is(err, ErrPreconditionFailed) {
				if attempt+1 < currentCASMaxRetries {
					if err := sleepBeforeCurrentCASRetry(ctx, attempt); err != nil {
						return nil, err
					}
				}
				continue
			}
			return nil, err
		}
		return updated, nil
	}

	return nil, ErrFenceConflict
}

func checkFenceToken(local, remote *FenceToken) error {
	if local == nil || remote == nil {
		return ErrFenced
	}
	if local.Epoch != remote.Epoch || local.Owner != remote.Owner {
		return ErrFenced
	}
	return nil
}

func (s *Store) checkFenceWithCurrent(role FenceRole, current *Current) error {
	if !s.observeFenceWithCurrent(role, current) {
		return ErrFenced
	}
	return nil
}

// observeFenceWithCurrent records fence loss as terminal process-local state.
// In particular, append reconciliation is allowed to return success for an
// older commit after a successor takes ownership, but later operations through
// this Store must then fail before attempting another publication.
func (s *Store) observeFenceWithCurrent(role FenceRole, current *Current) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	var localFence, remoteFence *FenceToken
	switch role {
	case FenceRoleWriter:
		localFence = s.writerFence
		if current != nil {
			remoteFence = current.WriterFence
		}
	case FenceRoleCompactor:
		localFence = s.compactorFence
		if current != nil {
			remoteFence = current.CompactorFence
		}
	default:
		return false
	}
	if checkFenceToken(localFence, remoteFence) == nil {
		return true
	}
	switch role {
	case FenceRoleWriter:
		s.writerFence = nil
	case FenceRoleCompactor:
		s.compactorFence = nil
	}
	return false
}

func nextEpochFromEntry(current uint64, entry *ManifestLogEntry) uint64 {
	next := current
	if next == 0 {
		next = 1
	}
	if entry == nil {
		return next
	}

	switch entry.Op {
	case LogOpAddSSTable:
		if entry.SSTable != nil && entry.SSTable.Epoch >= next {
			next = entry.SSTable.Epoch + 1
		}
	case LogOpCompaction:
		if entry.Compaction != nil {
			for _, sst := range entry.Compaction.AddSSTables {
				if sst.Epoch >= next {
					next = sst.Epoch + 1
				}
			}
		}
	case LogOpCheckpoint:
		if entry.Checkpoint != nil && entry.Checkpoint.NextEpoch > next {
			next = entry.Checkpoint.NextEpoch
		}
	case LogOpFenceClaim:
		if entry.Epoch >= next {
			next = entry.Epoch + 1
		}
	}

	return next
}

func tokenEpoch(token *FenceToken) uint64 {
	if token == nil {
		return 0
	}
	return token.Epoch
}
