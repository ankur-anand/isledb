package manifest

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/segmentio/ksuid"
)

var (
	ErrFenced        = errors.New("fenced: epoch superseded by newer owner")
	ErrFenceConflict = errors.New("fence conflict: concurrent claim detected")
)

const (
	defaultActiveEntryLimit = 1024
	defaultPageFanout       = 1024
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
	snapshot    string
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
	mu      sync.Mutex
	nextSeq uint64

	current     *Current
	currentETag string

	writerFence    *FenceToken
	compactorFence *FenceToken

	rcache *replayCache

	activeEntryLimit int
	pageFanout       int
}

func NewStore(store *blobstore.Store) *Store {
	return NewStoreWithStorage(NewBlobStoreBackend(store))
}

func NewStoreWithStorage(storage Storage) *Store {
	return &Store{
		storage:          storage,
		activeEntryLimit: defaultActiveEntryLimit,
		pageFanout:       defaultPageFanout,
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
	token, err := s.claimFence(ctx, FenceRoleWriter, ownerID)
	if err != nil {
		return nil, err
	}

	if err := s.writeFenceClaimEntry(ctx, FenceRoleWriter, token); err != nil {
		return nil, fmt.Errorf("write fence claim entry: %w", err)
	}

	return token, nil
}

func (s *Store) ClaimCompactor(ctx context.Context, ownerID string) (*FenceToken, error) {
	token, err := s.claimFence(ctx, FenceRoleCompactor, ownerID)
	if err != nil {
		return nil, err
	}

	if err := s.writeFenceClaimEntry(ctx, FenceRoleCompactor, token); err != nil {
		return nil, fmt.Errorf("write fence claim entry: %w", err)
	}

	return token, nil
}

func (s *Store) claimFence(ctx context.Context, role FenceRole, ownerID string) (*FenceToken, error) {
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

	current, _, err := s.readCurrentWithETag(ctx)
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

	if remoteFence == nil {
		return ErrFenced
	}

	if remoteFence.Epoch > localFence.Epoch {
		return ErrFenced
	}

	return nil
}

func (s *Store) WriterEpoch() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.writerFence == nil {
		return 0
	}
	return s.writerFence.Epoch
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
	const maxRetries = 3

	for attempt := 0; attempt < maxRetries; attempt++ {
		var current *Current
		var etag string
		var err error
		if attempt == 0 {
			s.mu.Lock()
			current = s.current.Clone()
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
		if updated.ChangeFeedLogStart == 0 {
			updated.ChangeFeedLogStart = updated.LogSeqStart
		}
		updated.ActiveEntries = append(updated.ActiveEntries, nextEntry)
		updated.NextSeq = nextEntry.Seq + 1
		updated.NextEpoch = nextEpochFromEntry(updated.NextEpoch, &nextEntry)

		if err := s.writeCurrentWithCAS(ctx, updated, etag); err != nil {
			if errors.Is(err, ErrPreconditionFailed) {
				continue
			}
			return err
		}
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
	entry := &ManifestLogEntry{
		Op:          LogOpAddSSTable,
		SSTable:     &sst,
		ChangeBatch: changeBatch,
	}
	if err := s.AppendWithWriterFence(ctx, entry); err != nil {
		return nil, err
	}
	return entry, nil
}

func (s *Store) AppendRemoveSSTablesWithFence(ctx context.Context, sstableIDs []string) (*ManifestLogEntry, error) {
	entry := &ManifestLogEntry{
		Op:               LogOpRemoveSSTable,
		RemoveSSTableIDs: sstableIDs,
	}
	if err := s.AppendWithCompactorFence(ctx, entry); err != nil {
		return nil, err
	}
	return entry, nil
}

func (s *Store) AppendCompactionWithFence(ctx context.Context, payload CompactionLogPayload) (*ManifestLogEntry, error) {
	entry := &ManifestLogEntry{
		Op:         LogOpCompaction,
		Compaction: &payload,
	}
	if err := s.AppendWithCompactorFence(ctx, entry); err != nil {
		return nil, err
	}
	return entry, nil
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
		current.IndexFrontier = append(keep, indexRef)
		ref = indexRef
	}
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
	sum := sha256.Sum256(data)
	checksum := fmt.Sprintf("sha256:%x", sum[:])
	id := fmt.Sprintf("%020d-%020d-%s", page.SeqLo, page.SeqHi, ksuid.New().String())
	path, err := pages.WritePage(ctx, page.Level, id, data)
	if err != nil {
		return PageRef{}, err
	}
	return PageRef{
		Level:     page.Level,
		SeqLo:     page.SeqLo,
		SeqHi:     page.SeqHi,
		Path:      path,
		Count:     page.Count,
		Checksum:  checksum,
		CreatedAt: page.CreatedAt,
	}, nil
}

func (s *Store) Replay(ctx context.Context) (*Manifest, error) {
	current, err := s.readCurrent(ctx)
	if err != nil {
		return nil, err
	}

	// Attempt incremental replay: if the snapshot and log window base haven't
	// changed, we only need to read the new delta entries.
	if m, ok := s.tryIncrementalReplay(ctx, current); ok {
		return m, nil
	}

	return s.fullReplay(ctx, current)
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
	if current.Snapshot != rc.snapshot || current.LogSeqStart != rc.logSeqStart {
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
		snapshot:             current.Snapshot,
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

	if current != nil && current.Snapshot != "" {
		data, err := s.storage.ReadSnapshot(ctx, current.Snapshot)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				return nil, fmt.Errorf("read snapshot %q: %w", current.Snapshot, err)
			}
			return nil, err
		}
		if len(data) > 0 {
			snap, err := DecodeSnapshot(data)
			if err != nil {
				return nil, err
			}
			m = snap
		} else {
			return nil, fmt.Errorf("snapshot %q is empty", current.Snapshot)
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
			snapshot:             current.Snapshot,
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

func (s *Store) WriteSnapshot(ctx context.Context, m *Manifest) (string, error) {
	if m == nil {
		return "", errors.New("nil manifest")
	}

	current, etag, err := s.readCurrentWithETag(ctx)
	if err != nil {
		return "", err
	}

	s.mu.Lock()
	localNextSeq := s.nextSeq
	s.mu.Unlock()

	nextSeq := localNextSeq
	if current != nil && current.NextSeq > nextSeq {
		nextSeq = current.NextSeq
	}

	manifestNextSeq := m.LogSeq + 1
	if m.LogSeq == 0 &&
		len(m.L0SSTs) == 0 &&
		len(m.SortedRuns) == 0 &&
		m.NextSortedRunID == 0 &&
		m.WriterFence == nil &&
		m.CompactorFence == nil &&
		m.NextEpoch <= 1 {
		manifestNextSeq = 0
	}
	if manifestNextSeq > nextSeq {
		nextSeq = manifestNextSeq
	}

	snap := m.Clone()
	if nextSeq > 0 {
		snap.LogSeq = nextSeq - 1
	}

	data, err := EncodeSnapshot(snap)
	if err != nil {
		return "", err
	}

	id := ksuid.New().String()
	path, err := s.storage.WriteSnapshot(ctx, id, data)
	if err != nil {
		return "", err
	}

	if current == nil {
		current = &Current{NextEpoch: m.NextEpoch}
	}
	normalizeCurrent(current)
	oldLogSeqStart := current.LogSeqStart
	if current.ChangeFeedLogStart == oldLogSeqStart {
		current.ChangeFeedLogStart = nextSeq
	}
	current.Snapshot = path
	current.LogSeqStart = nextSeq
	current.ActiveEntries = filterEntriesAtOrAfter(current.ActiveEntries, current.ChangeFeedLogStart)
	current.IndexFrontier = filterPageRefsAtOrAfter(current.IndexFrontier, current.ChangeFeedLogStart)
	if current.NextEpoch < m.NextEpoch {
		current.NextEpoch = m.NextEpoch
	}
	current.NextSeq = nextSeq

	if err := s.writeCurrentWithCAS(ctx, current, etag); err != nil {
		return "", err
	}
	s.mu.Lock()
	if s.nextSeq < nextSeq {
		s.nextSeq = nextSeq
	}
	s.mu.Unlock()
	return path, nil
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
	data, err := pages.ReadPage(ctx, ref.Path)
	if err != nil {
		return nil, err
	}
	if ref.Checksum != "" {
		sum := sha256.Sum256(data)
		checksum := fmt.Sprintf("sha256:%x", sum[:])
		if checksum != ref.Checksum {
			return nil, fmt.Errorf("manifest page checksum mismatch path=%q", ref.Path)
		}
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
	current, _, err := s.readCurrentWithETag(ctx)
	return current, err
}

// ReadCurrentData reads and decodes CURRENT using the most direct storage path available.
func (s *Store) ReadCurrentData(ctx context.Context) (*Current, error) {
	return s.readCurrentData(ctx)
}

func (s *Store) readCurrentData(ctx context.Context) (*Current, error) {
	data, _, err := s.storage.ReadCurrent(ctx)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			s.clearCurrentCache()
			return nil, nil
		}
		return nil, err
	}
	if len(data) == 0 {
		s.clearCurrentCache()
		return nil, nil
	}
	current, err := DecodeCurrent(data)
	if err != nil {
		return nil, err
	}
	normalizeCurrent(current)
	return current, nil
}

func (s *Store) readCurrentWithETag(ctx context.Context) (*Current, string, error) {
	data, etag, err := s.storage.ReadCurrent(ctx)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			s.clearCurrentCache()
			return nil, "", nil
		}
		return nil, "", err
	}
	if len(data) == 0 {
		s.clearCurrentCache()
		return nil, etag, nil
	}
	current, err := DecodeCurrent(data)
	if err != nil {
		return nil, "", err
	}
	normalizeCurrent(current)
	s.mu.Lock()
	s.current = current.Clone()
	s.currentETag = etag
	s.mu.Unlock()
	return current, etag, nil
}

func (s *Store) clearCurrentCache() {
	s.mu.Lock()
	s.current = nil
	s.currentETag = ""
	s.mu.Unlock()
}

func (s *Store) writeCurrentWithCAS(ctx context.Context, current *Current, etag string) error {
	normalizeCurrent(current)
	data, err := EncodeCurrent(current)
	if err != nil {
		return err
	}
	newETag, err := s.storage.WriteCurrentCAS(ctx, data, etag)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.current = current.Clone()
	s.currentETag = newETag
	s.mu.Unlock()
	return nil
}

// AdvanceChangeFeedLogStart advances CURRENT.change_feed_log_start and prunes
// retained manifest entry refs below the new floor. The floor is clamped to
// [current.change_feed_log_start, current.next_seq].
func (s *Store) AdvanceChangeFeedLogStart(ctx context.Context, floor uint64) (*Current, error) {
	const maxRetries = 3

	for attempt := 0; attempt < maxRetries; attempt++ {
		current, etag, err := s.readCurrentWithETag(ctx)
		if err != nil {
			return nil, err
		}
		if current == nil {
			return nil, nil
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
		updated.ActiveEntries = filterEntriesAtOrAfter(updated.ActiveEntries, floor)
		updated.IndexFrontier = filterPageRefsAtOrAfter(updated.IndexFrontier, floor)

		if err := s.writeCurrentWithCAS(ctx, updated, etag); err != nil {
			if errors.Is(err, ErrPreconditionFailed) {
				continue
			}
			return nil, err
		}
		return updated, nil
	}

	return nil, ErrFenceConflict
}

func (s *Store) checkFenceWithCurrent(role FenceRole, current *Current) error {
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
	if current == nil {
		return ErrFenced
	}

	var remoteFence *FenceToken
	switch role {
	case FenceRoleWriter:
		remoteFence = current.WriterFence
	case FenceRoleCompactor:
		remoteFence = current.CompactorFence
	}

	if remoteFence == nil {
		return ErrFenced
	}
	if remoteFence.Epoch > localFence.Epoch {
		return ErrFenced
	}
	return nil
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
	}

	return next
}

func tokenEpoch(token *FenceToken) uint64 {
	if token == nil {
		return 0
	}
	return token.Epoch
}
