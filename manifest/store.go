package manifest

import (
	"context"
	"encoding/json"
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

type FenceRole int

const (
	FenceRoleWriter FenceRole = iota
	FenceRoleCompactor
)

type Store struct {
	storage Storage
	mu      sync.Mutex
	nextSeq uint64

	current     *Current
	currentETag string

	writerFence    *FenceToken
	compactorFence *FenceToken
}

func NewStore(store *blobstore.Store) *Store {
	return NewStoreWithStorage(NewBlobStoreBackend(store))
}

func NewStoreWithStorage(storage Storage) *Store {
	return &Store{storage: storage}
}

func (s *Store) Storage() Storage {
	return s.storage
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

	switch role {
	case FenceRoleWriter:
		s.mu.Lock()
		if s.writerFence != nil {
			entry.Role = FenceRoleWriter
			entry.Epoch = s.writerFence.Epoch
		}
		s.mu.Unlock()
	case FenceRoleCompactor:
		s.mu.Lock()
		if s.compactorFence != nil {
			entry.Role = FenceRoleCompactor
			entry.Epoch = s.compactorFence.Epoch
		}
		s.mu.Unlock()
	}

	for attempt := 0; attempt < maxRetries; attempt++ {
		s.mu.Lock()
		entry.Seq = s.nextSeq
		s.mu.Unlock()

		if entry.ID.IsNil() {
			entry.ID = ksuid.New()
		}
		if entry.Timestamp.IsZero() {
			entry.Timestamp = time.Now().UTC()
		}

		data, err := EncodeLogEntry(entry)
		if err != nil {
			return fmt.Errorf("marshal log entry: %w", err)
		}

		logName := formatLogSeq(entry.Seq)
		_, err = s.storage.WriteLog(ctx, logName, data)
		if err != nil {
			if errors.Is(err, ErrPreconditionFailed) {
				// IMP: Only advance if Sequence collision has happened this prevent
				// the GAP.
				s.mu.Lock()
				if s.nextSeq <= entry.Seq {
					s.nextSeq = entry.Seq + 1
				}
				s.mu.Unlock()

				// we have a seq collision,
				// so 1. either compactor if we refactor later to run on separate process wrote its entry
				// or some other new process picked up this role.
				// so we will check if we still have our role with the fence.
				if role >= 0 {
					if fenceErr := s.checkFence(ctx, role); fenceErr != nil {
						return fenceErr // We've been fenced out
					}
					// still own fence, retry with next seq (same epoch)
					continue
				}
				return ErrFenceConflict
			}
			return fmt.Errorf("write log entry: %w", err)
		}

		// if written then only we advance.
		s.mu.Lock()
		if s.nextSeq <= entry.Seq {
			s.nextSeq = entry.Seq + 1
		}
		s.mu.Unlock()

		return s.appendCurrent(ctx, entry)
	}

	return ErrFenceConflict
}

func (s *Store) AppendAddSSTableWithFence(ctx context.Context, sst SSTMeta) (*ManifestLogEntry, error) {
	entry := &ManifestLogEntry{
		Op:      LogOpAddSSTable,
		SSTable: &sst,
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

func (s *Store) Replay(ctx context.Context) (*Manifest, error) {
	current, err := s.readCurrent(ctx)
	if err != nil {
		return nil, err
	}

	var m *Manifest
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

	logs := currentLogs(current, s.logPath)
	if logs == nil {
		logs, err = s.List(ctx)
		if err != nil {
			return nil, err
		}
	}

	entries := make([]*ManifestLogEntry, 0, len(logs))
	var maxSeq uint64
	var maxWriterFenceClaimEpoch uint64
	var maxCompactorFenceClaimEpoch uint64
	var maxWriterEntryEpoch uint64
	var maxCompactorEntryEpoch uint64
	for _, path := range logs {
		entry, err := s.Read(ctx, path)
		if err != nil {
			return nil, err
		}
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
		entries = append(entries, entry)
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
	s.mu.Unlock()

	return m, nil
}

func (s *Store) List(ctx context.Context) ([]string, error) {
	objects, err := s.storage.ListLogs(ctx)
	if err != nil {
		return nil, fmt.Errorf("list manifest logs: %w", err)
	}

	sort.Strings(objects)
	return objects, nil
}

func (s *Store) Read(ctx context.Context, path string) (*ManifestLogEntry, error) {
	data, err := s.storage.ReadLog(ctx, path)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, fmt.Errorf("log entry %s not found", path)
		}
		return nil, fmt.Errorf("read log entry: %w", err)
	}
	return DecodeLogEntry(data)
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

	// m.LogSeq is the highest applied sequence; next sequence should be LogSeq+1.
	manifestNextSeq := m.LogSeq + 1
	// we will preserve zero for a truly empty manifest state.
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
	current.Snapshot = path
	current.LogSeqStart = nextSeq
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

func currentLogs(current *Current, pathFn func(seq uint64) string) []string {
	if current == nil {
		return nil
	}
	if current.NextSeq <= current.LogSeqStart {
		return []string{}
	}
	return current.LogPaths(pathFn)
}

func formatLogSeq(seq uint64) string {
	return fmt.Sprintf("%020d", seq)
}

func (s *Store) readCurrent(ctx context.Context) (*Current, error) {
	current, _, err := s.readCurrentWithETag(ctx)
	return current, err
}

func (s *Store) readCurrentData(ctx context.Context) (*Current, error) {
	if reader, ok := s.storage.(interface {
		ReadCurrentData(ctx context.Context) ([]byte, error)
	}); ok {
		data, err := reader.ReadCurrentData(ctx)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				return nil, nil
			}
			return nil, err
		}
		if len(data) == 0 {
			return nil, nil
		}
		var current Current
		if err := json.Unmarshal(data, &current); err != nil {
			return nil, err
		}
		return &current, nil
	}

	return s.readCurrent(ctx)
}

func (s *Store) readCurrentWithETag(ctx context.Context) (*Current, string, error) {
	data, etag, err := s.storage.ReadCurrent(ctx)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			s.mu.Lock()
			s.currentETag = ""
			s.current = nil
			s.mu.Unlock()
			return nil, "", nil
		}
		return nil, "", err
	}
	var current Current
	if err := json.Unmarshal(data, &current); err != nil {
		return nil, "", err
	}
	s.mu.Lock()
	s.currentETag = etag
	s.current = &current
	s.mu.Unlock()
	return &current, etag, nil
}

func (s *Store) writeCurrentWithCAS(ctx context.Context, current *Current, expectedETag string) error {
	data, err := EncodeCurrent(current)
	if err != nil {
		return err
	}
	etag, err := s.storage.WriteCurrentCAS(ctx, data, expectedETag)
	if err != nil {
		return err
	}
	if current != nil {
		clone := *current
		s.mu.Lock()
		s.current = &clone
		s.currentETag = etag
		s.mu.Unlock()
	}
	return nil
}

func (s *Store) logPath(seq uint64) string {
	return s.storage.LogPath(formatLogSeq(seq))
}

func (s *Store) appendCurrent(ctx context.Context, entry *ManifestLogEntry) error {
	const maxRetries = 3

	for attempt := 0; attempt < maxRetries; attempt++ {
		s.mu.Lock()
		current := s.current
		etag := s.currentETag
		s.mu.Unlock()

		if current == nil {
			var err error
			current, etag, err = s.readCurrentWithETag(ctx)
			if err != nil {
				return err
			}
			if current == nil {
				current = &Current{NextEpoch: 1}
			}
		}

		updated := *current
		if updated.LogSeqStart == updated.NextSeq {
			updated.LogSeqStart = entry.Seq
		}
		nextSeq := entry.Seq + 1
		if updated.NextSeq > nextSeq {
			nextSeq = updated.NextSeq
		}
		updated.NextSeq = nextSeq
		updated.NextEpoch = nextEpochFromEntry(updated.NextEpoch, entry)

		if err := s.writeCurrentWithCAS(ctx, &updated, etag); err != nil {
			if errors.Is(err, ErrPreconditionFailed) {
				current, _, readErr := s.readCurrentWithETag(ctx)
				if readErr != nil {
					return readErr
				}
				if fenceErr := s.checkFenceWithCurrent(entry.Role, current); fenceErr != nil {
					return fenceErr
				}
				continue
			}
			return err
		}
		return nil
	}

	return ErrFenceConflict
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
