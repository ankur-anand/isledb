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
	return s.claimFence(ctx, FenceRoleWriter, ownerID)
}

func (s *Store) ClaimCompactor(ctx context.Context, ownerID string) (*FenceToken, error) {
	return s.claimFence(ctx, FenceRoleCompactor, ownerID)
}

func (s *Store) claimFence(ctx context.Context, role FenceRole, ownerID string) (*FenceToken, error) {
	const maxRetries = 5

	for attempt := 0; attempt < maxRetries; attempt++ {

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

				time.Sleep(time.Millisecond * 10 * time.Duration(attempt+1))
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

func (s *Store) Append(ctx context.Context, entry *ManifestLogEntry) error {
	return s.appendInternal(ctx, entry, FenceRole(-1))
}

func (s *Store) AppendWithWriterFence(ctx context.Context, entry *ManifestLogEntry) error {
	if err := s.CheckWriterFence(ctx); err != nil {
		return err
	}
	return s.appendInternal(ctx, entry, FenceRoleWriter)
}

func (s *Store) AppendWithCompactorFence(ctx context.Context, entry *ManifestLogEntry) error {
	if err := s.CheckCompactorFence(ctx); err != nil {
		return err
	}
	return s.appendInternal(ctx, entry, FenceRoleCompactor)
}

func (s *Store) appendInternal(ctx context.Context, entry *ManifestLogEntry, role FenceRole) error {
	s.mu.Lock()
	entry.Seq = s.nextSeq
	s.nextSeq++
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
		return fmt.Errorf("write log entry: %w", err)
	}

	return s.appendCurrent(ctx, entry)
}

func (s *Store) AppendAddSSTable(ctx context.Context, sst SSTMeta) (*ManifestLogEntry, error) {
	entry := &ManifestLogEntry{
		Op:      LogOpAddSSTable,
		SSTable: &sst,
	}
	if err := s.Append(ctx, entry); err != nil {
		return nil, err
	}
	return entry, nil
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

func (s *Store) AppendRemoveSSTables(ctx context.Context, sstableIDs []string) (*ManifestLogEntry, error) {
	entry := &ManifestLogEntry{
		Op:               LogOpRemoveSSTable,
		RemoveSSTableIDs: sstableIDs,
	}
	if err := s.Append(ctx, entry); err != nil {
		return nil, err
	}
	return entry, nil
}

func (s *Store) AppendCheckpoint(ctx context.Context, manifest *Manifest) (*ManifestLogEntry, error) {
	entry := &ManifestLogEntry{
		Op:         LogOpCheckpoint,
		Checkpoint: manifest,
	}
	if err := s.Append(ctx, entry); err != nil {
		return nil, err
	}
	return entry, nil
}

func (s *Store) AppendCompaction(ctx context.Context, payload CompactionLogPayload) (*ManifestLogEntry, error) {
	entry := &ManifestLogEntry{
		Op:         LogOpCompaction,
		Compaction: &payload,
	}
	if err := s.Append(ctx, entry); err != nil {
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
		if err != nil && !errors.Is(err, ErrNotFound) {
			return nil, err
		}
		if len(data) > 0 {
			snap, err := DecodeSnapshot(data)
			if err != nil {
				return nil, err
			}
			m = snap
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

	var maxSeq uint64
	for _, path := range logs {
		entry, err := s.Read(ctx, path)
		if err != nil {
			return nil, err
		}
		if entry.Seq > maxSeq {
			maxSeq = entry.Seq
		}
		m = ApplyLogEntry(m, entry)
	}

	if current != nil && current.NextEpoch > m.NextEpoch {
		m.NextEpoch = current.NextEpoch
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
	data, err := EncodeSnapshot(m)
	if err != nil {
		return "", err
	}

	id := ksuid.New().String()
	path, err := s.storage.WriteSnapshot(ctx, id, data)
	if err != nil {
		return "", err
	}

	current, err := s.readCurrent(ctx)
	if err != nil {
		return "", err
	}
	if current == nil {
		current = &Current{NextEpoch: m.NextEpoch}
	}
	current.Snapshot = path
	current.LogSeqStart = s.nextSeq
	current.NextEpoch = m.NextEpoch
	current.NextSeq = s.nextSeq

	if err := s.writeCurrent(ctx, current); err != nil {
		return "", err
	}
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

func (s *Store) readCurrentWithETag(ctx context.Context) (*Current, string, error) {
	data, etag, err := s.storage.ReadCurrent(ctx)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, "", nil
		}
		return nil, "", err
	}
	var current Current
	if err := json.Unmarshal(data, &current); err != nil {
		return nil, "", err
	}
	return &current, etag, nil
}

func (s *Store) writeCurrent(ctx context.Context, current *Current) error {
	data, err := EncodeCurrent(current)
	if err != nil {
		return err
	}
	return s.storage.WriteCurrent(ctx, data)
}

func (s *Store) writeCurrentWithCAS(ctx context.Context, current *Current, expectedETag string) error {
	data, err := EncodeCurrent(current)
	if err != nil {
		return err
	}
	return s.storage.WriteCurrentCAS(ctx, data, expectedETag)
}

func (s *Store) logPath(seq uint64) string {
	return s.storage.LogPath(formatLogSeq(seq))
}

func (s *Store) appendCurrent(ctx context.Context, entry *ManifestLogEntry) error {
	current, err := s.readCurrent(ctx)
	if err != nil {
		return err
	}
	if current == nil {
		current = &Current{NextEpoch: 1}
	}

	if current.LogSeqStart == current.NextSeq {
		current.LogSeqStart = entry.Seq
	}
	current.NextSeq = s.nextSeq
	current.NextEpoch = nextEpochFromEntry(current.NextEpoch, entry)

	return s.writeCurrent(ctx, current)
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
