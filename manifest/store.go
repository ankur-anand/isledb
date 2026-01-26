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

// Store manages manifest log IO using snapshot + log segments + CURRENT pointer.
type Store struct {
	store   *blobstore.Store
	mu      sync.Mutex
	nextSeq uint64
}

// NewStore creates a new manifest store.
func NewStore(store *blobstore.Store) *Store {
	return &Store{store: store}
}

// Append writes a new log entry to storage and updates CURRENT.
func (s *Store) Append(ctx context.Context, entry *ManifestLogEntry) error {
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
	path := s.store.ManifestLogPath(logName)
	if _, err := s.store.Write(ctx, path, data); err != nil {
		return fmt.Errorf("write log entry: %w", err)
	}

	return s.appendCurrent(ctx, path, entry)
}

// AppendAddSSTableWithVLogs logs an SSTable addition with optional VLogs.
func (s *Store) AppendAddSSTableWithVLogs(ctx context.Context, sst SSTMeta, vlogs []VLogMeta) (*ManifestLogEntry, error) {
	entry := &ManifestLogEntry{
		Op:       LogOpAddSSTable,
		SSTable:  &sst,
		AddVLogs: vlogs,
	}
	if err := s.Append(ctx, entry); err != nil {
		return nil, err
	}
	return entry, nil
}

// AppendRemoveSSTables logs SSTable removals.
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

// AppendCheckpoint logs a full manifest checkpoint.
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

// AppendCompaction logs an atomic compaction operation.
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

// Replay reads CURRENT, snapshot, and logs to rebuild the manifest.
func (s *Store) Replay(ctx context.Context) (*Manifest, error) {
	current, err := s.readCurrent(ctx)
	if err != nil {
		return nil, err
	}

	var m *Manifest
	if current != nil && current.Snapshot != "" {
		data, _, err := s.store.Read(ctx, current.Snapshot)
		if err != nil && !errors.Is(err, blobstore.ErrNotFound) {
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

	logs := currentLogs(current)
	if len(logs) == 0 {
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

// List returns all log entry paths in sequence order (oldest first).
func (s *Store) List(ctx context.Context) ([]string, error) {
	objects, err := s.store.ListManifestLogs(ctx)
	if err != nil {
		return nil, fmt.Errorf("list manifest logs: %w", err)
	}

	var entries []string
	for _, obj := range objects {
		if obj.IsDir {
			continue
		}
		entries = append(entries, obj.Key)
	}
	sort.Strings(entries)
	return entries, nil
}

// Read loads a single log entry by path.
func (s *Store) Read(ctx context.Context, path string) (*ManifestLogEntry, error) {
	data, _, err := s.store.Read(ctx, path)
	if err != nil {
		if errors.Is(err, blobstore.ErrNotFound) {
			return nil, fmt.Errorf("log entry %s not found", path)
		}
		return nil, fmt.Errorf("read log entry: %w", err)
	}
	return DecodeLogEntry(data)
}

// WriteSnapshot writes a snapshot and updates CURRENT to point to it.
func (s *Store) WriteSnapshot(ctx context.Context, m *Manifest) (string, error) {
	if m == nil {
		return "", errors.New("nil manifest")
	}
	data, err := EncodeSnapshot(m)
	if err != nil {
		return "", err
	}

	id := ksuid.New().String()
	path := s.store.ManifestSnapshotPath(id)
	if _, err := s.store.Write(ctx, path, data); err != nil {
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
	current.Logs = nil
	current.NextEpoch = m.NextEpoch
	current.NextSeq = s.nextSeq

	if err := s.writeCurrent(ctx, current); err != nil {
		return "", err
	}
	return path, nil
}

func currentLogs(current *Current) []string {
	if current == nil || len(current.Logs) == 0 {
		return nil
	}
	out := make([]string, len(current.Logs))
	copy(out, current.Logs)
	return out
}

func formatLogSeq(seq uint64) string {
	return fmt.Sprintf("%020d", seq)
}

func (s *Store) readCurrent(ctx context.Context) (*Current, error) {
	data, _, err := s.store.Read(ctx, s.store.ManifestPath())
	if err != nil {
		if errors.Is(err, blobstore.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	var current Current
	if err := json.Unmarshal(data, &current); err != nil {
		return nil, err
	}
	return &current, nil
}

func (s *Store) writeCurrent(ctx context.Context, current *Current) error {
	data, err := EncodeCurrent(current)
	if err != nil {
		return err
	}
	if _, err := s.store.Write(ctx, s.store.ManifestPath(), data); err != nil {
		return err
	}
	return nil
}

func (s *Store) appendCurrent(ctx context.Context, logPath string, entry *ManifestLogEntry) error {
	current, err := s.readCurrent(ctx)
	if err != nil {
		return err
	}
	if current == nil {
		current = &Current{NextEpoch: 1}
	}

	current.Logs = append(current.Logs, logPath)
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
