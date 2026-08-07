package manifest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/segmentio/ksuid"
)

const MaintenanceHeadLayoutVersion = 1

const maxMaintenanceCommandIDBytes = 128

var (
	ErrMaintenanceCommandPending  = errors.New("maintenance command already pending")
	ErrInvalidMaintenanceCommand  = errors.New("invalid maintenance command")
	ErrMaintenanceCommandRejected = errors.New("maintenance command rejected")
)

type MaintenanceCommandKind string

const (
	MaintenanceCommandCheckpoint      MaintenanceCommandKind = "checkpoint"
	MaintenanceCommandCompaction      MaintenanceCommandKind = "compaction"
	MaintenanceCommandRemoveSSTables  MaintenanceCommandKind = "remove_sstables"
	MaintenanceCommandChangeFeedFloor MaintenanceCommandKind = "change_feed_floor"
	MaintenanceCommandRetirementFloor MaintenanceCommandKind = "retirement_floor"
)

type MaintenanceHead struct {
	LayoutVersion int                 `json:"layout_version"`
	Epoch         uint64              `json:"epoch"`
	OwnerID       string              `json:"owner_id"`
	ClaimedAt     time.Time           `json:"claimed_at"`
	Generation    uint64              `json:"generation"`
	Pending       *MaintenanceCommand `json:"pending,omitempty"`
}

type MaintenanceCommand struct {
	ID         string                 `json:"id"`
	Epoch      uint64                 `json:"epoch"`
	Generation uint64                 `json:"generation"`
	Kind       MaintenanceCommandKind `json:"kind"`
	CreatedAt  time.Time              `json:"created_at"`

	Checkpoint      *CheckpointCommand     `json:"checkpoint,omitempty"`
	Compaction      *CompactionCommand     `json:"compaction,omitempty"`
	RemoveSSTables  *RemoveSSTablesCommand `json:"remove_sstables,omitempty"`
	ChangeFeedFloor *AdvanceFloorCommand   `json:"change_feed_floor,omitempty"`
	RetirementFloor *AdvanceFloorCommand   `json:"retirement_floor,omitempty"`
}

type CheckpointCommand struct {
	Snapshot          string `json:"snapshot"`
	BaseSnapshot      string `json:"base_snapshot,omitempty"`
	BaseLogSeqStart   uint64 `json:"base_log_seq_start"`
	SnapshotNextSeq   uint64 `json:"snapshot_next_seq"`
	FoldedReplayPages uint64 `json:"folded_replay_pages"`
	FoldedReplayBytes uint64 `json:"folded_replay_bytes"`
}

type CompactionCommand struct {
	Payload        CompactionLogPayload `json:"payload"`
	RetiredObjects []RetiredObject      `json:"retired_objects,omitempty"`
}

type RemoveSSTablesCommand struct {
	SSTableIDs     []string        `json:"sstable_ids"`
	RetiredObjects []RetiredObject `json:"retired_objects,omitempty"`
}

type AdvanceFloorCommand struct {
	Floor uint64 `json:"floor"`
}

type MaintenanceStatus string

const (
	MaintenanceStatusApplied  MaintenanceStatus = "applied"
	MaintenanceStatusRejected MaintenanceStatus = "rejected"
)

type MaintenanceReceipt struct {
	CommandID  string            `json:"command_id"`
	Epoch      uint64            `json:"epoch"`
	Generation uint64            `json:"generation"`
	Status     MaintenanceStatus `json:"status"`
	AppliedAt  time.Time         `json:"applied_at"`
}

type MaintenanceApplyResult struct {
	CommandID  string
	Epoch      uint64
	Generation uint64
	Status     MaintenanceStatus
	Changed    bool
}

func (r *MaintenanceReceipt) Matches(command *MaintenanceCommand) bool {
	return r != nil && command != nil &&
		r.CommandID == command.ID &&
		r.Epoch == command.Epoch &&
		r.Generation == command.Generation
}

func EncodeMaintenanceHead(head *MaintenanceHead) ([]byte, error) {
	return json.Marshal(head)
}

func DecodeMaintenanceHead(data []byte) (*MaintenanceHead, error) {
	var head MaintenanceHead
	if err := json.Unmarshal(data, &head); err != nil {
		return nil, err
	}
	if head.LayoutVersion != MaintenanceHeadLayoutVersion {
		return nil, fmt.Errorf("unsupported maintenance HEAD layout version=%d", head.LayoutVersion)
	}
	if head.Pending != nil {
		if err := head.Pending.Validate(); err != nil {
			return nil, err
		}
	}
	return &head, nil
}

func (c *MaintenanceCommand) Validate() error {
	if c == nil {
		return fmt.Errorf("%w: nil command", ErrInvalidMaintenanceCommand)
	}
	if c.ID == "" || len(c.ID) > maxMaintenanceCommandIDBytes {
		return fmt.Errorf("%w: command_id bytes=%d", ErrInvalidMaintenanceCommand, len(c.ID))
	}
	if c.Epoch == 0 || c.Generation == 0 || c.CreatedAt.IsZero() {
		return fmt.Errorf("%w: incomplete command identity", ErrInvalidMaintenanceCommand)
	}
	payloads := 0
	if c.Checkpoint != nil {
		payloads++
	}
	if c.Compaction != nil {
		payloads++
	}
	if c.RemoveSSTables != nil {
		payloads++
	}
	if c.ChangeFeedFloor != nil {
		payloads++
	}
	if c.RetirementFloor != nil {
		payloads++
	}
	if payloads != 1 {
		return fmt.Errorf("%w: payload count=%d", ErrInvalidMaintenanceCommand, payloads)
	}
	switch c.Kind {
	case MaintenanceCommandCheckpoint:
		if c.Checkpoint == nil || c.Checkpoint.Snapshot == "" {
			return fmt.Errorf("%w: invalid checkpoint", ErrInvalidMaintenanceCommand)
		}
	case MaintenanceCommandCompaction:
		if c.Compaction == nil {
			return fmt.Errorf("%w: missing compaction", ErrInvalidMaintenanceCommand)
		}
	case MaintenanceCommandRemoveSSTables:
		if c.RemoveSSTables == nil || len(c.RemoveSSTables.SSTableIDs) == 0 {
			return fmt.Errorf("%w: missing removed SSTs", ErrInvalidMaintenanceCommand)
		}
	case MaintenanceCommandChangeFeedFloor:
		if c.ChangeFeedFloor == nil {
			return fmt.Errorf("%w: missing change-feed floor", ErrInvalidMaintenanceCommand)
		}
	case MaintenanceCommandRetirementFloor:
		if c.RetirementFloor == nil {
			return fmt.Errorf("%w: missing retirement floor", ErrInvalidMaintenanceCommand)
		}
	default:
		return fmt.Errorf("%w: kind=%q", ErrInvalidMaintenanceCommand, c.Kind)
	}
	return nil
}

func (s *Store) ReadMaintenanceHead(ctx context.Context) (*MaintenanceHead, string, error) {
	data, etag, err := s.storage.ReadMaintenanceHead(ctx)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, "", nil
		}
		return nil, "", err
	}
	if len(data) == 0 {
		return nil, etag, nil
	}
	head, err := DecodeMaintenanceHead(data)
	return head, etag, err
}

func (s *Store) ClaimMaintenance(ctx context.Context, ownerID string) (*FenceToken, error) {
	if ownerID == "" {
		return nil, fmt.Errorf("empty maintenance owner")
	}
	for attempt := 0; attempt < currentCASMaxRetries; attempt++ {
		head, etag, err := s.ReadMaintenanceHead(ctx)
		if err != nil {
			return nil, err
		}
		if head == nil {
			head = &MaintenanceHead{LayoutVersion: MaintenanceHeadLayoutVersion}
		}
		if head.Epoch == ^uint64(0) || head.Generation == ^uint64(0) {
			return nil, fmt.Errorf("%w: maintenance counter exhausted", ErrInvalidMaintenanceCommand)
		}
		now := time.Now().UTC()
		head.Epoch++
		head.Generation++
		head.OwnerID = ownerID
		head.ClaimedAt = now
		body, err := EncodeMaintenanceHead(head)
		if err != nil {
			return nil, err
		}
		if _, err := s.storage.WriteMaintenanceHeadCAS(ctx, body, etag); err != nil {
			if errors.Is(err, ErrPreconditionFailed) {
				if err := sleepBeforeCurrentCASRetry(ctx, attempt); err != nil {
					return nil, err
				}
				continue
			}
			return nil, err
		}
		return &FenceToken{Epoch: head.Epoch, Owner: ownerID, ClaimedAt: now}, nil
	}
	return nil, ErrFenceConflict
}

func (s *Store) StageMaintenance(ctx context.Context, command MaintenanceCommand, token *FenceToken) (*MaintenanceHead, error) {
	head, etag, err := s.ReadMaintenanceHead(ctx)
	if err != nil {
		return nil, err
	}
	if head == nil || token == nil || head.Epoch != token.Epoch || head.OwnerID != token.Owner || !head.ClaimedAt.Equal(token.ClaimedAt) {
		return nil, ErrFenced
	}
	if head.Pending != nil {
		return nil, ErrMaintenanceCommandPending
	}
	if head.Generation == ^uint64(0) {
		return nil, fmt.Errorf("%w: maintenance generation exhausted", ErrInvalidMaintenanceCommand)
	}
	head.Generation++
	command.Epoch = head.Epoch
	command.Generation = head.Generation
	if command.CreatedAt.IsZero() {
		command.CreatedAt = time.Now().UTC()
	}
	if err := command.Validate(); err != nil {
		return nil, err
	}
	head.Pending = &command
	body, err := EncodeMaintenanceHead(head)
	if err != nil {
		return nil, err
	}
	if _, err := s.storage.WriteMaintenanceHeadCAS(ctx, body, etag); err != nil {
		if errors.Is(err, ErrPreconditionFailed) {
			return nil, ErrFenceConflict
		}
		return nil, err
	}
	return head, nil
}

func (s *Store) ClearMaintenance(ctx context.Context, commandID string, epoch, generation uint64, token *FenceToken) (*MaintenanceHead, error) {
	head, etag, err := s.ReadMaintenanceHead(ctx)
	if err != nil {
		return nil, err
	}
	if head == nil || token == nil || head.Epoch != token.Epoch || head.OwnerID != token.Owner || !head.ClaimedAt.Equal(token.ClaimedAt) {
		return nil, ErrFenced
	}
	if head.Pending == nil {
		return head, nil
	}
	if head.Pending.ID != commandID || head.Pending.Epoch != epoch || head.Pending.Generation != generation {
		return nil, ErrFenceConflict
	}
	if head.Generation == ^uint64(0) {
		return nil, fmt.Errorf("%w: maintenance generation exhausted", ErrInvalidMaintenanceCommand)
	}
	head.Generation++
	head.Pending = nil
	body, err := EncodeMaintenanceHead(head)
	if err != nil {
		return nil, err
	}
	if _, err := s.storage.WriteMaintenanceHeadCAS(ctx, body, etag); err != nil {
		if errors.Is(err, ErrPreconditionFailed) {
			return nil, ErrFenceConflict
		}
		return nil, err
	}
	return head, nil
}

// ApplyPendingMaintenance publishes the current maintenance command through
// the active writer fence. The command effects and receipt share one CURRENT
// CAS, so acknowledgement cannot become visible without the state change.
func (s *Store) ApplyPendingMaintenance(ctx context.Context) (MaintenanceApplyResult, error) {
	if err := s.checkLocalFence(FenceRoleWriter); err != nil {
		return MaintenanceApplyResult{}, err
	}
	head, _, err := s.ReadMaintenanceHead(ctx)
	if err != nil {
		return MaintenanceApplyResult{}, err
	}
	if head == nil || head.Pending == nil {
		return MaintenanceApplyResult{}, nil
	}
	command := *head.Pending
	if err := command.Validate(); err != nil {
		return MaintenanceApplyResult{}, err
	}

	s.commitMu.Lock()
	defer s.commitMu.Unlock()
	for attempt := 0; attempt < currentCASMaxRetries; attempt++ {
		current, etag, err := s.readCurrentWithETag(ctx)
		if err != nil {
			return MaintenanceApplyResult{}, err
		}
		if err := s.checkFenceWithCurrent(FenceRoleWriter, current); err != nil {
			return MaintenanceApplyResult{}, err
		}
		if receiptMatchesCommand(current.MaintenanceReceipt, &command) {
			return MaintenanceApplyResult{
				CommandID:  command.ID,
				Epoch:      command.Epoch,
				Generation: command.Generation,
				Status:     current.MaintenanceReceipt.Status,
			}, nil
		}

		updated := current.Clone()
		status := MaintenanceStatusApplied
		if err := s.applyMaintenanceCommand(ctx, updated, &command); err != nil {
			if !errors.Is(err, ErrMaintenanceCommandRejected) {
				return MaintenanceApplyResult{}, err
			}
			status = MaintenanceStatusRejected
		}
		updated.MaintenanceReceipt = &MaintenanceReceipt{
			CommandID:  command.ID,
			Epoch:      command.Epoch,
			Generation: command.Generation,
			Status:     status,
			AppliedAt:  time.Now().UTC(),
		}
		if err := s.rotateActiveEntriesForCurrentSize(ctx, updated); err != nil {
			return MaintenanceApplyResult{}, err
		}
		if err := s.writeCurrentWithCAS(ctx, updated, etag); err != nil {
			if errors.Is(err, ErrPreconditionFailed) {
				if attempt+1 < currentCASMaxRetries {
					if err := sleepBeforeCurrentCASRetry(ctx, attempt); err != nil {
						return MaintenanceApplyResult{}, err
					}
				}
				continue
			}
			return MaintenanceApplyResult{}, err
		}
		s.mu.Lock()
		if s.nextSeq < updated.NextSeq {
			s.nextSeq = updated.NextSeq
		}
		s.mu.Unlock()
		return MaintenanceApplyResult{
			CommandID:  command.ID,
			Epoch:      command.Epoch,
			Generation: command.Generation,
			Status:     status,
			Changed:    true,
		}, nil
	}
	return MaintenanceApplyResult{}, ErrFenceConflict
}

func (s *Store) applyMaintenanceCommand(ctx context.Context, current *Current, command *MaintenanceCommand) error {
	switch command.Kind {
	case MaintenanceCommandCheckpoint:
		checkpoint := command.Checkpoint
		if current.Snapshot != checkpoint.BaseSnapshot || current.LogSeqStart != checkpoint.BaseLogSeqStart ||
			current.NextSeq < checkpoint.SnapshotNextSeq ||
			current.StateReplayPages < checkpoint.FoldedReplayPages ||
			current.StateReplayBytes < checkpoint.FoldedReplayBytes {
			return ErrMaintenanceCommandRejected
		}
		oldLogSeqStart := current.LogSeqStart
		if !current.ChangeFeedEnabled && current.ChangeFeedLogStart == oldLogSeqStart {
			current.ChangeFeedLogStart = checkpoint.SnapshotNextSeq
		}
		current.Snapshot = checkpoint.Snapshot
		current.LogSeqStart = checkpoint.SnapshotNextSeq
		current.StateReplayPages -= checkpoint.FoldedReplayPages
		current.StateReplayBytes -= checkpoint.FoldedReplayBytes
		pruneRetainedManifestRefs(current)
		return nil

	case MaintenanceCommandCompaction:
		entry := &ManifestLogEntry{
			MaintenanceCommandID: command.ID,
			Op:                   LogOpCompaction,
			Compaction:           &command.Compaction.Payload,
			RetiredObjects:       append([]RetiredObject(nil), command.Compaction.RetiredObjects...),
		}
		if err := validateCompactionPayload(command.Compaction.Payload); err != nil {
			return fmt.Errorf("%w: %v", ErrMaintenanceCommandRejected, err)
		}
		if err := validateRetiredObjects(entry); err != nil {
			return fmt.Errorf("%w: %v", ErrMaintenanceCommandRejected, err)
		}
		return s.appendWriterOwnedMaintenanceEntry(ctx, current, entry)

	case MaintenanceCommandRemoveSSTables:
		entry := &ManifestLogEntry{
			MaintenanceCommandID: command.ID,
			Op:                   LogOpRemoveSSTable,
			RemoveSSTableIDs:     append([]string(nil), command.RemoveSSTables.SSTableIDs...),
			RetiredObjects:       append([]RetiredObject(nil), command.RemoveSSTables.RetiredObjects...),
		}
		if err := validateRetiredObjects(entry); err != nil {
			return fmt.Errorf("%w: %v", ErrMaintenanceCommandRejected, err)
		}
		return s.appendWriterOwnedMaintenanceEntry(ctx, current, entry)

	case MaintenanceCommandChangeFeedFloor:
		floor := command.ChangeFeedFloor.Floor
		if floor < current.ChangeFeedLogStart {
			floor = current.ChangeFeedLogStart
		}
		if floor > current.NextSeq {
			floor = current.NextSeq
		}
		current.ChangeFeedLogStart = floor
		pruneRetainedManifestRefs(current)
		return nil

	case MaintenanceCommandRetirementFloor:
		floor := command.RetirementFloor.Floor
		if floor < current.RetirementLogStart {
			floor = current.RetirementLogStart
		}
		if floor > current.NextSeq {
			floor = current.NextSeq
		}
		current.RetirementLogStart = floor
		pruneRetainedManifestRefs(current)
		return nil
	default:
		return fmt.Errorf("%w: kind=%q", ErrInvalidMaintenanceCommand, command.Kind)
	}
}

func (s *Store) appendWriterOwnedMaintenanceEntry(ctx context.Context, current *Current, entry *ManifestLogEntry) error {
	if err := validateRetiredObjects(entry); err != nil {
		return err
	}
	if len(current.ActiveEntries) >= s.activeLimit() {
		if err := s.rotateActiveEntries(ctx, current); err != nil {
			return err
		}
	}
	entry.ID = ksuid.New()
	entry.Seq = current.NextSeq
	entry.Role = FenceRoleWriter
	entry.Epoch = current.WriterFence.Epoch
	entry.Timestamp = time.Now().UTC()
	if current.LogSeqStart == current.NextSeq {
		current.LogSeqStart = entry.Seq
	}
	current.ActiveEntries = append(current.ActiveEntries, *entry)
	current.NextSeq = entry.Seq + 1
	current.NextEpoch = nextEpochFromEntry(current.NextEpoch, entry)
	return nil
}

func receiptMatchesCommand(receipt *MaintenanceReceipt, command *MaintenanceCommand) bool {
	return receipt.Matches(command)
}

func pruneRetainedManifestRefs(current *Current) {
	retainedFrom := retainedEntryFloor(current)
	current.ActiveEntries = filterEntriesAtOrAfter(current.ActiveEntries, retainedFrom)
	current.IndexFrontier = filterPageRefsAtOrAfter(current.IndexFrontier, retainedFrom)
}
