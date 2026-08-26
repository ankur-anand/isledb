package manifest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/segmentio/ksuid"
)

const MaintenanceHeadLayoutVersion = 2

const maxMaintenanceCommandIDBytes = 128
const maxMaintenanceSchedulingWorkUnits uint32 = 4
const MaxChangeFeedDeleteTargetsPerCommand = MaxRetiredObjectsPerEntry

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
	Scheduling MaintenanceScheduling  `json:"scheduling,omitempty"`

	Checkpoint      *CheckpointCommand     `json:"checkpoint,omitempty"`
	Compaction      *CompactionCommand     `json:"compaction,omitempty"`
	RemoveSSTables  *RemoveSSTablesCommand `json:"remove_sstables,omitempty"`
	ChangeFeedFloor *AdvanceFloorCommand   `json:"change_feed_floor,omitempty"`
}

// MaintenanceScheduling records the bounded cost of a primary maintenance
// command so the writer can advance durable fairness state atomically with
// publication.
type MaintenanceScheduling struct {
	WorkUnits uint32 `json:"work_units,omitempty"`
}

type CheckpointCommand struct {
	Snapshot                 ObjectRef  `json:"snapshot"`
	BaseSnapshot             *ObjectRef `json:"base_snapshot,omitempty"`
	BaseLogSeqStart          uint64     `json:"base_log_seq_start"`
	SnapshotNextSeq          uint64     `json:"snapshot_next_seq"`
	FoldedReplayPages        uint64     `json:"folded_replay_pages"`
	FoldedReplayBytes        uint64     `json:"folded_replay_bytes"`
	FoldedReplayMaxPageLevel uint8      `json:"folded_replay_max_page_level,omitempty"`
}

type CompactionCommand struct {
	Payload        CompactionLogPayload `json:"payload"`
	RetiredObjects []RetiredObject      `json:"retired_objects,omitempty"`
}

type RemoveSSTablesCommand struct {
	SSTableIDs     []string        `json:"sstable_ids"`
	RetiredObjects []RetiredObject `json:"retired_objects,omitempty"`
}

type ChangeFeedDeleteTarget struct {
	Path     string `json:"path"`
	ID       string `json:"id"`
	Seq      uint64 `json:"seq"`
	Size     int64  `json:"size"`
	Checksum string `json:"checksum,omitempty"`
}

type AdvanceFloorCommand struct {
	Floor           uint64                   `json:"floor"`
	GracePeriod     time.Duration            `json:"grace_period_nanos,omitempty"`
	DeletionTargets []ChangeFeedDeleteTarget `json:"deletion_targets,omitempty"`
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
	if payloads != 1 {
		return fmt.Errorf("%w: payload count=%d", ErrInvalidMaintenanceCommand, payloads)
	}
	switch c.Kind {
	case MaintenanceCommandCheckpoint:
		if c.Scheduling.WorkUnits != 0 {
			return fmt.Errorf("%w: checkpoint work_units=%d", ErrInvalidMaintenanceCommand, c.Scheduling.WorkUnits)
		}
		if c.Checkpoint == nil {
			return fmt.Errorf("%w: invalid checkpoint", ErrInvalidMaintenanceCommand)
		}
		if err := validateManifestObjectRef(c.Checkpoint.Snapshot, manifestObjectKindSnapshot); err != nil {
			return fmt.Errorf("%w: invalid checkpoint snapshot: %v", ErrInvalidMaintenanceCommand, err)
		}
		if c.Checkpoint.BaseSnapshot != nil {
			if err := validateManifestObjectRef(*c.Checkpoint.BaseSnapshot, manifestObjectKindSnapshot); err != nil {
				return fmt.Errorf("%w: invalid checkpoint base snapshot: %v", ErrInvalidMaintenanceCommand, err)
			}
		}
	case MaintenanceCommandCompaction:
		if c.Compaction == nil {
			return fmt.Errorf("%w: missing compaction", ErrInvalidMaintenanceCommand)
		}
		if c.Scheduling.WorkUnits > maxMaintenanceSchedulingWorkUnits {
			return fmt.Errorf("%w: compaction work_units=%d max=%d",
				ErrInvalidMaintenanceCommand, c.Scheduling.WorkUnits, maxMaintenanceSchedulingWorkUnits)
		}
		if err := validateRetiredObjects(&ManifestLogEntry{
			Op:             LogOpCompaction,
			Compaction:     &c.Compaction.Payload,
			RetiredObjects: c.Compaction.RetiredObjects,
		}); err != nil {
			return fmt.Errorf("%w: invalid compaction retirement: %v", ErrInvalidMaintenanceCommand, err)
		}
	case MaintenanceCommandRemoveSSTables:
		if c.Scheduling.WorkUnits != 0 {
			return fmt.Errorf("%w: remove SST work_units=%d", ErrInvalidMaintenanceCommand, c.Scheduling.WorkUnits)
		}
		if c.RemoveSSTables == nil || len(c.RemoveSSTables.SSTableIDs) == 0 {
			return fmt.Errorf("%w: missing removed SSTs", ErrInvalidMaintenanceCommand)
		}
		if err := validateRetiredObjects(&ManifestLogEntry{
			Op:               LogOpRemoveSSTable,
			RemoveSSTableIDs: c.RemoveSSTables.SSTableIDs,
			RetiredObjects:   c.RemoveSSTables.RetiredObjects,
		}); err != nil {
			return fmt.Errorf("%w: invalid removed-SST retirement: %v", ErrInvalidMaintenanceCommand, err)
		}
	case MaintenanceCommandChangeFeedFloor:
		if c.Scheduling.WorkUnits != 0 {
			return fmt.Errorf("%w: change-feed floor work_units=%d", ErrInvalidMaintenanceCommand, c.Scheduling.WorkUnits)
		}
		if c.ChangeFeedFloor == nil {
			return fmt.Errorf("%w: missing change-feed floor", ErrInvalidMaintenanceCommand)
		}
		if err := validateChangeFeedFloorCommand(c.ChangeFeedFloor); err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidMaintenanceCommand, err)
		}
	default:
		return fmt.Errorf("%w: kind=%q", ErrInvalidMaintenanceCommand, c.Kind)
	}
	return nil
}

func validateChangeFeedFloorCommand(command *AdvanceFloorCommand) error {
	if command == nil || command.GracePeriod < 0 {
		return errors.New("invalid change-feed deletion grace period")
	}
	if len(command.DeletionTargets) > MaxChangeFeedDeleteTargetsPerCommand {
		return fmt.Errorf("change-feed deletion target count=%d max=%d",
			len(command.DeletionTargets), MaxChangeFeedDeleteTargetsPerCommand)
	}
	if len(command.DeletionTargets) == 0 {
		if command.GracePeriod != 0 {
			return errors.New("change-feed deletion grace period has no targets")
		}
		return nil
	}
	seenPaths := make(map[string]struct{}, len(command.DeletionTargets))
	seenIDs := make(map[string]struct{}, len(command.DeletionTargets))
	var previousSeq uint64
	for i, target := range command.DeletionTargets {
		if target.Path == "" || target.ID == "" || target.Size < 0 || target.Seq >= command.Floor {
			return fmt.Errorf("invalid change-feed deletion target index=%d", i)
		}
		if i > 0 && target.Seq <= previousSeq {
			return errors.New("change-feed deletion targets are not sequence ordered")
		}
		previousSeq = target.Seq
		if _, exists := seenPaths[target.Path]; exists {
			return fmt.Errorf("duplicate change-feed deletion path=%q", target.Path)
		}
		if _, exists := seenIDs[target.ID]; exists {
			return fmt.Errorf("duplicate change-feed deletion id=%q", target.ID)
		}
		seenPaths[target.Path] = struct{}{}
		seenIDs[target.ID] = struct{}{}
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
	var checkpointVerification checkpointSnapshotVerification
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
		if err := s.applyMaintenanceCommand(ctx, updated, &command, &checkpointVerification); err != nil {
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
		if status == MaintenanceStatusApplied {
			advanceMaintenanceScheduler(updated, &command)
		}
		data, err := s.encodeCurrentForCASWithRotation(ctx, updated)
		if err != nil {
			return MaintenanceApplyResult{}, err
		}
		if err := s.writeEncodedCurrentWithCAS(ctx, updated, data, etag); err != nil {
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

func advanceMaintenanceScheduler(current *Current, command *MaintenanceCommand) {
	if current == nil || command == nil {
		return
	}
	switch command.Kind {
	case MaintenanceCommandCheckpoint:
		current.MaintenanceScheduler.LastPrimary = MaintenanceCommandCheckpoint
		current.MaintenanceScheduler.CompactionUnitsSinceCheckpoint = 0
	case MaintenanceCommandCompaction:
		units := command.Scheduling.WorkUnits
		if units == 0 {
			units = 1
		}
		current.MaintenanceScheduler.LastPrimary = MaintenanceCommandCompaction
		current.MaintenanceScheduler.CompactionUnitsSinceCheckpoint = saturatingAddUint32(
			current.MaintenanceScheduler.CompactionUnitsSinceCheckpoint, units)
		sourceLevel := command.Compaction.Payload.SourceLevel
		if sourceLevel == 0 {
			current.MaintenanceScheduler.L0UnitsSinceLower = saturatingAddUint32(
				current.MaintenanceScheduler.L0UnitsSinceLower, units)
			return
		}
		current.MaintenanceScheduler.L0UnitsSinceLower = 0
		if sourceLevel != ^uint32(0) {
			current.MaintenanceScheduler.NextLowerLevel = sourceLevel + 1
		}
	}
}

func saturatingAddUint32(a, b uint32) uint32 {
	if a > ^uint32(0)-b {
		return ^uint32(0)
	}
	return a + b
}

// checkpointSnapshotVerification caches a deterministic candidate validation
// across CURRENT CAS retries. Immutable snapshot candidates never change at a
// given path, so downloading and decoding one more than once would only add
// work without changing the result.
type checkpointSnapshotVerification struct {
	done bool
	err  error
}

func (s *Store) applyMaintenanceCommand(
	ctx context.Context,
	current *Current,
	command *MaintenanceCommand,
	verification *checkpointSnapshotVerification,
) error {
	switch command.Kind {
	case MaintenanceCommandCheckpoint:
		checkpoint := command.Checkpoint
		if !objectRefsEqual(current.Snapshot, checkpoint.BaseSnapshot) || current.LogSeqStart != checkpoint.BaseLogSeqStart ||
			checkpoint.SnapshotNextSeq < current.LogSeqStart ||
			current.NextSeq < checkpoint.SnapshotNextSeq ||
			current.StateReplayPages < checkpoint.FoldedReplayPages ||
			current.StateReplayBytes < checkpoint.FoldedReplayBytes {
			return ErrMaintenanceCommandRejected
		}
		if verification == nil {
			return fmt.Errorf("%w: checkpoint snapshot was not verified", ErrInvalidMaintenanceCommand)
		}
		if !verification.done {
			verification.err = s.verifyCheckpointSnapshot(ctx, current, checkpoint)
			// Missing, malformed, or corrupt immutable input is a permanent
			// command rejection. Other storage failures remain retryable and
			// must not be recorded as a durable rejection receipt.
			if verification.err == nil || errors.Is(verification.err, ErrMaintenanceCommandRejected) {
				verification.done = true
			}
		}
		if verification.err != nil {
			return verification.err
		}
		oldLogSeqStart := current.LogSeqStart
		if !current.ChangeFeedEnabled && current.ChangeFeedLogStart == oldLogSeqStart {
			current.ChangeFeedLogStart = checkpoint.SnapshotNextSeq
		}
		current.Snapshot = checkpoint.Snapshot.Clone()
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
		return s.appendWriterOwnedMaintenanceEntry(ctx, current, entry)

	case MaintenanceCommandRemoveSSTables:
		entry := &ManifestLogEntry{
			MaintenanceCommandID: command.ID,
			Op:                   LogOpRemoveSSTable,
			RemoveSSTableIDs:     append([]string(nil), command.RemoveSSTables.SSTableIDs...),
			RetiredObjects:       append([]RetiredObject(nil), command.RemoveSSTables.RetiredObjects...),
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
	default:
		return fmt.Errorf("%w: kind=%q", ErrInvalidMaintenanceCommand, command.Kind)
	}
}

// verifyCheckpointSnapshot verifies every property on which publishing the
// candidate depends before its ObjectRef can become visible through CURRENT.
// The checksum covers the complete encoded envelope; DecodeSnapshot then
// verifies the zstd envelope and bounded raw size before JSON decoding.
func (s *Store) verifyCheckpointSnapshot(ctx context.Context, current *Current, checkpoint *CheckpointCommand) error {
	data, err := s.storage.ReadSnapshot(ctx, checkpoint.Snapshot.Path)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return fmt.Errorf("%w: checkpoint snapshot %q is missing", ErrMaintenanceCommandRejected, checkpoint.Snapshot.Path)
		}
		return fmt.Errorf("read checkpoint snapshot %q: %w", checkpoint.Snapshot.Path, err)
	}
	if len(data) == 0 {
		return fmt.Errorf("%w: checkpoint snapshot %q is empty", ErrMaintenanceCommandRejected, checkpoint.Snapshot.Path)
	}
	if err := verifyManifestObjectRef(data, checkpoint.Snapshot, manifestObjectKindSnapshot); err != nil {
		return fmt.Errorf("%w: checkpoint snapshot reference: %v", ErrMaintenanceCommandRejected, err)
	}
	snapshot, err := DecodeSnapshot(data)
	if err != nil {
		return fmt.Errorf("%w: decode checkpoint snapshot: %v", ErrMaintenanceCommandRejected, err)
	}
	if snapshot.Version != 2 {
		return fmt.Errorf("%w: checkpoint snapshot version=%d want=2", ErrMaintenanceCommandRejected, snapshot.Version)
	}
	if snapshot.NextEpoch == 0 {
		return fmt.Errorf("%w: checkpoint snapshot next_epoch is zero", ErrMaintenanceCommandRejected)
	}
	if snapshot.NextEpoch > current.NextEpoch {
		return fmt.Errorf("%w: checkpoint snapshot next_epoch=%d exceeds CURRENT=%d",
			ErrMaintenanceCommandRejected, snapshot.NextEpoch, current.NextEpoch)
	}
	if err := validateCheckpointSnapshotFence("writer", snapshot.WriterFence, snapshot.NextEpoch); err != nil {
		return err
	}
	if err := validateCheckpointSnapshotFence("compactor", snapshot.CompactorFence, snapshot.NextEpoch); err != nil {
		return err
	}
	if checkpoint.SnapshotNextSeq == 0 {
		if snapshot.LogSeq != 0 || len(snapshot.L0SSTs) != 0 || len(snapshot.Levels) != 0 {
			return fmt.Errorf("%w: non-empty checkpoint snapshot at next_seq=0", ErrMaintenanceCommandRejected)
		}
	} else if want := checkpoint.SnapshotNextSeq - 1; snapshot.LogSeq != want {
		return fmt.Errorf("%w: checkpoint snapshot log_seq=%d want=%d", ErrMaintenanceCommandRejected, snapshot.LogSeq, want)
	}
	if err := snapshot.ValidateLevels(); err != nil {
		return fmt.Errorf("%w: checkpoint snapshot topology: %v", ErrMaintenanceCommandRejected, err)
	}
	return nil
}

func validateCheckpointSnapshotFence(role string, fence *FenceToken, nextEpoch uint64) error {
	if fence == nil {
		return nil
	}
	if fence.Epoch == 0 || fence.Epoch >= nextEpoch || fence.Owner == "" || fence.ClaimedAt.IsZero() {
		return fmt.Errorf("%w: invalid checkpoint snapshot %s fence", ErrMaintenanceCommandRejected, role)
	}
	return nil
}

func (s *Store) appendWriterOwnedMaintenanceEntry(ctx context.Context, current *Current, entry *ManifestLogEntry) error {
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
	if err := validateRetiredObjects(entry); err != nil {
		return err
	}
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
