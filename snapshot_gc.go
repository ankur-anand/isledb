package isledb

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/manifest"
)

const (
	snapshotRetirementMarkerPrefix = "manifest/gc/snapshots"
	manifestSnapshotObjectPrefix   = "manifest/snapshots"
	snapshotRetirementMarkVersion  = 1

	defaultSnapshotDeleteBatchSize  = 128
	defaultSnapshotMarkerScanLimit  = 1024
	defaultSnapshotOrphanScanLimit  = 1024
	defaultSnapshotSweepInterval    = time.Minute
	defaultSnapshotOrphanAuditEvery = time.Hour
	defaultSnapshotSafetyMargin     = time.Minute
	snapshotMarkCASMaxRetries       = 8
)

type snapshotRetirementMark struct {
	Version int `json:"version"`

	Path         string `json:"path"`
	EncodedBytes uint64 `json:"encoded_bytes,omitempty"`
	Checksum     string `json:"checksum,omitempty"`

	Reason    string    `json:"reason"`
	RetiredAt time.Time `json:"retired_at"`
	NotBefore time.Time `json:"not_before"`
}

type snapshotCleanerOptions struct {
	DeleteBatchSize  int
	MarkerScanLimit  int
	OrphanScanLimit  int
	SweepInterval    time.Duration
	OrphanAuditEvery time.Duration
	SafetyMargin     time.Duration
	Now              func() time.Time
	Deleter          objectDeleter
}

type snapshotCleaner struct {
	store       *blobstore.Store
	manifestLog *manifest.Store
	opts        snapshotCleanerOptions
	delete      objectDeleter

	mu           sync.Mutex
	nextSweep    time.Time
	nextAudit    time.Time
	snapshotIter *blobstore.ListIterator
	markerIter   *blobstore.ListIterator
}

func defaultSnapshotCleanerOptions() snapshotCleanerOptions {
	return snapshotCleanerOptions{
		DeleteBatchSize:  defaultSnapshotDeleteBatchSize,
		MarkerScanLimit:  defaultSnapshotMarkerScanLimit,
		OrphanScanLimit:  defaultSnapshotOrphanScanLimit,
		SweepInterval:    defaultSnapshotSweepInterval,
		OrphanAuditEvery: defaultSnapshotOrphanAuditEvery,
		SafetyMargin:     defaultSnapshotSafetyMargin,
		Now:              func() time.Time { return time.Now().UTC() },
	}
}

func newSnapshotCleaner(store *blobstore.Store, manifestLog *manifest.Store, opts snapshotCleanerOptions) *snapshotCleaner {
	defaults := defaultSnapshotCleanerOptions()
	if opts.DeleteBatchSize <= 0 {
		opts.DeleteBatchSize = defaults.DeleteBatchSize
	}
	if opts.MarkerScanLimit <= 0 {
		opts.MarkerScanLimit = defaults.MarkerScanLimit
	}
	if opts.OrphanScanLimit <= 0 {
		opts.OrphanScanLimit = defaults.OrphanScanLimit
	}
	if opts.SweepInterval <= 0 {
		opts.SweepInterval = defaults.SweepInterval
	}
	if opts.OrphanAuditEvery <= 0 {
		opts.OrphanAuditEvery = defaults.OrphanAuditEvery
	}
	if opts.SafetyMargin < 0 {
		opts.SafetyMargin = 0
	} else if opts.SafetyMargin == 0 {
		opts.SafetyMargin = defaults.SafetyMargin
	}
	if opts.Now == nil {
		opts.Now = defaults.Now
	}
	deleter := opts.Deleter
	if deleter == nil {
		deleter = store
	}
	return &snapshotCleaner{store: store, manifestLog: manifestLog, opts: opts, delete: deleter}
}

func (c *snapshotCleaner) runOnce(ctx context.Context) (stats ManifestSnapshotCleanupStats, err error) {
	start := time.Now()
	defer func() { stats.Duration = time.Since(start) }()

	now := c.opts.Now().UTC()
	c.mu.Lock()
	doSweep := c.nextSweep.IsZero() || !now.Before(c.nextSweep)
	doAudit := c.nextAudit.IsZero() || !now.Before(c.nextAudit)
	if doSweep {
		c.nextSweep = now.Add(c.opts.SweepInterval)
	}
	if doAudit {
		c.nextAudit = now.Add(c.opts.OrphanAuditEvery)
	}
	c.mu.Unlock()

	if doAudit {
		audit, auditErr := c.discoverOrphans(ctx, now)
		mergeManifestSnapshotCleanupStats(&stats, audit)
		if auditErr != nil {
			stats.Failures++
			err = errors.Join(err, fmt.Errorf("audit manifest snapshots: %w", auditErr))
		}
	}
	if doSweep {
		sweep, sweepErr := c.sweep(ctx, now)
		mergeManifestSnapshotCleanupStats(&stats, sweep)
		if sweepErr != nil {
			stats.Failures++
			err = errors.Join(err, fmt.Errorf("sweep manifest snapshots: %w", sweepErr))
		}
	}
	return stats, err
}

func (c *snapshotCleaner) markCheckpointOutcome(
	ctx context.Context,
	current *manifest.Current,
	command *manifest.MaintenanceCommand,
	receipt *manifest.MaintenanceReceipt,
) (bool, error) {
	if current == nil || command == nil || command.Kind != manifest.MaintenanceCommandCheckpoint ||
		command.Checkpoint == nil || receipt == nil || !receipt.Matches(command) {
		return false, nil
	}
	observedAt := c.opts.Now().UTC()
	retiredAt := receipt.AppliedAt.UTC()
	if retiredAt.IsZero() || retiredAt.Before(observedAt) {
		// Writer and maintenance may run on hosts with skewed clocks. Starting
		// the grace period no earlier than local observation prevents an old
		// writer timestamp from shortening the pinned-view lifetime.
		retiredAt = observedAt
	}
	switch receipt.Status {
	case manifest.MaintenanceStatusApplied:
		if command.Checkpoint.BaseSnapshot == nil {
			return false, nil
		}
		return c.mark(ctx, *command.Checkpoint.BaseSnapshot, retiredAt, current.PinnedViewAge(), "checkpoint_replaced")
	case manifest.MaintenanceStatusRejected:
		return c.mark(ctx, command.Checkpoint.Snapshot, retiredAt, current.PinnedViewAge(), "checkpoint_rejected")
	default:
		return false, fmt.Errorf("unknown checkpoint receipt status %q", receipt.Status)
	}
}

func (c *snapshotCleaner) markAbandonedCandidate(
	ctx context.Context,
	current *manifest.Current,
	ref manifest.ObjectRef,
	reason string,
) (bool, error) {
	age := manifest.DefaultMaxPinnedViewAge
	if current != nil {
		age = current.PinnedViewAge()
	}
	return c.mark(ctx, ref, c.opts.Now().UTC(), age, reason)
}

func (c *snapshotCleaner) mark(
	ctx context.Context,
	ref manifest.ObjectRef,
	retiredAt time.Time,
	pinnedViewAge time.Duration,
	reason string,
) (bool, error) {
	return c.markWithExistingPolicy(ctx, ref, retiredAt, pinnedViewAge, reason, true)
}

func (c *snapshotCleaner) markWithExistingPolicy(
	ctx context.Context,
	ref manifest.ObjectRef,
	retiredAt time.Time,
	pinnedViewAge time.Duration,
	reason string,
	extendExisting bool,
) (bool, error) {
	if !validManifestSnapshotPath(c.store, ref.Path) {
		return false, fmt.Errorf("invalid snapshot retirement path %q", ref.Path)
	}
	if retiredAt.IsZero() {
		retiredAt = c.opts.Now().UTC()
	}
	if pinnedViewAge <= 0 {
		pinnedViewAge = manifest.DefaultMaxPinnedViewAge
	}
	mark := snapshotRetirementMark{
		Version:      snapshotRetirementMarkVersion,
		Path:         ref.Path,
		EncodedBytes: ref.EncodedBytes,
		Checksum:     ref.Checksum,
		Reason:       reason,
		RetiredAt:    retiredAt.UTC(),
		NotBefore:    retiredAt.UTC().Add(pinnedViewAge).Add(c.opts.SafetyMargin),
	}
	return c.writeSnapshotRetirementMark(ctx, mark, extendExisting)
}

func (c *snapshotCleaner) writeSnapshotRetirementMark(
	ctx context.Context,
	mark snapshotRetirementMark,
	extendExisting bool,
) (bool, error) {
	if err := validateSnapshotRetirementMark(c.store, mark); err != nil {
		return false, err
	}
	markerPath := snapshotRetirementMarkerPath(c.store, mark.Path)
	for attempt := 0; attempt < snapshotMarkCASMaxRetries; attempt++ {
		payload, err := json.Marshal(mark)
		if err != nil {
			return false, err
		}
		_, err = c.store.WriteIfNotExist(ctx, markerPath, payload)
		if err == nil {
			return true, nil
		}
		if !errors.Is(err, blobstore.ErrPreconditionFailed) {
			return false, err
		}

		data, matchToken, exists, err := readObjectWithCAS(ctx, c.store, markerPath)
		if err != nil {
			return false, fmt.Errorf("verify existing snapshot retirement marker: %w", err)
		}
		if !exists {
			continue
		}
		existing, err := decodeSnapshotRetirementMark(c.store, markerPath, data)
		if err != nil {
			return false, fmt.Errorf("verify existing snapshot retirement marker: %w", err)
		}
		if existing.Path != mark.Path {
			return false, fmt.Errorf("existing snapshot retirement marker targets %q, want %q", existing.Path, mark.Path)
		}
		if !extendExisting || !existing.NotBefore.Before(mark.NotBefore) {
			return false, nil
		}

		// A snapshot can first look abandoned and later become the published
		// base of another checkpoint (for example after an ambiguous stage
		// response). Extend the marker so the latest retirement always gets a
		// complete pinned-view grace period.
		if err := writeObjectCAS(ctx, c.store, markerPath, payload, matchToken, true); err != nil {
			if isGCMarkCASConflict(err) {
				continue
			}
			return false, err
		}
		return true, nil
	}
	return false, fmt.Errorf("update snapshot retirement marker after %d CAS retries: %w",
		snapshotMarkCASMaxRetries, blobstore.ErrPreconditionFailed)
}

func (c *snapshotCleaner) discoverOrphans(ctx context.Context, now time.Time) (ManifestSnapshotCleanupStats, error) {
	stats := ManifestSnapshotCleanupStats{}
	protected, current, err := c.protectedSnapshots(ctx)
	if err != nil {
		return stats, err
	}
	pinnedViewAge := manifest.DefaultMaxPinnedViewAge
	if current != nil {
		pinnedViewAge = current.PinnedViewAge()
	}

	c.mu.Lock()
	if c.snapshotIter == nil {
		c.snapshotIter = c.store.NewListIterator(blobstore.ListOptions{Prefix: manifestSnapshotObjectPrefix + "/"})
	}
	iter := c.snapshotIter
	c.mu.Unlock()

	for stats.ObjectsScanned < c.opts.OrphanScanLimit {
		object, err := iter.Next(ctx)
		if errors.Is(err, io.EOF) {
			c.mu.Lock()
			if c.snapshotIter == iter {
				c.snapshotIter = nil
			}
			c.mu.Unlock()
			return stats, nil
		}
		if err != nil {
			c.mu.Lock()
			if c.snapshotIter == iter {
				c.snapshotIter = nil
			}
			c.mu.Unlock()
			return stats, err
		}
		if object.IsDir {
			continue
		}
		stats.ObjectsScanned++
		if !validManifestSnapshotPath(c.store, object.Key) {
			continue
		}
		if _, live := protected[object.Key]; live {
			stats.Protected++
			continue
		}
		ref := manifest.ObjectRef{Path: object.Key}
		if object.Size > 0 {
			ref.EncodedBytes = uint64(object.Size)
		}
		// Receipt reconciliation normally creates the marker. The audit is a
		// crash-recovery fallback, so use a cheap existence check instead of a
		// failed create plus marker read for every already-retired snapshot.
		markerPath := snapshotRetirementMarkerPath(c.store, ref.Path)
		if _, err := c.store.Attributes(ctx, markerPath); err == nil {
			continue
		} else if !errors.Is(err, blobstore.ErrNotFound) {
			return stats, err
		}
		marked, err := c.markWithExistingPolicy(ctx, ref, now, pinnedViewAge, "orphan_audit", false)
		if err != nil {
			return stats, err
		}
		if marked {
			stats.SnapshotsMarked++
		}
	}
	return stats, nil
}

func (c *snapshotCleaner) sweep(ctx context.Context, now time.Time) (ManifestSnapshotCleanupStats, error) {
	stats := ManifestSnapshotCleanupStats{}
	protected, _, err := c.protectedSnapshots(ctx)
	if err != nil {
		return stats, err
	}

	c.mu.Lock()
	if c.markerIter == nil {
		c.markerIter = c.store.NewListIterator(blobstore.ListOptions{Prefix: snapshotRetirementMarkerPrefix + "/"})
	}
	iter := c.markerIter
	c.mu.Unlock()

	for stats.MarkersScanned < c.opts.MarkerScanLimit && stats.DeleteAttempts < c.opts.DeleteBatchSize {
		object, err := iter.Next(ctx)
		if errors.Is(err, io.EOF) {
			c.mu.Lock()
			if c.markerIter == iter {
				c.markerIter = nil
			}
			c.mu.Unlock()
			return stats, nil
		}
		if err != nil {
			c.mu.Lock()
			if c.markerIter == iter {
				c.markerIter = nil
			}
			c.mu.Unlock()
			return stats, err
		}
		if object.IsDir {
			continue
		}
		stats.MarkersScanned++
		mark, err := c.readMark(ctx, object.Key)
		if err != nil {
			stats.Failures++
			continue
		}
		if _, live := protected[mark.Path]; live {
			stats.Protected++
			if err := c.delete.Delete(ctx, object.Key); err != nil {
				stats.Failures++
				continue
			}
			stats.MarkersCleared++
			continue
		}
		if now.Before(mark.NotBefore) {
			stats.Deferred++
			continue
		}

		stats.DeleteAttempts++
		if err := c.delete.Delete(ctx, mark.Path); err != nil {
			stats.Failures++
			continue
		}
		if err := c.delete.Delete(ctx, object.Key); err != nil {
			stats.Failures++
			continue
		}
		stats.SnapshotsDeleted++
		stats.MarkersCleared++
	}
	return stats, nil
}

func (c *snapshotCleaner) readMark(ctx context.Context, markerPath string) (snapshotRetirementMark, error) {
	data, _, err := c.store.Read(ctx, markerPath)
	if err != nil {
		return snapshotRetirementMark{}, err
	}
	return decodeSnapshotRetirementMark(c.store, markerPath, data)
}

func decodeSnapshotRetirementMark(store *blobstore.Store, markerPath string, data []byte) (snapshotRetirementMark, error) {
	var mark snapshotRetirementMark
	if err := json.Unmarshal(data, &mark); err != nil {
		return snapshotRetirementMark{}, fmt.Errorf("decode snapshot retirement marker %q: %w", markerPath, err)
	}
	if err := validateSnapshotRetirementMark(store, mark); err != nil {
		return snapshotRetirementMark{}, fmt.Errorf("snapshot retirement marker %q: %w", markerPath, err)
	}
	if markerPath != snapshotRetirementMarkerPath(store, mark.Path) {
		return snapshotRetirementMark{}, fmt.Errorf("snapshot retirement marker path mismatch %q", markerPath)
	}
	return mark, nil
}

func (c *snapshotCleaner) protectedSnapshots(ctx context.Context) (map[string]struct{}, *manifest.Current, error) {
	// Read HEAD first. If checkpoint application races these reads, either HEAD
	// still protects both candidate and base, or the later CURRENT protects the
	// newly published candidate.
	head, _, err := c.manifestLog.ReadMaintenanceHead(ctx)
	if err != nil {
		return nil, nil, err
	}
	current, err := c.manifestLog.ReadCurrentData(ctx)
	if err != nil {
		return nil, nil, err
	}
	protected := make(map[string]struct{}, 3)
	if current != nil && current.Snapshot != nil && current.Snapshot.Path != "" {
		protected[current.Snapshot.Path] = struct{}{}
	}
	if head != nil && head.Pending != nil && head.Pending.Checkpoint != nil {
		checkpoint := head.Pending.Checkpoint
		if checkpoint.Snapshot.Path != "" {
			protected[checkpoint.Snapshot.Path] = struct{}{}
		}
		if checkpoint.BaseSnapshot != nil && checkpoint.BaseSnapshot.Path != "" {
			protected[checkpoint.BaseSnapshot.Path] = struct{}{}
		}
	}
	return protected, current, nil
}

func snapshotRetirementMarkerPath(store *blobstore.Store, snapshotPath string) string {
	digest := sha256.Sum256([]byte(snapshotPath))
	return storeKey(store, snapshotRetirementMarkerPrefix, hex.EncodeToString(digest[:])+".json")
}

func validManifestSnapshotPath(store *blobstore.Store, objectPath string) bool {
	prefix := storeKey(store, manifestSnapshotObjectPrefix) + "/"
	if !strings.HasPrefix(objectPath, prefix) || !strings.HasSuffix(objectPath, ".manifest.zst") {
		return false
	}
	relative := strings.TrimPrefix(objectPath, prefix)
	return relative != "" && !strings.Contains(relative, "/")
}

func validateSnapshotRetirementMark(store *blobstore.Store, mark snapshotRetirementMark) error {
	if mark.Version != snapshotRetirementMarkVersion {
		return fmt.Errorf("unsupported version=%d", mark.Version)
	}
	if !validManifestSnapshotPath(store, mark.Path) {
		return fmt.Errorf("invalid snapshot path %q", mark.Path)
	}
	if mark.Reason == "" || mark.RetiredAt.IsZero() || mark.NotBefore.IsZero() {
		return errors.New("incomplete snapshot retirement marker")
	}
	if mark.NotBefore.Before(mark.RetiredAt) {
		return errors.New("snapshot retirement marker not_before precedes retired_at")
	}
	return nil
}

func mergeManifestSnapshotCleanupStats(dst *ManifestSnapshotCleanupStats, src ManifestSnapshotCleanupStats) {
	if dst == nil {
		return
	}
	dst.SnapshotsMarked += src.SnapshotsMarked
	dst.DeleteAttempts += src.DeleteAttempts
	dst.SnapshotsDeleted += src.SnapshotsDeleted
	dst.Protected += src.Protected
	dst.Deferred += src.Deferred
	dst.Failures += src.Failures
	dst.MarkersScanned += src.MarkersScanned
	dst.MarkersCleared += src.MarkersCleared
	dst.ObjectsScanned += src.ObjectsScanned
	dst.Duration += src.Duration
}
