package isledb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/manifest"
)

const (
	manifestPageObjectPrefix        = "manifest/pages"
	manifestPageRetirementPrefix    = "manifest/gc/page-marks"
	manifestPageRetirementVersion   = 1
	defaultManifestPageScanLimit    = 1024
	defaultManifestPageMarkerLimit  = 1024
	defaultManifestPageOrphanGrace  = 24 * time.Hour
	defaultManifestPageSafetyMargin = time.Minute
	manifestPageRetirementMaxBytes  = 16 << 10
)

type manifestPageRetirementMark struct {
	Version int              `json:"version"`
	Page    manifest.PageRef `json:"page"`

	ObservedAt    time.Time     `json:"observed_at"`
	PinnedViewAge time.Duration `json:"pinned_view_age_nanos"`
	SafetyMargin  time.Duration `json:"safety_margin_nanos"`
	OrphanGrace   time.Duration `json:"orphan_grace_nanos"`
	NotBefore     time.Time     `json:"not_before"`
	ObservedFloor uint64        `json:"observed_floor"`
	Reason        string        `json:"reason"`
}

type manifestPageCleanerOptions struct {
	DeleteBatchSize  int
	PlanScanLimit    int
	PageScanLimit    int
	MarkerScanLimit  int
	SweepInterval    time.Duration
	OrphanAuditEvery time.Duration
	OrphanGrace      time.Duration
	SafetyMargin     time.Duration
	Now              func() time.Time
	Deleter          objectDeleter
}

type manifestPageCleaner struct {
	store       *blobstore.Store
	manifestLog *manifest.Store
	opts        manifestPageCleanerOptions
	delete      objectDeleter

	mu         sync.Mutex
	nextSweep  time.Time
	nextAudit  time.Time
	pageIter   *blobstore.ListIterator
	markerIter *blobstore.ListIterator

	// Plan reclaim state is owned by the serialized manifest reclamation lane.
	planIter        *blobstore.ListIterator
	pendingPlanKey  string
	activeReadyKey  string
	activePlan      *manifestPageDeletionPlan
	activePageLevel int
	activePageIter  *blobstore.ListIterator
	planCache       *boundedPlanCache[manifestPageDeletionPlan]
	seenRescan      uint64
	rescan          atomic.Uint64
}

func defaultManifestPageCleanerOptions() manifestPageCleanerOptions {
	return manifestPageCleanerOptions{
		DeleteBatchSize:  defaultSnapshotDeleteBatchSize,
		PlanScanLimit:    defaultManifestPagePlanScanLimit,
		PageScanLimit:    defaultManifestPageScanLimit,
		MarkerScanLimit:  defaultManifestPageMarkerLimit,
		SweepInterval:    defaultSnapshotSweepInterval,
		OrphanAuditEvery: defaultSnapshotOrphanAuditEvery,
		OrphanGrace:      defaultManifestPageOrphanGrace,
		SafetyMargin:     defaultManifestPageSafetyMargin,
		Now:              func() time.Time { return time.Now().UTC() },
	}
}

func newManifestPageCleaner(store *blobstore.Store, manifestLog *manifest.Store, opts manifestPageCleanerOptions) *manifestPageCleaner {
	defaults := defaultManifestPageCleanerOptions()
	if opts.DeleteBatchSize <= 0 {
		opts.DeleteBatchSize = defaults.DeleteBatchSize
	}
	if opts.PlanScanLimit <= 0 {
		opts.PlanScanLimit = defaults.PlanScanLimit
	}
	if opts.PageScanLimit <= 0 {
		opts.PageScanLimit = defaults.PageScanLimit
	}
	if opts.MarkerScanLimit <= 0 {
		opts.MarkerScanLimit = defaults.MarkerScanLimit
	}
	if opts.SweepInterval <= 0 {
		opts.SweepInterval = defaults.SweepInterval
	}
	if opts.OrphanAuditEvery <= 0 {
		opts.OrphanAuditEvery = defaults.OrphanAuditEvery
	}
	if opts.OrphanGrace < 0 {
		opts.OrphanGrace = 0
	} else if opts.OrphanGrace == 0 {
		opts.OrphanGrace = defaults.OrphanGrace
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
	return &manifestPageCleaner{
		store: store, manifestLog: manifestLog, opts: opts, delete: deleter,
		planCache: newDeletionPlanCache[manifestPageDeletionPlan](),
	}
}

func (c *manifestPageCleaner) runOnce(ctx context.Context) (stats ManifestPageCleanupStats, err error) {
	stats, _, err = c.runScheduledOnce(ctx)
	return stats, err
}

func (c *manifestPageCleaner) runScheduledOnce(
	ctx context.Context,
) (stats ManifestPageCleanupStats, schedule reclamationLaneSchedule, err error) {
	now := c.opts.Now().UTC()
	stats, err = c.runOnceAt(ctx, now)

	c.mu.Lock()
	nextDue := c.nextAudit
	pageIteratorActive := c.pageIter != nil
	markerIteratorActive := c.markerIter != nil
	c.mu.Unlock()
	if c.pendingPlanKey != "" {
		if deadline, _, parseErr := parseDeletionPlanReadyName(c.pendingPlanKey); parseErr == nil {
			nextDue = earlierReclamationDeadline(nextDue, deadline)
		}
	}
	planIteratorActive := c.activePlan != nil || (c.planIter != nil && c.pendingPlanKey == "")
	schedule = reclamationLaneSchedule{
		observedAt: now,
		nextDue:    nextDue,
		idle:       err == nil && !pageIteratorActive && !markerIteratorActive && !planIteratorActive,
	}
	return stats, schedule, err
}

func (c *manifestPageCleaner) runOnceAt(
	ctx context.Context,
	now time.Time,
) (stats ManifestPageCleanupStats, err error) {
	start := time.Now()
	defer func() { stats.Duration = time.Since(start) }()
	if err := checkContext(ctx); err != nil {
		return stats, err
	}

	plans, planErr := c.reclaimPlans(ctx, now)
	mergeManifestPageCleanupStats(&stats, plans)
	if planErr != nil {
		if cancelErr := reclamationCancellation(ctx, planErr); cancelErr != nil {
			return stats, fmt.Errorf("reclaim manifest page plans: %w", cancelErr)
		}
		stats.Failures++
		err = errors.Join(err, fmt.Errorf("reclaim manifest page plans: %w", planErr))
	}
	c.mu.Lock()
	doSweep := c.nextSweep.IsZero() || !now.Before(c.nextSweep)
	doAudit := c.nextAudit.IsZero() || !now.Before(c.nextAudit)
	if doSweep {
		c.nextSweep = now.Add(c.opts.SweepInterval)
	}
	if doAudit {
		// The audit is the fallback for malformed, skipped, or out-of-range page
		// objects. Its liveness must not depend on the primary range-plan queue
		// becoming empty: a continuous stream of future plans, or one stuck plan,
		// must not suppress fallback discovery indefinitely.
		c.nextAudit = now.Add(c.opts.OrphanAuditEvery)
	}
	c.mu.Unlock()

	if doAudit {
		audit, auditErr := c.discover(ctx, now)
		mergeManifestPageCleanupStats(&stats, audit)
		if auditErr != nil {
			if cancelErr := reclamationCancellation(ctx, auditErr); cancelErr != nil {
				return stats, fmt.Errorf("audit manifest pages: %w", cancelErr)
			}
			stats.Failures++
			err = errors.Join(err, fmt.Errorf("audit manifest pages: %w", auditErr))
		}
	}
	if doSweep {
		sweep, sweepErr := c.sweep(ctx, now)
		mergeManifestPageCleanupStats(&stats, sweep)
		if sweepErr != nil {
			if cancelErr := reclamationCancellation(ctx, sweepErr); cancelErr != nil {
				return stats, fmt.Errorf("sweep manifest pages: %w", cancelErr)
			}
			stats.Failures++
			err = errors.Join(err, fmt.Errorf("sweep manifest pages: %w", sweepErr))
		}
	}
	return stats, err
}

func (c *manifestPageCleaner) discover(ctx context.Context, now time.Time) (ManifestPageCleanupStats, error) {
	stats := ManifestPageCleanupStats{}
	current, err := c.manifestLog.ReadCurrentData(ctx)
	if err != nil {
		return stats, err
	}

	c.mu.Lock()
	if c.pageIter == nil {
		c.pageIter = c.store.NewListIterator(blobstore.ListOptions{Prefix: manifestPageObjectPrefix + "/"})
	}
	iter := c.pageIter
	c.mu.Unlock()

	floor, floorKnown := manifestPageRetentionFloor(current)
	for stats.ObjectsScanned < c.opts.PageScanLimit {
		object, err := iter.Next(ctx)
		if errors.Is(err, io.EOF) {
			c.mu.Lock()
			if c.pageIter == iter {
				c.pageIter = nil
			}
			c.mu.Unlock()
			return stats, nil
		}
		if err != nil {
			c.resetPageIterator(iter)
			return stats, err
		}
		if object.IsDir {
			continue
		}
		stats.ObjectsScanned++
		hint, ok := manifestPageKeyHintFromPath(c.store, object.Key)
		if !ok {
			stats.Failures++
			continue
		}
		if floorKnown && hint.HasRange && hint.SeqHi >= floor {
			// The filename is only a negative filter. A key claiming that a page
			// is still protected can at worst retain garbage; it can never
			// authorize deletion.
			stats.Protected++
			continue
		}
		if floorKnown && hint.HasRange {
			markerPath := manifestPageRetirementMarkerPath(c.store, object.Key)
			if _, err := c.store.Attributes(ctx, markerPath); err == nil {
				// A prior audit already downloaded and proved this candidate. The
				// sweeper owns the fresh-CURRENT check before physical deletion.
				stats.Protected++
				continue
			} else if !errors.Is(err, blobstore.ErrNotFound) {
				if cancelErr := reclamationCancellation(ctx, err); cancelErr != nil {
					c.resetPageIterator(iter)
					return stats, cancelErr
				}
				stats.Failures++
				continue
			}
		}
		data, _, err := c.store.Read(ctx, object.Key)
		if err != nil {
			if cancelErr := reclamationCancellation(ctx, err); cancelErr != nil {
				c.resetPageIterator(iter)
				return stats, cancelErr
			}
			stats.Failures++
			continue
		}
		candidate, _, err := manifest.InspectCommitPage(object.Key, data)
		if err != nil || candidate.Level != hint.Level ||
			(hint.HasRange && (candidate.SeqLo != hint.SeqLo || candidate.SeqHi != hint.SeqHi)) {
			stats.Failures++
			continue
		}
		reachable, reads, err := c.manifestLog.IsPageReachable(ctx, current, candidate)
		stats.ReachabilityGETs += reads
		if err != nil {
			if cancelErr := reclamationCancellation(ctx, err); cancelErr != nil {
				c.resetPageIterator(iter)
				return stats, cancelErr
			}
			stats.Failures++
			continue
		}
		if reachable {
			stats.Protected++
			continue
		}
		safe, floor := manifestPageDeletionProven(current, candidate)
		if !safe {
			// Without a committed floor beyond the complete page range, the
			// page can belong to a paused publication. Uncertainty leaks.
			stats.Protected++
			continue
		}
		marked, err := c.mark(ctx, candidate, current, floor, now)
		if err != nil {
			if cancelErr := reclamationCancellation(ctx, err); cancelErr != nil {
				c.resetPageIterator(iter)
				return stats, cancelErr
			}
			stats.Failures++
			continue
		}
		if marked {
			stats.PagesMarked++
		}
	}
	return stats, nil
}

func (c *manifestPageCleaner) sweep(ctx context.Context, now time.Time) (ManifestPageCleanupStats, error) {
	stats := ManifestPageCleanupStats{}
	c.mu.Lock()
	if c.markerIter == nil {
		c.markerIter = c.store.NewListIterator(blobstore.ListOptions{Prefix: manifestPageRetirementPrefix + "/"})
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
			c.resetMarkerIterator(iter)
			return stats, err
		}
		if object.IsDir {
			continue
		}
		stats.MarkersScanned++
		mark, err := c.readMark(ctx, object.Key)
		if err != nil {
			if cancelErr := reclamationCancellation(ctx, err); cancelErr != nil {
				c.resetMarkerIterator(iter)
				return stats, cancelErr
			}
			stats.Failures++
			continue
		}
		if now.Before(mark.NotBefore) {
			stats.Deferred++
			continue
		}

		// Reload CURRENT for every due deletion. An object marked unreachable
		// can have become visible before its publishing CAS completed.
		current, err := c.manifestLog.ReadCurrentData(ctx)
		if err != nil {
			if cancelErr := reclamationCancellation(ctx, err); cancelErr != nil {
				c.resetMarkerIterator(iter)
				return stats, cancelErr
			}
			stats.Failures++
			continue
		}
		reachable, reads, err := c.manifestLog.IsPageReachable(ctx, current, mark.Page)
		stats.ReachabilityGETs += reads
		if err != nil {
			if cancelErr := reclamationCancellation(ctx, err); cancelErr != nil {
				c.resetMarkerIterator(iter)
				return stats, cancelErr
			}
			stats.Failures++
			continue
		}
		if reachable {
			stats.Protected++
			if err := c.delete.Delete(ctx, object.Key); err != nil {
				if cancelErr := reclamationCancellation(ctx, err); cancelErr != nil {
					c.resetMarkerIterator(iter)
					return stats, cancelErr
				}
				stats.Failures++
				continue
			}
			stats.MarkersCleared++
			continue
		}
		safe, _ := manifestPageDeletionProven(current, mark.Page)
		if !safe {
			stats.Deferred++
			continue
		}

		data, _, err := c.store.Read(ctx, mark.Page.Path)
		if err != nil {
			if errors.Is(err, blobstore.ErrNotFound) {
				if err := c.delete.Delete(ctx, object.Key); err != nil {
					if cancelErr := reclamationCancellation(ctx, err); cancelErr != nil {
						c.resetMarkerIterator(iter)
						return stats, cancelErr
					}
					stats.Failures++
					continue
				}
				stats.MarkersCleared++
				continue
			}
			if cancelErr := reclamationCancellation(ctx, err); cancelErr != nil {
				c.resetMarkerIterator(iter)
				return stats, cancelErr
			}
			stats.Failures++
			continue
		}
		ref, _, err := manifest.InspectCommitPage(mark.Page.Path, data)
		if err != nil || ref != mark.Page {
			stats.Failures++
			continue
		}

		stats.DeleteAttempts++
		if err := c.delete.Delete(ctx, mark.Page.Path); err != nil {
			if cancelErr := reclamationCancellation(ctx, err); cancelErr != nil {
				c.resetMarkerIterator(iter)
				return stats, cancelErr
			}
			stats.Failures++
			continue
		}
		if err := c.delete.Delete(ctx, object.Key); err != nil {
			if cancelErr := reclamationCancellation(ctx, err); cancelErr != nil {
				c.resetMarkerIterator(iter)
				return stats, cancelErr
			}
			stats.Failures++
			continue
		}
		stats.PagesDeleted++
		stats.MarkersCleared++
	}
	return stats, nil
}

func (c *manifestPageCleaner) mark(
	ctx context.Context,
	page manifest.PageRef,
	current *manifest.Current,
	floor uint64,
	now time.Time,
) (bool, error) {
	pinnedAge := manifest.DefaultMaxPinnedViewAge
	if current != nil {
		pinnedAge = current.PinnedViewAge()
	}
	notBefore := now.Add(pinnedAge).Add(c.opts.SafetyMargin)
	if orphanDeadline := page.CreatedAt.Add(c.opts.OrphanGrace); orphanDeadline.After(notBefore) {
		notBefore = orphanDeadline
	}
	mark := manifestPageRetirementMark{
		Version:       manifestPageRetirementVersion,
		Page:          page,
		ObservedAt:    now,
		PinnedViewAge: pinnedAge,
		SafetyMargin:  c.opts.SafetyMargin,
		OrphanGrace:   c.opts.OrphanGrace,
		NotBefore:     notBefore,
		ObservedFloor: floor,
		Reason:        "below_retained_floor",
	}
	if err := validateManifestPageRetirementMark(c.store, mark); err != nil {
		return false, err
	}
	payload, err := json.Marshal(mark)
	if err != nil {
		return false, err
	}
	path := manifestPageRetirementMarkerPath(c.store, page.Path)
	if _, err := c.store.WriteIfNotExist(ctx, path, payload); err == nil {
		return true, nil
	} else if !errors.Is(err, blobstore.ErrPreconditionFailed) {
		return false, err
	}
	existing, err := c.readMark(ctx, path)
	if err != nil {
		return false, err
	}
	if existing.Page != page {
		return false, fmt.Errorf("manifest page retirement marker collision path=%q", path)
	}
	return false, nil
}

func (c *manifestPageCleaner) readMark(ctx context.Context, path string) (manifestPageRetirementMark, error) {
	data, _, err := c.store.Read(ctx, path)
	if err != nil {
		return manifestPageRetirementMark{}, err
	}
	return decodeManifestPageRetirementMark(c.store, path, data)
}

func decodeManifestPageRetirementMark(store *blobstore.Store, markerPath string, data []byte) (manifestPageRetirementMark, error) {
	if len(data) == 0 || len(data) > manifestPageRetirementMaxBytes {
		return manifestPageRetirementMark{}, fmt.Errorf("invalid manifest page marker bytes=%d", len(data))
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var mark manifestPageRetirementMark
	if err := decoder.Decode(&mark); err != nil {
		return manifestPageRetirementMark{}, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return manifestPageRetirementMark{}, errors.New("manifest page marker has trailing JSON")
		}
		return manifestPageRetirementMark{}, err
	}
	if err := validateManifestPageRetirementMark(store, mark); err != nil {
		return manifestPageRetirementMark{}, err
	}
	if markerPath != manifestPageRetirementMarkerPath(store, mark.Page.Path) {
		return manifestPageRetirementMark{}, fmt.Errorf("manifest page marker path mismatch %q", markerPath)
	}
	return mark, nil
}

func validateManifestPageRetirementMark(store *blobstore.Store, mark manifestPageRetirementMark) error {
	if mark.Version != manifestPageRetirementVersion {
		return fmt.Errorf("unsupported manifest page marker version=%d", mark.Version)
	}
	level, ok := manifestPagePathLevel(store, mark.Page.Path)
	if !ok || level != mark.Page.Level {
		return fmt.Errorf("invalid manifest page path=%q level=%d", mark.Page.Path, mark.Page.Level)
	}
	if err := manifest.ValidatePageRef(mark.Page); err != nil {
		return err
	}
	if mark.ObservedAt.IsZero() || mark.PinnedViewAge <= 0 || mark.SafetyMargin < 0 || mark.OrphanGrace < 0 ||
		mark.NotBefore.IsZero() || mark.Reason != "below_retained_floor" || mark.ObservedFloor <= mark.Page.SeqHi {
		return errors.New("incomplete manifest page marker timing")
	}
	wantNotBefore := mark.ObservedAt.Add(mark.PinnedViewAge).Add(mark.SafetyMargin)
	if orphanDeadline := mark.Page.CreatedAt.Add(mark.OrphanGrace); orphanDeadline.After(wantNotBefore) {
		wantNotBefore = orphanDeadline
	}
	if !mark.NotBefore.Equal(wantNotBefore) {
		return errors.New("manifest page marker deadline mismatch")
	}
	return nil
}

func manifestPageDeletionProven(current *manifest.Current, page manifest.PageRef) (bool, uint64) {
	floor, ok := manifestPageRetentionFloor(current)
	if !ok {
		return false, 0
	}
	if floor > page.SeqHi {
		return true, floor
	}
	return false, floor
}

func manifestPageRetirementMarkerPath(store *blobstore.Store, pagePath string) string {
	digest := sha256.Sum256([]byte(pagePath))
	return storeKey(store, manifestPageRetirementPrefix, hex.EncodeToString(digest[:])+".json")
}

func manifestPagePathLevel(store *blobstore.Store, objectPath string) (uint8, bool) {
	prefix := storeKey(store, manifestPageObjectPrefix) + "/"
	if !strings.HasPrefix(objectPath, prefix) || !strings.HasSuffix(objectPath, ".page.zst") {
		return 0, false
	}
	relative := strings.TrimPrefix(objectPath, prefix)
	parts := strings.Split(relative, "/")
	if len(parts) != 2 || len(parts[0]) != 3 || parts[0][0] != 'l' || len(parts[1]) <= len(".page.zst") {
		return 0, false
	}
	level, err := strconv.ParseUint(parts[0][1:], 10, 8)
	if err != nil || fmt.Sprintf("l%02d", level) != parts[0] {
		return 0, false
	}
	return uint8(level), true
}

const manifestPageKeySequenceDigits = 20

type manifestPageKeyHint struct {
	Level    uint8
	SeqLo    uint64
	SeqHi    uint64
	HasRange bool
}

// manifestPageKeyHintFromPath returns structural path information plus an
// optional, untrusted sequence-range hint. Missing or legacy range encodings
// deliberately fall back to the old full-object inspection path.
func manifestPageKeyHintFromPath(store *blobstore.Store, objectPath string) (manifestPageKeyHint, bool) {
	level, ok := manifestPagePathLevel(store, objectPath)
	if !ok {
		return manifestPageKeyHint{}, false
	}
	hint := manifestPageKeyHint{Level: level}
	name := strings.TrimSuffix(path.Base(objectPath), ".page.zst")
	const rangePrefixBytes = 1 + manifestPageKeySequenceDigits + 2 + manifestPageKeySequenceDigits + 1
	if len(name) <= rangePrefixBytes || name[0] != 'h' ||
		name[1+manifestPageKeySequenceDigits:1+manifestPageKeySequenceDigits+2] != "-l" ||
		name[rangePrefixBytes-1] != '-' {
		return hint, true
	}
	seqHiText := name[1 : 1+manifestPageKeySequenceDigits]
	seqLoText := name[1+manifestPageKeySequenceDigits+2 : rangePrefixBytes-1]
	seqHi, err := strconv.ParseUint(seqHiText, 10, 64)
	if err != nil || fmt.Sprintf("%020d", seqHi) != seqHiText {
		return hint, true
	}
	seqLo, err := strconv.ParseUint(seqLoText, 10, 64)
	if err != nil || fmt.Sprintf("%020d", seqLo) != seqLoText || seqHi < seqLo {
		return hint, true
	}
	hint.SeqLo = seqLo
	hint.SeqHi = seqHi
	hint.HasRange = true
	return hint, true
}

func manifestPageRetentionFloor(current *manifest.Current) (uint64, bool) {
	if current == nil {
		return 0, false
	}
	floor := current.LogSeqStart
	if current.ChangeFeedLogStart < floor {
		floor = current.ChangeFeedLogStart
	}
	return floor, floor > 0
}

func (c *manifestPageCleaner) resetPageIterator(iter *blobstore.ListIterator) {
	c.mu.Lock()
	if c.pageIter == iter {
		c.pageIter = nil
	}
	c.mu.Unlock()
}

func (c *manifestPageCleaner) resetMarkerIterator(iter *blobstore.ListIterator) {
	c.mu.Lock()
	if c.markerIter == iter {
		c.markerIter = nil
	}
	c.mu.Unlock()
}

func mergeManifestPageCleanupStats(dst *ManifestPageCleanupStats, src ManifestPageCleanupStats) {
	if dst == nil {
		return
	}
	dst.PlansPrepared += src.PlansPrepared
	dst.PlansScanned += src.PlansScanned
	dst.PlansCompleted += src.PlansCompleted
	dst.PagesMarked += src.PagesMarked
	dst.PagesDeleted += src.PagesDeleted
	dst.Protected += src.Protected
	dst.Deferred += src.Deferred
	dst.Failures += src.Failures
	dst.MarkersScanned += src.MarkersScanned
	dst.MarkersCleared += src.MarkersCleared
	dst.ObjectsScanned += src.ObjectsScanned
	dst.DeleteAttempts += src.DeleteAttempts
	dst.ReachabilityGETs += src.ReachabilityGETs
	dst.Duration += src.Duration
}
