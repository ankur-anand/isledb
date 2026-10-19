package isledb

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal"
	"github.com/ankur-anand/isledb/internal/manifest"
	"github.com/cockroachdb/pebble/v2/sstable"
)

// A compaction is published in one manifest entry. The manifest format bounds
// both removed and added objects, so this is a format invariant rather than a
// tuning knob.
const maxCompactionSSTsPerJob = manifest.MaxRetiredObjectsPerEntry

// maxCompactionOutputOvershoot is how far past its target a compaction output
// can run on its last entry: the largest value a writer accepts by default.
const maxCompactionOutputOvershoot = defaultMaxValueBytes

var errCompactorClosed = errors.New("compactor closed")

type compactionJob struct {
	// ReadBytes is what the job pulled from the object store: the inputs of a
	// rewrite, the verified sources of a checked move, and nothing at all for
	// an unchecked move.
	ReadBytes int64

	DestinationLevel    uint32
	InputSSTs           []string
	OutputSSTs          []compactionOutput
	MetadataOnly        bool
	ForcedLevelCreation bool
}

// compactionOutput describes one SST produced or repositioned by a compaction.
type compactionOutput struct {
	ID    string
	Bytes int64
	Level uint32
}

// compactor moves and rewrites SSTs through non-overlapping levels.
type compactor struct {
	store        *blobstore.Store
	manifestLog  *manifest.Store
	opts         compactorOptions
	stageCommand maintenanceCommandStager

	mu       sync.Mutex
	manifest *manifestState

	lifecycleMu sync.Mutex
	activeRuns  sync.WaitGroup
	runGate     chan struct{}

	fenced     atomic.Bool
	fenceToken *manifest.FenceToken
	scratch    *compactionScratchWorkspace

	closed atomic.Bool
}

func newCompactor(ctx context.Context, store *blobstore.Store, manifestLog *manifest.Store, opts compactorOptions) (*compactor, error) {
	return newCompactorWithFence(ctx, store, manifestLog, opts, nil)
}

func newCompactorWithFence(ctx context.Context, store *blobstore.Store, manifestLog *manifest.Store, opts compactorOptions, fence *manifest.FenceToken) (*compactor, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	opts = normalizeCompactorOptions(opts)

	m, err := manifestLog.Replay(ctx)
	if err != nil {
		return nil, fmt.Errorf("replay manifest: %w", err)
	}

	c := &compactor{
		store:       store,
		manifestLog: manifestLog,
		opts:        opts,
		manifest:    m,
		runGate:     make(chan struct{}, 1),
	}

	scratchOwner := compactionScratchMaintenance
	if fence == nil {
		scratchOwner = compactionScratchStandalone
		ownerID := opts.OwnerID
		if ownerID == "" {
			ownerID = fmt.Sprintf("compactor-%d-%d", time.Now().UnixNano(), m.NextEpoch)
		}
		token, err := manifestLog.ClaimCompactor(ctx, ownerID)
		if err != nil {
			return nil, fmt.Errorf("claim compactor fence: %w", err)
		}
		fence = token
	}
	token := *fence
	c.fenceToken = &token
	scratch, err := openCompactionScratchWorkspace(
		opts.ScratchDir, store.ScratchNamespace(), scratchOwner, token.Epoch)
	if err != nil {
		return nil, fmt.Errorf("open compaction scratch: %w", err)
	}
	c.scratch = scratch
	c.opts.ScratchDir = scratch.Path()

	return c, nil
}

func normalizeCompactorOptions(opts compactorOptions) compactorOptions {
	d := defaultCompactorOptions()
	if opts.InputReadParallelism <= 0 {
		opts.InputReadParallelism = d.InputReadParallelism
	}
	if opts.Trigger.L0SSTCount <= 0 {
		opts.Trigger.L0SSTCount = d.Trigger.L0SSTCount
	}
	if opts.Trigger.BaseLevelBytes <= 0 {
		opts.Trigger.BaseLevelBytes = d.Trigger.BaseLevelBytes
	}
	if opts.Trigger.LevelSizeMultiplier < 2 {
		opts.Trigger.LevelSizeMultiplier = d.Trigger.LevelSizeMultiplier
	}
	if opts.Trigger.L0ToBaseRatio <= 0 {
		opts.Trigger.L0ToBaseRatio = d.Trigger.L0ToBaseRatio
	}
	if opts.Trigger.L0MaxDepth <= 0 {
		opts.Trigger.L0MaxDepth = d.Trigger.L0MaxDepth
	}
	if opts.Trigger.L0MaxAge <= 0 {
		opts.Trigger.L0MaxAge = d.Trigger.L0MaxAge
	}
	if opts.Output.BloomBitsPerKey == 0 {
		opts.Output.BloomBitsPerKey = d.Output.BloomBitsPerKey
	}
	if opts.Output.BlockBytes == 0 {
		opts.Output.BlockBytes = d.Output.BlockBytes
	}
	opts.Output.Compression = cmp.Or(opts.Output.Compression, d.Output.Compression)
	if opts.Output.TargetSSTBytes <= 0 {
		opts.Output.TargetSSTBytes = d.Output.TargetSSTBytes
	}
	return opts
}

func (c *compactor) Close(ctx context.Context) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	c.lifecycleMu.Lock()
	c.closed.Store(true)
	c.lifecycleMu.Unlock()
	if err := waitGroupContext(ctx, &c.activeRuns); err != nil {
		return err
	}
	return c.scratch.Close()
}

func (c *compactor) closeDB() error {
	return c.closeWithTimeout(30 * time.Second)
}

func (c *compactor) closeWithTimeout(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return c.Close(ctx)
}

func (c *compactor) refreshWithCurrent(ctx context.Context) (*manifest.Current, error) {
	m, current, err := c.manifestLog.ReplayWithCurrent(ctx)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.manifest = m
	c.mu.Unlock()
	return current, nil
}

// runSelected executes at most one compaction chosen from all currently
// executable level plans. It is used by Maintenance so checkpoint arbitration
// happens before any expensive compaction work begins.
func (c *compactor) runSelected(
	ctx context.Context,
	selector func(*manifest.Current, []compactionCandidate) *compactionCandidate,
) (*compactionCandidate, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if err := c.beginRun(ctx); err != nil {
		return nil, err
	}
	defer c.finishRun()

	if c.fenced.Load() {
		return nil, manifest.ErrFenced
	}
	current, err := c.refreshWithCurrent(ctx)
	if err != nil {
		return nil, fmt.Errorf("refresh manifest: %w", err)
	}
	c.mu.Lock()
	m := c.manifest.Clone()
	c.mu.Unlock()
	candidates, err := c.planCompactionCandidates(m)
	if err != nil {
		return nil, err
	}
	selected := selector(current, candidates)
	if selected == nil {
		return nil, nil
	}
	chosen := *selected
	if err := c.executeCompaction(ctx, m, chosen.plan); err != nil {
		if isFenceError(err) {
			c.fenced.Store(true)
			return nil, err
		}
		return nil, fmt.Errorf("compact L%d to L%d: %w",
			chosen.plan.sourceLevel, chosen.plan.destinationLevel, err)
	}
	return &chosen, nil
}

func (c *compactor) beginRun(ctx context.Context) error {
	if c.closed.Load() {
		return errCompactorClosed
	}
	if err := c.acquireRun(ctx); err != nil {
		return err
	}

	c.lifecycleMu.Lock()
	if c.closed.Load() {
		c.lifecycleMu.Unlock()
		c.releaseRun()
		return errCompactorClosed
	}
	c.activeRuns.Add(1)
	c.lifecycleMu.Unlock()
	return nil
}

func (c *compactor) finishRun() {
	c.activeRuns.Done()
	c.releaseRun()
}

func (c *compactor) acquireRun(ctx context.Context) error {
	select {
	case c.runGate <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *compactor) releaseRun() {
	select {
	case <-c.runGate:
	default:
	}
}

type levelCompactionPlan struct {
	sourceLevel         uint32
	destinationLevel    uint32
	sourceSSTs          []sstMetadata
	destinationSSTs     []sstMetadata
	metadataOnly        bool
	forcedLevelCreation bool
}

// compactionOutputIdentityVersion domain-separates revisions of the attempt-key
// encoding. Cross-process output isolation comes from the compactor fence.
const compactionOutputIdentityVersion uint64 = 1

// compactionSSTStreamIdentity derives an output namespace scoped to one active
// compactor fence. The same plan retries to the same names while that ownership
// remains active; a successor fence receives different names and therefore
// cannot overwrite an older process's in-flight output.
func (c *compactor) compactionSSTStreamIdentity(
	plan *levelCompactionPlan,
	epoch uint64,
	createdAt time.Time,
) (sstStreamSetIdentity, int64, error) {
	if c == nil || plan == nil || c.fenceToken == nil || c.fenceToken.Epoch == 0 ||
		c.fenceToken.Owner == "" || c.fenceToken.ClaimedAt.IsZero() {
		return sstStreamSetIdentity{}, 0, errors.New("incomplete compactor attempt identity")
	}
	fence := c.fenceToken
	cutoff := compactionExpiryCutoff(plan)
	var cutoffMillis int64
	if !cutoff.IsZero() {
		cutoffMillis = cutoff.UnixMilli()
	}

	var encoded bytes.Buffer
	writeUint64 := func(value uint64) {
		_ = binary.Write(&encoded, binary.BigEndian, value)
	}
	writeInt64 := func(value int64) {
		_ = binary.Write(&encoded, binary.BigEndian, value)
	}
	writeString := func(value string) {
		writeUint64(uint64(len(value)))
		_, _ = encoded.WriteString(value)
	}
	writeSSTs := func(ssts []sstMetadata) {
		writeUint64(uint64(len(ssts)))
		for _, sst := range ssts {
			// Preserve execution order. It participates in duplicate-key
			// precedence, so sorting only for identity would be unsafe.
			writeString(sst.ID)
			writeString(sst.Checksum)
			writeInt64(sst.Size)
		}
	}

	writeUint64(compactionOutputIdentityVersion)
	writeUint64(fence.Epoch)
	writeString(fence.Owner)
	writeInt64(fence.ClaimedAt.UTC().UnixNano())
	writeUint64(uint64(plan.sourceLevel))
	writeUint64(uint64(plan.destinationLevel))
	writeInt64(cutoffMillis)
	writeInt64(c.opts.Output.TargetSSTBytes)
	writeInt64(int64(c.opts.Output.BloomBitsPerKey))
	writeInt64(int64(c.opts.Output.BlockBytes))
	writeString(strings.ToLower(c.opts.Output.Compression))
	writeSSTs(plan.sourceSSTs)
	writeSSTs(plan.destinationSSTs)

	digest := sha256.Sum256(encoded.Bytes())
	return sstStreamSetIdentity{
		OutputKey: hex.EncodeToString(digest[:]),
		Epoch:     epoch,
		CreatedAt: createdAt.UTC(),
	}, cutoffMillis, nil
}

// compactionExpiryCutoff is intentionally derived from immutable inputs rather
// than wall time. The oldest input creation time is no later than the creation
// time of the SST containing any entry, so converting only expirations below
// this floor cannot expire an entry early. It also keeps retry output
// byte-for-byte stable. A zero timestamp disables expiry rewriting defensively.
func compactionExpiryCutoff(plan *levelCompactionPlan) time.Time {
	var cutoff time.Time
	valid := true
	consider := func(ssts []sstMetadata) {
		for _, sst := range ssts {
			if sst.CreatedAt.IsZero() {
				valid = false
				continue
			}
			if cutoff.IsZero() || sst.CreatedAt.Before(cutoff) {
				cutoff = sst.CreatedAt
			}
		}
	}
	consider(plan.sourceSSTs)
	consider(plan.destinationSSTs)
	if !valid {
		return time.Time{}
	}
	return cutoff.UTC()
}

func (c *compactor) planCompactionCandidates(m *manifestState) ([]compactionCandidate, error) {
	if m == nil {
		return nil, nil
	}
	candidates := make([]compactionCandidate, 0, len(m.Levels)+1)
	var firstPlanningErr error
	now := time.Now()
	drainable := c.drainableL0(m, now)
	if c.l0NeedsPlanning(drainable) {
		// Criticality is a property of L0's depth, not of whether a plan could
		// be built for it. Computing it before the attempt means a level that
		// cannot be planned still reports as critical, instead of going quiet
		// exactly when it is most backed up.
		critical := l0CompactionCritical(len(drainable), c.opts.Trigger.L0SSTCount)
		// Not yet worth rewriting L1: wait for more to arrive. New L0 files
		// are merged with each other by the writer, from memory.
		if c.l0ShouldDrain(drainable, m, now) {
			inputs := drainable
			if len(inputs) > maxCompactionSSTsPerJob {
				inputs = inputs[len(inputs)-maxCompactionSSTsPerJob:]
			}
			plan, err := c.buildLevelPlanWithDrain(m, 0, 1, inputs)
			if err != nil {
				firstPlanningErr = err
				c.reportPlanningBlocked(0, len(drainable), critical, err)
			} else {
				candidate := compactionCandidate{
					plan:      plan,
					workUnits: 1,
					critical:  critical,
				}
				if plan.sourceLevel != 0 {
					// This lower-level plan is a prerequisite selected specifically to
					// unblock L0. Run it before unrelated level work; otherwise the
					// lower-level round robin can delay the drain while L0 keeps growing.
					return []compactionCandidate{candidate}, nil
				}
				candidates = append(candidates, candidate)
			}
		}
	}
	plannedLevels := make(map[uint32]struct{}, len(candidates))
	for i := range candidates {
		plannedLevels[candidates[i].plan.sourceLevel] = struct{}{}
	}
	for i := range m.Levels {
		level := &m.Levels[i]
		if level.TotalSize() <= c.levelTargetBytes(level.Number) {
			continue
		}
		if _, exists := plannedLevels[level.Number]; exists {
			continue
		}
		limit := min(maxCompactionSSTsPerJob, len(level.SSTs))
		plan, err := c.buildLevelPlanWithDrain(m, level.Number, level.Number+1, level.SSTs[:limit])
		if err != nil {
			if firstPlanningErr == nil {
				firstPlanningErr = err
			}
			c.reportPlanningBlocked(level.Number, len(level.SSTs), false, err)
			continue
		}
		if _, exists := plannedLevels[plan.sourceLevel]; exists {
			continue
		}
		candidates = append(candidates, compactionCandidate{
			plan:      plan,
			workUnits: 1,
		})
		plannedLevels[plan.sourceLevel] = struct{}{}
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		return candidates[i].plan.sourceLevel < candidates[j].plan.sourceLevel
	})
	if len(candidates) == 0 && firstPlanningErr != nil {
		return nil, firstPlanningErr
	}
	return candidates, nil
}

// l0NeedsPlanning reports whether the drainable part of L0 holds enough SSTs
// for an L0 job.
func (c *compactor) l0NeedsPlanning(drainable []sstMetadata) bool {
	return len(drainable) >= c.opts.Trigger.L0SSTCount
}

// drainableL0 is the part of L0 maintenance may drain, newest first: from the
// old end, every file up to the first flush file younger than L0MaxAge. The
// writer merges its recent flush files from memory and publishes the result
// under the compaction grammar, so a young flush file is one it still holds.
// A drain stops below it, not just around it: moving a newer L0 file into L1
// while an older one stays in L0 would put an older version of a key above a
// newer one. A flush file older than L0MaxAge is nobody's; it drains like
// any other.
func (c *compactor) drainableL0(m *manifestState, now time.Time) []sstMetadata {
	if m == nil {
		return nil
	}
	l0 := m.L0SSTs
	start := len(l0)
	for start > 0 {
		sst := l0[start-1]
		if !isCompactionSSTID(sst.ID) && now.Sub(sst.OldestData()) < c.opts.Trigger.L0MaxAge {
			break
		}
		start--
	}
	return l0[start:]
}

// l0Work returns how many drainable SSTs L0 held as of the last refresh, and
// whether that is enough for an L0 job.
func (c *compactor) l0Work() (ssts int, enough bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	drainable := c.drainableL0(c.manifest, time.Now())
	return len(drainable), c.l0NeedsPlanning(drainable)
}

// l0ShouldDrain reports whether L0 should merge into L1 now: when it holds at
// least L0ToBaseRatio of the L1 bytes it overlaps, so the rewrite of L1 is
// worth it (always so when it overlaps nothing, as with keys written in
// order); when some key is covered by L0MaxDepth L0 SSTs; or when L0's oldest
// data is L0MaxAge old.
func (c *compactor) l0ShouldDrain(l0 []sstMetadata, m *manifestState, now time.Time) bool {
	if len(l0) == 0 {
		return false
	}
	var l0Bytes, overlapBytes int64
	oldest := l0[0].OldestData()
	for _, sst := range l0 {
		l0Bytes += sst.Size
		if t := sst.OldestData(); !t.IsZero() && (oldest.IsZero() || t.Before(oldest)) {
			oldest = t
		}
	}
	minKey, maxKey := sstBounds(l0)
	if l1 := m.Level(1); l1 != nil {
		for _, sst := range l1.OverlappingSSTs(minKey, maxKey) {
			overlapBytes += sst.Size
		}
	}
	switch {
	case float64(l0Bytes) >= c.opts.Trigger.L0ToBaseRatio*float64(overlapBytes):
		return true
	case l0Depth(l0) >= c.opts.Trigger.L0MaxDepth:
		return true
	case !oldest.IsZero() && now.Sub(oldest) >= c.opts.Trigger.L0MaxAge:
		return true
	}
	return false
}

// l0Depth is the largest number of SSTs covering any one key: the L0 SSTs a
// lookup of that key checks.
func l0Depth(ssts []sstMetadata) int {
	type edge struct {
		key   []byte
		start bool
	}
	edges := make([]edge, 0, 2*len(ssts))
	for _, sst := range ssts {
		edges = append(edges, edge{sst.MinKey, true}, edge{sst.MaxKey, false})
	}
	// Key ranges are inclusive, so at an equal key a start counts before an
	// end.
	sort.Slice(edges, func(i, j int) bool {
		if c := bytes.Compare(edges[i].key, edges[j].key); c != 0 {
			return c < 0
		}
		return edges[i].start && !edges[j].start
	})
	depth, deepest := 0, 0
	for _, e := range edges {
		if e.start {
			depth++
			deepest = max(deepest, depth)
		} else {
			depth--
		}
	}
	return deepest
}

func (c *compactor) reportPlanningBlocked(sourceLevel uint32, sstCount int, critical bool, err error) {
	if c.opts.OnPlanningBlocked != nil {
		c.opts.OnPlanningBlocked(sourceLevel, sstCount, critical, err)
	}
}

// buildLevelPlanWithDrain returns the requested promotion when it fits in one
// manifest entry. If the destination is too fragmented, it recursively moves
// or compacts that destination downward first. Every successful plan still
// targets exactly one adjacent level; the caller retries the original
// promotion on the next maintenance cycle.
func (c *compactor) buildLevelPlanWithDrain(
	m *manifestState,
	sourceLevel, destinationLevel uint32,
	candidates []sstMetadata,
) (*levelCompactionPlan, error) {
	plan, err := c.buildLevelPlan(m, sourceLevel, destinationLevel, candidates)
	if err == nil {
		return plan, nil
	}
	if len(candidates) == 0 || destinationLevel == ^uint32(0) {
		return nil, err
	}

	// buildLevelPlan shrinks to one source before failing. Reproduce that final
	// source here to identify the exact destination range that must drain.
	blockingSource := candidates[:1]
	if sourceLevel == 0 {
		blockingSource = candidates[len(candidates)-1:]
	}
	minKey, maxKey := sstBounds(blockingSource)
	destination := m.Level(destinationLevel)
	if destination == nil {
		return nil, err
	}
	blockers := destination.OverlappingSSTs(minKey, maxKey)
	if len(blockers) == 0 {
		return nil, err
	}
	if len(blockers) > maxCompactionSSTsPerJob {
		blockers = blockers[:maxCompactionSSTsPerJob]
	}

	drain, drainErr := c.buildLevelPlanWithDrain(
		m, destinationLevel, destinationLevel+1, blockers)
	if drainErr != nil {
		return nil, fmt.Errorf("%w; drain L%d first: %v", err, destinationLevel, drainErr)
	}
	// A recursively selected drain may reach past the current bottom level.
	// Materializing that level is the bounded escape hatch for a destination
	// whose overlap cannot fit in one manifest entry. It is deliberately
	// distinguished from ordinary size-driven bottom-level growth: repeated
	// forced creation increases read depth and can defer overwrite/tombstone
	// reclamation, so operators need a visible signal when it happens.
	if m.Level(drain.destinationLevel) == nil {
		drain.forcedLevelCreation = true
	}
	return drain, nil
}

func (c *compactor) levelTargetBytes(level uint32) int64 {
	target := c.opts.Trigger.BaseLevelBytes
	for n := uint32(1); n < level; n++ {
		multiplier := int64(c.opts.Trigger.LevelSizeMultiplier)
		if target > (1<<63-1)/multiplier {
			return 1<<63 - 1
		}
		target *= multiplier
	}
	return target
}

func (c *compactor) buildLevelPlan(m *manifestState, sourceLevel, destinationLevel uint32, candidates []sstMetadata) (*levelCompactionPlan, error) {
	for count := len(candidates); count > 0; count-- {
		selected := candidates[:count]
		if sourceLevel == 0 {
			selected = candidates[len(candidates)-count:]
		}
		source := append([]sstMetadata(nil), selected...)
		minKey, maxKey := sstBounds(source)
		var destination []sstMetadata
		if level := m.Level(destinationLevel); level != nil {
			destination = level.OverlappingSSTs(minKey, maxKey)
		}
		// Move eligibility is structural: disjoint sources with nothing to
		// merge in the destination can change level by manifest edit alone.
		// Checksum validation does not disqualify a move, it only means the
		// sources are read and verified before the move is committed.
		metadataOnly := len(destination) == 0 && sstsDoNotOverlap(source)
		if len(source)+len(destination) <= maxCompactionSSTsPerJob {
			plan := &levelCompactionPlan{
				sourceLevel:      sourceLevel,
				destinationLevel: destinationLevel,
				sourceSSTs:       source,
				destinationSSTs:  destination,
				metadataOnly:     metadataOnly,
			}
			return plan, nil
		}
	}
	// Report the widest source the loop tried and how much of the destination
	// it had to drag along: naming only the option that tripped hides the fact
	// that shrinking the source cannot help when the sources span the whole
	// key range and the destination overlap never shrinks with them.
	destinationCount := 0
	if len(candidates) > 0 {
		if level := m.Level(destinationLevel); level != nil {
			oneSource := candidates[:1]
			if sourceLevel == 0 {
				oneSource = candidates[len(candidates)-1:]
			}
			minKey, maxKey := sstBounds(oneSource)
			destinationCount = len(level.OverlappingSSTs(minKey, maxKey))
		}
	}
	return nil, fmt.Errorf(
		"compaction L%d to L%d cannot be planned: one source plus %d destination SSTs "+
			"requires %d inputs, over the %d limit on inputs and retirement records per job",
		sourceLevel, destinationLevel, destinationCount, destinationCount+1,
		maxCompactionSSTsPerJob)
}

// oldestData returns when the oldest data in ssts was written.
func oldestData(ssts []sstMetadata) time.Time {
	var oldest time.Time
	for _, sst := range ssts {
		if t := sst.OldestData(); !t.IsZero() && (oldest.IsZero() || t.Before(oldest)) {
			oldest = t
		}
	}
	return oldest.UTC()
}

func sstBounds(ssts []sstMetadata) ([]byte, []byte) {
	var minKey, maxKey []byte
	for i := range ssts {
		if i == 0 || bytes.Compare(ssts[i].MinKey, minKey) < 0 {
			minKey = ssts[i].MinKey
		}
		if i == 0 || bytes.Compare(ssts[i].MaxKey, maxKey) > 0 {
			maxKey = ssts[i].MaxKey
		}
	}
	return minKey, maxKey
}

func sstsDoNotOverlap(ssts []sstMetadata) bool {
	if len(ssts) < 2 {
		return true
	}
	ordered := append([]sstMetadata(nil), ssts...)
	sort.Slice(ordered, func(i, j int) bool {
		return bytes.Compare(ordered[i].MinKey, ordered[j].MinKey) < 0
	})
	for i := 1; i < len(ordered); i++ {
		if bytes.Compare(ordered[i-1].MaxKey, ordered[i].MinKey) >= 0 {
			return false
		}
	}
	return true
}

func (c *compactor) executeCompaction(ctx context.Context, m *manifestState, plan *levelCompactionPlan) (err error) {
	job := compactionJob{
		DestinationLevel:    plan.destinationLevel,
		MetadataOnly:        plan.metadataOnly,
		ForcedLevelCreation: plan.forcedLevelCreation,
	}
	for _, sst := range plan.sourceSSTs {
		job.InputSSTs = append(job.InputSSTs, sst.ID)
	}
	for _, sst := range plan.destinationSSTs {
		job.InputSSTs = append(job.InputSSTs, sst.ID)
	}
	if c.opts.OnCompactionStart != nil {
		c.opts.OnCompactionStart(job)
	}
	defer func() {
		if c.opts.OnCompactionEnd != nil {
			c.opts.OnCompactionEnd(job, err)
		}
	}()

	outputs := plan.sourceSSTs
	if plan.metadataOnly {
		// A move publishes the same objects at a new level, so verification
		// costs the read a rewrite would have done anyway and skips the write
		// entirely: no new objects, no retirement records, no reclamation.
		if c.opts.Safety.ValidateSSTChecksum {
			readBytes, verifyErr := c.verifyMoveSources(ctx, plan.sourceSSTs)
			if verifyErr != nil {
				return verifyErr
			}
			job.ReadBytes = readBytes
		}
	} else {
		for _, sst := range plan.sourceSSTs {
			job.ReadBytes += sst.Size
		}
		for _, sst := range plan.destinationSSTs {
			job.ReadBytes += sst.Size
		}
		identity, expiryCutoffMillis, identityErr := c.compactionSSTStreamIdentity(
			plan, m.NextEpoch, time.Now().UTC())
		if identityErr != nil {
			return identityErr
		}
		inputs := append(append([]sstMetadata(nil), plan.sourceSSTs...), plan.destinationSSTs...)
		iters, readers, openErr := c.openSSTs(ctx, inputs)
		if openErr != nil {
			return openErr
		}
		defer func() {
			for _, reader := range readers {
				_ = reader.Close()
			}
		}()
		// An L0 merge's output takes one position in L0, so it is never
		// split; its inputs are planned to fit one SST.
		targetSize := c.opts.Output.TargetSSTBytes
		if plan.destinationLevel == 0 {
			targetSize = math.MaxInt64
		}
		results, writeErr := c.writeCompactedSSTs(
			ctx, newMergeIterator(iters), identity, expiryCutoffMillis, targetSize)
		if writeErr != nil {
			return writeErr
		}
		outputs = make([]sstMetadata, len(results))
		for i := range results {
			outputs[i] = results[i].Meta
		}
	}
	for i := range outputs {
		outputs[i].Level = plan.destinationLevel
	}
	if plan.destinationLevel == 0 && !plan.metadataOnly {
		// The output is new but its data is not: it keeps the age of the
		// oldest input, which the L0 age limit reads.
		oldest := oldestData(plan.sourceSSTs)
		for i := range outputs {
			outputs[i].OldestDataAt = oldest
		}
	}

	for _, output := range outputs {
		job.OutputSSTs = append(job.OutputSSTs, compactionOutput{
			ID:    output.ID,
			Bytes: output.Size,
			Level: output.Level,
		})
	}
	payload := manifest.CompactionLogPayload{
		RemoveSSTableIDs: job.InputSSTs,
		SourceLevel:      plan.sourceLevel,
		DestinationLevel: plan.destinationLevel,
		AddSSTables:      outputs,
	}
	return c.appendCompaction(ctx, m, payload)
}

func (c *compactor) appendCompaction(ctx context.Context, m *manifestState, payload manifest.CompactionLogPayload) error {
	added := make(map[string]struct{}, len(payload.AddSSTables))
	for _, sst := range payload.AddSSTables {
		added[sst.ID] = struct{}{}
	}
	retiredIDs := make([]string, 0, len(payload.RemoveSSTableIDs))
	for _, id := range payload.RemoveSSTableIDs {
		if _, stillLive := added[id]; !stillLive {
			retiredIDs = append(retiredIDs, id)
		}
	}
	retired, err := retiredSSTObjects(c.store, m, retiredIDs)
	if err != nil {
		return err
	}
	if c.stageCommand != nil {
		return c.stageCommand(ctx, manifest.MaintenanceCommand{
			Kind: manifest.MaintenanceCommandCompaction,
			Scheduling: manifest.MaintenanceScheduling{
				WorkUnits: 1,
			},
			Compaction: &manifest.CompactionCommand{
				Payload:        payload,
				RetiredObjects: retired,
			},
		})
	}
	_, err = c.manifestLog.AppendCompactionWithFence(ctx, payload, retired)
	if err != nil && isFenceError(err) {
		c.fenced.Store(true)
	}
	if err != nil {
		return err
	}

	return nil
}

func (c *compactor) IsFenced() bool {
	return c.fenced.Load()
}

func (c *compactor) FenceToken() *manifest.FenceToken {
	return c.fenceToken
}

// verifyMoveSources reads and checksums the sources of a move. It reuses the
// job's read parallelism and returns the bytes fetched so the cost of a
// verified move is visible next to the cost of a rewrite.
func (c *compactor) verifyMoveSources(ctx context.Context, ssts []sstMetadata) (int64, error) {
	if len(ssts) == 0 {
		return 0, nil
	}

	parentCtx := ctx
	parallelism := min(max(c.opts.InputReadParallelism, 1), len(ssts))
	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	errs := make([]error, len(ssts))
	completed := make([]bool, len(ssts))
	jobs := make(chan int)
	var wg sync.WaitGroup

	for worker := 0; worker < parallelism; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				if err := ctx.Err(); err != nil {
					errs[i] = err
					completed[i] = true
					continue
				}
				if errs[i] = c.verifyOneSST(ctx, ssts[i]); errs[i] != nil {
					cancel()
				}
				completed[i] = true
			}
		}()
	}
sendJobs:
	for i := range ssts {
		select {
		case jobs <- i:
		case <-ctx.Done():
			break sendJobs
		}
	}
	close(jobs)
	wg.Wait()

	var cancellationErr error
	for i := range errs {
		if errs[i] == nil {
			continue
		}
		if !errors.Is(errs[i], context.Canceled) {
			return 0, errs[i]
		}
		if cancellationErr == nil {
			cancellationErr = errs[i]
		}
	}
	if cancellationErr != nil {
		return 0, cancellationErr
	}

	var readBytes int64
	for i := range completed {
		if !completed[i] {
			if err := parentCtx.Err(); err != nil {
				return 0, err
			}
			return 0, fmt.Errorf("verify move source %s: verification did not complete", ssts[i].ID)
		}
		readBytes += ssts[i].Size
	}
	return readBytes, nil
}

func (c *compactor) verifyOneSST(ctx context.Context, sst sstMetadata) error {
	if err := verifyCompactionSST(ctx, c.store, sst); err != nil {
		return fmt.Errorf("verify move source %s: %w", sst.ID, err)
	}
	return nil
}

type openSSTResult struct {
	iter   sstable.Iterator
	reader *sstable.Reader
	err    error
}

func (c *compactor) openSSTs(ctx context.Context, ssts []sstMetadata) ([]sstable.Iterator, []*sstable.Reader, error) {
	if len(ssts) == 0 {
		return nil, nil, nil
	}

	parallelism := c.opts.InputReadParallelism
	if parallelism < 1 {
		parallelism = 1
	}
	if parallelism > len(ssts) {
		parallelism = len(ssts)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	results := make([]openSSTResult, len(ssts))
	jobs := make(chan int)
	var wg sync.WaitGroup

	for worker := 0; worker < parallelism; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				if err := ctx.Err(); err != nil {
					results[i].err = err
					continue
				}
				results[i] = c.openOneSST(ctx, ssts[i])
				if results[i].err != nil {
					cancel()
				}
			}
		}()
	}

	var sendErr error
sendJobs:
	for i := range ssts {
		select {
		case jobs <- i:
		case <-ctx.Done():
			sendErr = ctx.Err()
			break sendJobs
		}
	}
	close(jobs)
	wg.Wait()

	iters := make([]sstable.Iterator, 0, len(ssts))
	readers := make([]*sstable.Reader, 0, len(ssts))

	for i := range results {
		if results[i].err != nil {
			cleanupOpenResults(results)
			return nil, nil, results[i].err
		}
	}
	if sendErr != nil {
		cleanupOpenResults(results)
		return nil, nil, sendErr
	}

	for i := range results {
		if results[i].iter == nil || results[i].reader == nil {
			cleanupOpenResults(results)
			return nil, nil, fmt.Errorf("open sst %s: missing reader", ssts[i].ID)
		}
		iters = append(iters, results[i].iter)
		readers = append(readers, results[i].reader)
	}

	return iters, readers, nil
}

func (c *compactor) openOneSST(ctx context.Context, sst sstMetadata) openSSTResult {
	readable, err := stageCompactionSST(ctx, c.store, sst, c.opts.ScratchDir, c.opts.Safety.ValidateSSTChecksum)
	if err != nil {
		return openSSTResult{err: err}
	}

	reader, err := sstable.NewReader(ctx, readable, sstable.ReaderOptions{})
	if err != nil {
		_ = readable.Close()
		return openSSTResult{err: err}
	}

	iter, err := reader.NewIter(sstable.NoTransforms, nil, nil, sstable.AssertNoBlobHandles)
	if err != nil {
		_ = reader.Close()
		return openSSTResult{err: err}
	}

	return openSSTResult{iter: iter, reader: reader}
}

func cleanupOpenResults(results []openSSTResult) {
	for _, result := range results {
		if result.iter != nil {
			_ = result.iter.Close()
		}
		if result.reader != nil {
			_ = result.reader.Close()
		}
	}
}

func (c *compactor) writeCompactedSSTs(
	ctx context.Context,
	iter *kMergeIterator,
	identity sstStreamSetIdentity,
	expiryCutoffMillis int64,
	targetSize int64,
) (results []streamSSTResult, err error) {
	defer func() {
		err = errors.Join(err, iter.close())
	}()

	sstOpts := sstWriterOptions{
		BloomBitsPerKey: c.opts.Output.BloomBitsPerKey,
		BlockSize:       c.opts.Output.BlockBytes,
		Compression:     c.opts.Output.Compression,
	}

	adapter := &mergeIteratorAdapter{iter: iter, nowMs: expiryCutoffMillis}

	// An output ends at the first entry past the target, so it can exceed it
	// by that entry, its index and its Bloom filter.
	upload := blobstore.UploadInOneRequest(c.opts.Output.TargetSSTBytes*5/4 + maxCompactionOutputOvershoot)
	uploadFn := func(ctx context.Context, sstID string, r io.Reader) error {
		sstPath := c.store.SSTPath(sstID)
		_, err := c.store.WriteReader(ctx, sstPath, r, upload)
		return err
	}

	results, err = writeMultipleSSTsStreaming(
		ctx, adapter, sstOpts, identity, targetSize, uploadFn)
	if err != nil {
		if errors.Is(err, errEmptyIterator) {
			return nil, nil
		}
		return nil, err
	}

	return results, nil
}

type mergeIteratorAdapter struct {
	iter    *kMergeIterator
	current *internal.MemEntry
	done    bool
	err     error
	nowMs   int64
}

func (a *mergeIteratorAdapter) Next() bool {
	if a.done {
		return false
	}
	if !a.iter.Next() {
		a.done = true
		return false
	}

	entry, err := a.iter.entry()
	if err != nil {
		a.err = err
		a.done = true
		return false
	}

	a.current = &internal.MemEntry{
		Key:      entry.Key,
		Seq:      entry.Seq,
		Kind:     entry.Kind,
		Value:    entry.Value,
		ExpireAt: entry.ExpireAt,
	}

	if entry.ExpireAt > 0 && entry.ExpireAt <= a.nowMs {
		a.current.Kind = internal.OpDelete
		a.current.Value = nil
		a.current.ExpireAt = 0
	}

	return true
}

func (a *mergeIteratorAdapter) Entry() internal.MemEntry {
	if a.current == nil {
		return internal.MemEntry{}
	}
	return *a.current
}

func (a *mergeIteratorAdapter) Err() error {
	if a.err != nil {
		return a.err
	}
	return a.iter.Err()
}

func (a *mergeIteratorAdapter) Close() error {

	return nil
}
