package isledb

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
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

type compactionJobType int

const (
	compactionL0ToL1 compactionJobType = iota
	compactionLevelToLevel
)

var errCompactorClosed = errors.New("compactor closed")

type compactionJob struct {
	Type             compactionJobType
	SourceLevel      uint32
	DestinationLevel uint32
	InputSSTs        []string
	OutputSSTs       []compactionOutput
	MetadataOnly     bool
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

	if fence == nil {
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
	if opts.Trigger.MaxInputSSTs <= 0 || opts.Trigger.MaxInputSSTs > manifest.MaxRetiredObjectsPerEntry {
		opts.Trigger.MaxInputSSTs = d.Trigger.MaxInputSSTs
	}
	if opts.Trigger.MaxInputBytes <= 0 {
		opts.Trigger.MaxInputBytes = d.Trigger.MaxInputBytes
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
	return waitGroupContext(ctx, &c.activeRuns)
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
		return nil, fmt.Errorf("L%d to L%d compaction: %w",
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
	sourceLevel      uint32
	destinationLevel uint32
	sourceSSTs       []sstMetadata
	destinationSSTs  []sstMetadata
	metadataOnly     bool
	workUnits        uint32
}

func (c *compactor) planCompactionCandidates(m *manifestState) ([]compactionCandidate, error) {
	if m == nil {
		return nil, nil
	}
	candidates := make([]compactionCandidate, 0, len(m.Levels)+1)
	var firstPlanningErr error
	if m.L0SSTCount() >= c.opts.Trigger.L0SSTCount {
		inputs := m.L0SSTs
		if len(inputs) > c.opts.Trigger.MaxInputSSTs {
			inputs = inputs[len(inputs)-c.opts.Trigger.MaxInputSSTs:]
		}
		plan, err := c.buildLevelPlan(m, 0, 1, inputs)
		if err != nil {
			firstPlanningErr = err
		} else {
			inputBytes, workUnits := compactionPlanWorkUnits(plan, c.opts.Trigger.MaxInputBytes)
			plan.workUnits = workUnits
			candidates = append(candidates, compactionCandidate{
				plan:       plan,
				inputBytes: inputBytes,
				workUnits:  workUnits,
				critical:   l0CompactionCritical(m.L0SSTCount(), c.opts.Trigger.L0SSTCount),
			})
		}
	}
	for i := range m.Levels {
		level := &m.Levels[i]
		if level.TotalSize() <= c.levelTargetBytes(level.Number) {
			continue
		}
		limit := min(c.opts.Trigger.MaxInputSSTs, len(level.SSTs))
		plan, err := c.buildLevelPlan(m, level.Number, level.Number+1, level.SSTs[:limit])
		if err != nil {
			if firstPlanningErr == nil {
				firstPlanningErr = err
			}
			continue
		}
		inputBytes, workUnits := compactionPlanWorkUnits(plan, c.opts.Trigger.MaxInputBytes)
		plan.workUnits = workUnits
		candidates = append(candidates, compactionCandidate{
			plan:       plan,
			inputBytes: inputBytes,
			workUnits:  workUnits,
		})
	}
	if len(candidates) == 0 && firstPlanningErr != nil {
		return nil, firstPlanningErr
	}
	return candidates, nil
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
		metadataOnly := len(destination) == 0 && sstsDoNotOverlap(source) &&
			!c.opts.Safety.ValidateSSTChecksum
		if metadataOnly || len(source)+len(destination) <= c.opts.Trigger.MaxInputSSTs {
			plan := &levelCompactionPlan{
				sourceLevel:      sourceLevel,
				destinationLevel: destinationLevel,
				sourceSSTs:       source,
				destinationSSTs:  destination,
				metadataOnly:     metadataOnly,
			}
			inputBytes, _ := compactionPlanWorkUnits(plan, c.opts.Trigger.MaxInputBytes)
			if metadataOnly || inputBytes <= c.opts.Trigger.MaxInputBytes || count == 1 {
				return plan, nil
			}
		}
	}
	return nil, fmt.Errorf("compaction overlap exceeds max input SSTs=%d for L%d to L%d", c.opts.Trigger.MaxInputSSTs, sourceLevel, destinationLevel)
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
	jobType := compactionLevelToLevel
	if plan.sourceLevel == 0 {
		jobType = compactionL0ToL1
	}
	job := compactionJob{
		Type:             jobType,
		SourceLevel:      plan.sourceLevel,
		DestinationLevel: plan.destinationLevel,
		MetadataOnly:     plan.metadataOnly,
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
	if !plan.metadataOnly {
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
		results, writeErr := c.writeCompactedSSTs(ctx, newMergeIterator(iters), m.NextEpoch)
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
	return c.appendCompaction(ctx, m, payload, plan.workUnits)
}

func (c *compactor) appendCompaction(ctx context.Context, m *manifestState, payload manifest.CompactionLogPayload, workUnits uint32) error {
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
				WorkUnits: workUnits,
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
	path := c.store.SSTPath(sst.ID)
	var data []byte
	var err error
	if sst.Size > 0 {
		data, err = c.store.ReadRange(ctx, path, 0, sst.Size)
	} else {
		data, _, err = c.store.Read(ctx, path)
	}
	if err != nil {
		return openSSTResult{err: fmt.Errorf("read sst %s: %w", sst.ID, err)}
	}
	if err := validateSSTDataForCompaction(sst, data, c.opts.Safety.ValidateSSTChecksum); err != nil {
		return openSSTResult{err: err}
	}

	data, err = trimSSTData(sst, data)
	if err != nil {
		return openSSTResult{err: err}
	}

	reader, err := sstable.NewReader(ctx, newSSTReadable(data), sstable.ReaderOptions{})
	if err != nil {
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

func validateSSTDataForCompaction(meta sstMetadata, data []byte, verify bool) error {
	if !verify {
		return nil
	}

	var err error
	data, err = trimSSTData(meta, data)
	if err != nil {
		return err
	}

	sum := sha256.Sum256(data)
	hashHex := hex.EncodeToString(sum[:])

	if meta.Checksum == "" {
		return fmt.Errorf("sst %s: missing checksum", meta.ID)
	}
	algo, expected, ok := strings.Cut(meta.Checksum, ":")
	if !ok || algo != "sha256" {
		return fmt.Errorf("sst %s: unsupported checksum %q", meta.ID, meta.Checksum)
	}
	if expected != hashHex {
		return fmt.Errorf("sst %s: checksum mismatch", meta.ID)
	}

	return nil
}

func (c *compactor) writeCompactedSSTs(ctx context.Context, iter *kMergeIterator, epoch uint64) ([]streamSSTResult, error) {
	defer iter.close()

	sstOpts := sstWriterOptions{
		BloomBitsPerKey: c.opts.Output.BloomBitsPerKey,
		BlockSize:       c.opts.Output.BlockBytes,
		Compression:     c.opts.Output.Compression,
	}

	adapter := &mergeIteratorAdapter{iter: iter, nowMs: time.Now().UnixMilli()}

	uploadFn := func(ctx context.Context, sstID string, r io.Reader) error {
		sstPath := c.store.SSTPath(sstID)
		_, err := c.store.WriteReader(ctx, sstPath, r, nil)
		return err
	}

	results, err := writeMultipleSSTsStreaming(ctx, adapter, sstOpts, epoch, c.opts.Output.TargetSSTBytes, uploadFn)
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
