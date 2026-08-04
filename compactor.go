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
	"log/slog"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal"
	"github.com/ankur-anand/isledb/manifest"
	"github.com/cockroachdb/pebble/v2/sstable"
)

type CompactionJobType int

const (
	CompactionL0ToL1 CompactionJobType = iota
	CompactionLevelToLevel
)

const CompactionMaxIterations = 100

var errCompactorClosed = errors.New("compactor closed")

type CompactionJob struct {
	Type             CompactionJobType
	SourceLevel      uint32
	DestinationLevel uint32
	InputSSTs        []string
	OutputSSTs       []SSTMeta
	MetadataOnly     bool
}

// compactor moves and rewrites SSTs through non-overlapping levels.
type compactor struct {
	store         *blobstore.Store
	manifestLog   *manifest.Store
	gcCursorStore manifest.GCCursorStorage
	opts          compactorOptions

	mu       sync.Mutex
	manifest *Manifest

	lifecycleMu sync.Mutex
	ticker      *time.Ticker
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	activeRuns  sync.WaitGroup
	runGate     chan struct{}

	fenced                   atomic.Bool
	fenceToken               *manifest.FenceToken
	consecutiveL0Compactions int

	running atomic.Bool
	closed  atomic.Bool
}

func newCompactor(ctx context.Context, store *blobstore.Store, manifestLog *manifest.Store, opts compactorOptions) (*compactor, error) {
	return newCompactorWithFence(ctx, store, manifestLog, opts, nil)
}

func newCompactorWithFence(ctx context.Context, store *blobstore.Store, manifestLog *manifest.Store, opts compactorOptions, fence *manifest.FenceToken) (*compactor, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	opts = normalizeCompactorOptions(opts, store)

	m, err := manifestLog.Replay(ctx)
	if err != nil {
		return nil, fmt.Errorf("replay manifest: %w", err)
	}

	c := &compactor{
		store:         store,
		manifestLog:   manifestLog,
		gcCursorStore: opts.GCCursorStorage,
		opts:          opts,
		manifest:      m,
		runGate:       make(chan struct{}, 1),
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

func normalizeCompactorOptions(opts compactorOptions, store *blobstore.Store) compactorOptions {
	d := defaultCompactorOptions()
	if opts.InputReadParallelism <= 0 {
		opts.InputReadParallelism = d.InputReadParallelism
	}
	if opts.Trigger.CheckInterval <= 0 {
		opts.Trigger.CheckInterval = d.Trigger.CheckInterval
	}
	if opts.Trigger.L0SSTCount <= 0 {
		opts.Trigger.L0SSTCount = d.Trigger.L0SSTCount
	}
	if opts.Trigger.MaxConsecutiveL0Compactions <= 0 {
		opts.Trigger.MaxConsecutiveL0Compactions = d.Trigger.MaxConsecutiveL0Compactions
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
	if opts.GCCursorStorage == nil {
		opts.GCCursorStorage = newGCCursorStorage(store)
	}
	if opts.GCDeleteBatchSize <= 0 {
		opts.GCDeleteBatchSize = d.GCDeleteBatchSize
	}
	if opts.GCGracePeriod == 0 {
		opts.GCGracePeriod = d.GCGracePeriod
	}
	return opts
}

func (c *compactor) Start(ctx context.Context) error {
	if err := checkContext(ctx); err != nil {
		return err
	}

	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()

	if c.closed.Load() {
		return errCompactorClosed
	}
	if !c.running.CompareAndSwap(false, true) {
		return nil
	}

	loopCtx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	c.ticker = time.NewTicker(c.opts.Trigger.CheckInterval)
	c.wg.Add(1)
	go c.compactionLoop(loopCtx, c.ticker)
	return nil
}

func (c *compactor) stopLoop() {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()

	if c.cancel != nil {
		c.cancel()
		c.cancel = nil
	}
	if c.ticker != nil {
		c.ticker.Stop()
		c.ticker = nil
	}
	c.running.Store(false)
}

func (c *compactor) Close(ctx context.Context) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if c.closed.CompareAndSwap(false, true) {
		c.stopLoop()
	}
	if err := waitGroupContext(ctx, &c.wg); err != nil {
		return err
	}
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

func (c *compactor) refresh(ctx context.Context) error {
	m, err := c.manifestLog.Replay(ctx)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.manifest = m
	c.mu.Unlock()
	return nil
}

func (c *compactor) compactionLoop(ctx context.Context, ticker *time.Ticker) {
	defer c.wg.Done()
	defer func() {
		ticker.Stop()
		c.lifecycleMu.Lock()
		if c.ticker == ticker {
			c.ticker = nil
			c.cancel = nil
		}
		c.lifecycleMu.Unlock()
		c.running.Store(false)
	}()
	for {
		select {
		case <-ticker.C:
			if err := c.RunOnce(ctx); err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				if isFenceError(err) {
					slog.Error("isledb: compactor fenced, stopping background compaction")
					return
				}
				if errors.Is(err, manifest.ErrFenceConflict) {
					slog.Debug("isledb: compaction skipped after concurrent manifest update")
					continue
				}
				slog.Error("isledb: compaction error", "error", err)
			}
		case <-ctx.Done():
			return
		}
	}
}

// RunOnce performs one scheduler compaction pass and returns when no work remains.
func (c *compactor) RunOnce(ctx context.Context) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if err := c.beginRun(ctx); err != nil {
		return err
	}
	defer c.finishRun()

	for i := 0; i < CompactionMaxIterations; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if c.fenced.Load() {
			return manifest.ErrFenced
		}

		if err := c.refresh(ctx); err != nil {
			return fmt.Errorf("refresh manifest: %w", err)
		}

		c.mu.Lock()
		m := c.manifest.Clone()
		c.mu.Unlock()

		plan, err := c.planCompaction(m)
		if err != nil {
			return err
		}
		if plan != nil {
			if err := c.executeCompaction(ctx, m, plan); err != nil {
				if isFenceError(err) {
					c.fenced.Store(true)
					return err
				}
				return fmt.Errorf("L%d to L%d compaction: %w", plan.sourceLevel, plan.destinationLevel, err)
			}
			if plan.sourceLevel == 0 {
				c.consecutiveL0Compactions++
			} else {
				c.consecutiveL0Compactions = 0
			}
			continue
		}

		c.runSSTSweeperBestEffort(ctx)
		return nil
	}

	slog.Warn("isledb: compaction hit max iterations, possible infinite loop or excessive L0 accumulation",
		"CompactionMaxIterations", CompactionMaxIterations)
	c.runSSTSweeperBestEffort(ctx)
	return nil
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

func (c *compactor) runSSTSweeperBestEffort(ctx context.Context) {
	if err := c.manifestLog.CheckCompactorFence(ctx); err != nil {
		return
	}
	if _, err := runRetirementSweeper(ctx, c.store, c.manifestLog, c.gcCursorStore, c.fenceToken, c.opts.GCDeleteBatchSize); err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		slog.Warn("isledb: compactor sst sweep failed", "error", err)
	}
}

type levelCompactionPlan struct {
	sourceLevel      uint32
	destinationLevel uint32
	sourceSSTs       []SSTMeta
	destinationSSTs  []SSTMeta
	metadataOnly     bool
}

func (c *compactor) planCompaction(m *Manifest) (*levelCompactionPlan, error) {
	var levelPlan *levelCompactionPlan
	for i := range m.Levels {
		level := &m.Levels[i]
		if level.TotalSize() <= c.levelTargetBytes(level.Number) {
			continue
		}
		limit := c.opts.Trigger.MaxInputSSTs
		if limit > len(level.SSTs) {
			limit = len(level.SSTs)
		}
		var err error
		levelPlan, err = c.buildLevelPlan(m, level.Number, level.Number+1, level.SSTs[:limit])
		if err != nil {
			return nil, err
		}
		break
	}

	if m.L0SSTCount() >= c.opts.Trigger.L0SSTCount &&
		(levelPlan == nil || c.consecutiveL0Compactions < c.opts.Trigger.MaxConsecutiveL0Compactions) {
		inputs := m.L0SSTs
		if len(inputs) > c.opts.Trigger.MaxInputSSTs {
			inputs = inputs[len(inputs)-c.opts.Trigger.MaxInputSSTs:]
		}
		return c.buildLevelPlan(m, 0, 1, inputs)
	}

	return levelPlan, nil
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

func (c *compactor) buildLevelPlan(m *Manifest, sourceLevel, destinationLevel uint32, candidates []SSTMeta) (*levelCompactionPlan, error) {
	for count := len(candidates); count > 0; count-- {
		selected := candidates[:count]
		if sourceLevel == 0 {
			selected = candidates[len(candidates)-count:]
		}
		source := append([]SSTMeta(nil), selected...)
		minKey, maxKey := sstBounds(source)
		var destination []SSTMeta
		if level := m.Level(destinationLevel); level != nil {
			destination = level.OverlappingSSTs(minKey, maxKey)
		}
		metadataOnly := len(destination) == 0 && sstsDoNotOverlap(source) &&
			!c.opts.Safety.ValidateSSTChecksum && c.opts.Safety.SSTHashVerifier == nil
		if metadataOnly || len(source)+len(destination) <= c.opts.Trigger.MaxInputSSTs {
			return &levelCompactionPlan{
				sourceLevel:      sourceLevel,
				destinationLevel: destinationLevel,
				sourceSSTs:       source,
				destinationSSTs:  destination,
				metadataOnly:     metadataOnly,
			}, nil
		}
	}
	return nil, fmt.Errorf("compaction overlap exceeds max input SSTs=%d for L%d to L%d", c.opts.Trigger.MaxInputSSTs, sourceLevel, destinationLevel)
}

func sstBounds(ssts []SSTMeta) ([]byte, []byte) {
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

func sstsDoNotOverlap(ssts []SSTMeta) bool {
	if len(ssts) < 2 {
		return true
	}
	ordered := append([]SSTMeta(nil), ssts...)
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

func (c *compactor) executeCompaction(ctx context.Context, m *Manifest, plan *levelCompactionPlan) (err error) {
	jobType := CompactionLevelToLevel
	if plan.sourceLevel == 0 {
		jobType = CompactionL0ToL1
	}
	job := CompactionJob{
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
		inputs := append(append([]SSTMeta(nil), plan.sourceSSTs...), plan.destinationSSTs...)
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
		outputs = make([]SSTMeta, len(results))
		for i := range results {
			outputs[i] = results[i].Meta
		}
	}
	for i := range outputs {
		outputs[i].Level = plan.destinationLevel
	}

	job.OutputSSTs = append(job.OutputSSTs, outputs...)
	payload := manifest.CompactionLogPayload{
		RemoveSSTableIDs: job.InputSSTs,
		SourceLevel:      plan.sourceLevel,
		DestinationLevel: plan.destinationLevel,
		AddSSTables:      outputs,
	}
	return c.appendCompaction(ctx, m, payload)
}

func (c *compactor) appendCompaction(ctx context.Context, m *Manifest, payload manifest.CompactionLogPayload) error {
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
	retired, err := retiredSSTObjects(c.store, m, retiredIDs, c.opts.GCGracePeriod)
	if err != nil {
		return err
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

func (c *compactor) openSSTs(ctx context.Context, ssts []SSTMeta) ([]sstable.Iterator, []*sstable.Reader, error) {
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

func (c *compactor) openOneSST(ctx context.Context, sst SSTMeta) openSSTResult {
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
	if err := validateSSTDataForCompaction(sst, data, c.opts.Safety.ValidateSSTChecksum, c.opts.Safety.SSTHashVerifier); err != nil {
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

func validateSSTDataForCompaction(meta SSTMeta, data []byte, verify bool, verifier SSTHashVerifier) error {
	if verifier != nil && meta.Signature == nil {
		return fmt.Errorf("sst %s: missing signature", meta.ID)
	}

	needHash := verify || verifier != nil
	if !needHash {
		return nil
	}

	var err error
	data, err = trimSSTData(meta, data)
	if err != nil {
		return err
	}

	sum := sha256.Sum256(data)
	hashHex := hex.EncodeToString(sum[:])

	if verify {
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
	}

	if verifier != nil {
		if meta.Signature.Hash != "" && meta.Signature.Hash != hashHex {
			return fmt.Errorf("sst %s: signature hash mismatch", meta.ID)
		}
		if err := verifier.VerifyHash(sum[:], *meta.Signature); err != nil {
			return fmt.Errorf("sst %s: signature verify: %w", meta.ID, err)
		}
	}

	return nil
}

func (c *compactor) writeCompactedSSTs(ctx context.Context, iter *kMergeIterator, epoch uint64) ([]streamSSTResult, error) {
	defer iter.close()

	sstOpts := SSTWriterOptions{
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
		if errors.Is(err, ErrEmptyIterator) {
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
		Inline:   entry.Inline,
		Value:    entry.Value,
		BlobID:   entry.BlobID,
		ExpireAt: entry.ExpireAt,
	}

	if entry.ExpireAt > 0 && entry.ExpireAt <= a.nowMs {
		a.current.Kind = internal.OpDelete
		a.current.Inline = false
		a.current.Value = nil
		a.current.BlobID = [32]byte{}

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
