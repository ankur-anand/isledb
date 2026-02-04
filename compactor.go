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
	CompactionL0Flush CompactionJobType = iota
	CompactionConsecutiveMerge
)

const CompactionMaxIterations = 100

type CompactionJob struct {
	Type      CompactionJobType
	InputSSTs []string
	InputRuns []uint32
	OutputRun *SortedRun
}

// Compactor merges SSTs into sorted runs in the background.
type Compactor struct {
	store       *blobstore.Store
	manifestLog *manifest.Store
	opts        CompactorOptions

	mu       sync.Mutex
	manifest *Manifest

	ticker *time.Ticker
	stopCh chan struct{}
	wg     sync.WaitGroup

	fenced     atomic.Bool
	fenceToken *manifest.FenceToken

	running atomic.Bool
	closed  atomic.Bool
}

func newCompactor(ctx context.Context, store *blobstore.Store, manifestLog *manifest.Store, opts CompactorOptions) (*Compactor, error) {
	d := DefaultCompactorOptions()
	opts.L0CompactionThreshold = cmp.Or(opts.L0CompactionThreshold, d.L0CompactionThreshold)
	opts.MinSources = cmp.Or(opts.MinSources, d.MinSources)
	opts.MaxSources = cmp.Or(opts.MaxSources, d.MaxSources)
	opts.SizeThreshold = cmp.Or(opts.SizeThreshold, d.SizeThreshold)
	opts.BloomBitsPerKey = cmp.Or(opts.BloomBitsPerKey, d.BloomBitsPerKey)
	opts.BlockSize = cmp.Or(opts.BlockSize, d.BlockSize)
	opts.Compression = cmp.Or(opts.Compression, d.Compression)
	opts.CheckInterval = cmp.Or(opts.CheckInterval, d.CheckInterval)
	opts.TargetSSTSize = cmp.Or(opts.TargetSSTSize, d.TargetSSTSize)

	m, err := manifestLog.Replay(ctx)
	if err != nil {
		return nil, fmt.Errorf("replay manifest: %w", err)
	}

	c := &Compactor{
		store:       store,
		manifestLog: manifestLog,
		opts:        opts,
		manifest:    m,
		stopCh:      make(chan struct{}),
	}

	if opts.EnableFencing {
		ownerID := opts.OwnerID
		if ownerID == "" {
			ownerID = fmt.Sprintf("compactor-%d-%d", time.Now().UnixNano(), m.NextEpoch)
		}
		token, err := manifestLog.ClaimCompactor(ctx, ownerID)
		if err != nil {
			return nil, fmt.Errorf("claim compactor fence: %w", err)
		}
		c.fenceToken = token
	}

	return c, nil
}

func (c *Compactor) Start() {
	if !c.running.CompareAndSwap(false, true) {
		return
	}
	c.ticker = time.NewTicker(c.opts.CheckInterval)
	c.wg.Add(1)
	go c.compactionLoop()
}

func (c *Compactor) Stop() {
	if !c.running.CompareAndSwap(true, false) {
		return
	}
	close(c.stopCh)
	if c.ticker != nil {
		c.ticker.Stop()
	}
	c.wg.Wait()
}

func (c *Compactor) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	c.Stop()
	return nil
}

func (c *Compactor) Refresh(ctx context.Context) error {
	m, err := c.manifestLog.Replay(ctx)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.manifest = m
	c.mu.Unlock()
	return nil
}

func (c *Compactor) compactionLoop() {
	defer c.wg.Done()
	for {
		select {
		case <-c.ticker.C:
			if err := c.RunCompaction(context.Background()); err != nil {

				if errors.Is(err, manifest.ErrFenced) {
					slog.Error("isledb: compactor fenced, stopping background compaction")
					return
				}
				slog.Error("isledb: compaction error", "error", err)
			}
		case <-c.stopCh:
			return
		}
	}
}

// RunCompaction performs a compaction cycle and returns when no work remains.
func (c *Compactor) RunCompaction(ctx context.Context) error {

	for i := 0; i < CompactionMaxIterations; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if c.fenced.Load() {
			return manifest.ErrFenced
		}

		if err := c.Refresh(ctx); err != nil {
			return fmt.Errorf("refresh manifest: %w", err)
		}

		c.mu.Lock()
		m := c.manifest.Clone()
		c.mu.Unlock()

		if m.L0SSTCount() >= c.opts.L0CompactionThreshold {
			if err := c.compactL0(ctx, m); err != nil {

				if errors.Is(err, manifest.ErrFenced) {
					c.fenced.Store(true)
					return err
				}
				return fmt.Errorf("L0 compaction: %w", err)
			}
			continue
		}

		if job := c.findConsecutiveCompaction(m); job != nil {
			if err := c.compactRuns(ctx, m, job); err != nil {

				if errors.Is(err, manifest.ErrFenced) {
					c.fenced.Store(true)
					return err
				}
				return fmt.Errorf("consecutive compaction: %w", err)
			}
			continue
		}

		return nil
	}

	slog.Warn("isledb: compaction hit max iterations, possible infinite loop or excessive L0 accumulation",
		"CompactionMaxIterations", CompactionMaxIterations)
	return nil
}

func (c *Compactor) compactL0(ctx context.Context, m *Manifest) error {
	if len(m.L0SSTs) == 0 {
		return nil
	}

	job := CompactionJob{
		Type:      CompactionL0Flush,
		InputSSTs: make([]string, len(m.L0SSTs)),
	}
	for i, sst := range m.L0SSTs {
		job.InputSSTs[i] = sst.ID
	}

	if c.opts.OnCompactionStart != nil {
		c.opts.OnCompactionStart(job)
	}

	iters, readers, err := c.openSSTs(ctx, m.L0SSTs)
	if err != nil {
		if c.opts.OnCompactionEnd != nil {
			c.opts.OnCompactionEnd(job, err)
		}
		return err
	}
	defer func() {
		for _, r := range readers {
			_ = r.Close()
		}
	}()

	mergeIter := newMergeIterator(iters)

	newEpoch := m.NextEpoch
	results, err := c.writeCompactedSSTs(ctx, mergeIter, newEpoch)
	if err != nil {
		if c.opts.OnCompactionEnd != nil {
			c.opts.OnCompactionEnd(job, err)
		}
		return err
	}

	if len(results) == 0 {

		if c.opts.OnCompactionEnd != nil {
			c.opts.OnCompactionEnd(job, nil)
		}
		return nil
	}

	newRunID := m.NextSortedRunID
	newRun := &SortedRun{
		ID:   newRunID,
		SSTs: make([]SSTMeta, len(results)),
	}
	for i, r := range results {
		newRun.SSTs[i] = r.Meta
	}
	sort.Slice(newRun.SSTs, func(i, j int) bool {
		return bytes.Compare(newRun.SSTs[i].MinKey, newRun.SSTs[j].MinKey) < 0
	})
	job.OutputRun = newRun

	payload := manifest.CompactionLogPayload{
		RemoveSSTableIDs: job.InputSSTs,
		AddSortedRun:     newRun,
	}
	if err := c.appendCompaction(ctx, payload); err != nil {
		if c.opts.OnCompactionEnd != nil {
			c.opts.OnCompactionEnd(job, err)
		}
		return err
	}

	if c.opts.OnCompactionEnd != nil {
		c.opts.OnCompactionEnd(job, nil)
	}

	return nil
}

func (c *Compactor) findConsecutiveCompaction(m *Manifest) *CompactionJob {
	runs := m.FindConsecutiveSimilarRuns(
		c.opts.MinSources,
		c.opts.MaxSources,
		c.opts.SizeThreshold,
	)

	if len(runs) == 0 {
		return nil
	}

	job := &CompactionJob{
		Type:      CompactionConsecutiveMerge,
		InputRuns: make([]uint32, len(runs)),
		InputSSTs: make([]string, 0),
	}

	for i, run := range runs {
		job.InputRuns[i] = run.ID
		for _, sst := range run.SSTs {
			job.InputSSTs = append(job.InputSSTs, sst.ID)
		}
	}

	return job
}

func (c *Compactor) compactRuns(ctx context.Context, m *Manifest, job *CompactionJob) error {
	if c.opts.OnCompactionStart != nil {
		c.opts.OnCompactionStart(*job)
	}

	var sstsToMerge []SSTMeta
	for _, runID := range job.InputRuns {
		run := m.GetSortedRun(runID)
		if run != nil {
			sstsToMerge = append(sstsToMerge, run.SSTs...)
		}
	}

	if len(sstsToMerge) == 0 {
		return nil
	}

	iters, readers, err := c.openSSTs(ctx, sstsToMerge)
	if err != nil {
		if c.opts.OnCompactionEnd != nil {
			c.opts.OnCompactionEnd(*job, err)
		}
		return err
	}
	defer func() {
		for _, r := range readers {
			_ = r.Close()
		}
	}()

	mergeIter := newMergeIterator(iters)

	newEpoch := m.NextEpoch
	results, err := c.writeCompactedSSTs(ctx, mergeIter, newEpoch)
	if err != nil {
		if c.opts.OnCompactionEnd != nil {
			c.opts.OnCompactionEnd(*job, err)
		}
		return err
	}

	if len(results) == 0 {

		if c.opts.OnCompactionEnd != nil {
			c.opts.OnCompactionEnd(*job, nil)
		}
		return nil
	}

	newRunID := m.NextSortedRunID
	newRun := &SortedRun{
		ID:   newRunID,
		SSTs: make([]SSTMeta, len(results)),
	}
	for i, r := range results {
		newRun.SSTs[i] = r.Meta
	}

	sort.Slice(newRun.SSTs, func(i, j int) bool {
		return bytes.Compare(newRun.SSTs[i].MinKey, newRun.SSTs[j].MinKey) < 0
	})
	job.OutputRun = newRun

	payload := manifest.CompactionLogPayload{
		RemoveSSTableIDs:   job.InputSSTs,
		RemoveSortedRunIDs: job.InputRuns,
		AddSortedRun:       newRun,
	}
	if err := c.appendCompaction(ctx, payload); err != nil {
		if c.opts.OnCompactionEnd != nil {
			c.opts.OnCompactionEnd(*job, err)
		}
		return err
	}

	if c.opts.OnCompactionEnd != nil {
		c.opts.OnCompactionEnd(*job, nil)
	}

	return nil
}

func (c *Compactor) appendCompaction(ctx context.Context, payload manifest.CompactionLogPayload) error {
	var err error
	if c.opts.EnableFencing {
		_, err = c.manifestLog.AppendCompactionWithFence(ctx, payload)
	} else {
		_, err = c.manifestLog.AppendCompaction(ctx, payload)
	}
	if err != nil && errors.Is(err, manifest.ErrFenced) {
		c.fenced.Store(true)
	}
	return err
}

func (c *Compactor) IsFenced() bool {
	return c.fenced.Load()
}

func (c *Compactor) FenceToken() *manifest.FenceToken {
	return c.fenceToken
}

func (c *Compactor) openSSTs(ctx context.Context, ssts []SSTMeta) ([]sstable.Iterator, []*sstable.Reader, error) {
	iters := make([]sstable.Iterator, 0, len(ssts))
	readers := make([]*sstable.Reader, 0, len(ssts))

	cleanup := func() {
		for _, it := range iters {
			_ = it.Close()
		}
		for _, r := range readers {
			_ = r.Close()
		}
	}

	for _, sst := range ssts {
		path := c.store.SSTPath(sst.ID)
		var data []byte
		var err error
		if sst.Size > 0 {
			data, _, err = c.store.ReadRange(ctx, path, 0, sst.Size)
		} else {
			data, _, err = c.store.Read(ctx, path)
		}
		if err != nil {
			cleanup()
			return nil, nil, fmt.Errorf("read sst %s: %w", sst.ID, err)
		}
		if err := validateSSTDataForCompaction(sst, data, c.opts.ValidateSSTChecksum, c.opts.SSTHashVerifier); err != nil {
			cleanup()
			return nil, nil, err
		}

		data, err = trimSSTData(sst, data)
		if err != nil {
			cleanup()
			return nil, nil, err
		}

		reader, err := sstable.NewReader(ctx, newSSTReadable(data), sstable.ReaderOptions{})
		if err != nil {
			cleanup()
			return nil, nil, err
		}
		readers = append(readers, reader)

		iter, err := reader.NewIter(sstable.NoTransforms, nil, nil, sstable.AssertNoBlobHandles)
		if err != nil {
			cleanup()
			return nil, nil, err
		}
		iters = append(iters, iter)
	}

	return iters, readers, nil
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

func (c *Compactor) writeCompactedSSTs(ctx context.Context, iter *kMergeIterator, epoch uint64) ([]streamSSTResult, error) {
	defer iter.close()

	sstOpts := SSTWriterOptions{
		BloomBitsPerKey: c.opts.BloomBitsPerKey,
		BlockSize:       c.opts.BlockSize,
		Compression:     c.opts.Compression,
	}

	adapter := &mergeIteratorAdapter{iter: iter, nowMs: time.Now().UnixMilli()}

	uploadFn := func(ctx context.Context, sstID string, r io.Reader) error {
		sstPath := c.store.SSTPath(sstID)
		_, err := c.store.WriteReader(ctx, sstPath, r, nil)
		return err
	}

	results, err := writeMultipleSSTsStreaming(ctx, adapter, sstOpts, epoch, c.opts.TargetSSTSize, uploadFn)
	if err != nil {
		if errors.Is(err, ErrEmptyIterator) {
			return nil, nil
		}
		return nil, err
	}

	for i := range results {
		results[i].Meta.Level = 1
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
