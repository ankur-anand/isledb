package isledb

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/manifest"
	"github.com/cockroachdb/pebble/v2/objstorage"
	"github.com/cockroachdb/pebble/v2/sstable"
	"github.com/segmentio/ksuid"
)

// The writer merges its own L0 files. Every flush is committed as one small
// L0 file as usual; with L0Merge on, the writer also keeps the bytes it
// uploaded. The held files form a run: the writer's newest commits, adjacent
// in L0. When the run reaches L0Merge.TargetBytes, or its oldest file is
// L0Merge.MaxAge old, the run closes and a background goroutine merges it
// from memory into one L0 file, uploaded under the compaction grammar. The
// committer then publishes the merge, which replaces the run in L0.

const (
	// l0MergeHeldRuns bounds what the writer holds: this many runs of
	// TargetBytes. Past it, flushes are committed without a copy.
	l0MergeHeldRuns = 2
	// l0MergeIdentityVersion is hashed into output names. Bump it when what
	// the name covers changes.
	l0MergeIdentityVersion = 1
	l0MergeIdentityKind    = "writer-l0-merge"
)

// heldSST is one committed flush the writer still has: the object as it was
// uploaded, the SST followed by its Bloom sidecar, and the committed metadata.
type heldSST struct {
	meta sstMetadata
	data []byte
}

// l0Run is an adjacent stretch of the writer's commits, oldest first.
type l0Run struct {
	ssts  []heldSST
	bytes int64
}

func (r *l0Run) empty() bool { return len(r.ssts) == 0 }

func (r *l0Run) oldest() time.Time {
	if r.empty() {
		return time.Time{}
	}
	return r.ssts[0].meta.CreatedAt
}

func (r *l0Run) add(sst heldSST) {
	r.ssts = append(r.ssts, sst)
	r.bytes += int64(len(sst.data))
}

func (r *l0Run) metas() []sstMetadata {
	metas := make([]sstMetadata, len(r.ssts))
	for i, sst := range r.ssts {
		metas[i] = sst.meta
	}
	return metas
}

func (r *l0Run) touches(ids map[string]struct{}) bool {
	for _, sst := range r.ssts {
		if _, ok := ids[sst.meta.ID]; ok {
			return true
		}
	}
	return false
}

// mergedL0Run is a run the writer has merged and uploaded, not yet published.
// commitID stays the same across publish attempts, so an uncertain CAS
// result cannot replace the run twice.
type mergedL0Run struct {
	commitID string
	intent   sstMergeIntent
	inputs   []sstMetadata // oldest first, as committed
	output   sstMetadata
}

func (m *mergedL0Run) outputTarget(store *blobstore.Store) []sstDeletionTarget {
	return []sstDeletionTarget{{ID: m.output.ID, Key: store.SSTPath(m.output.ID), Size: m.output.Size}}
}

func (m *mergedL0Run) touches(ids map[string]struct{}) bool {
	for _, sst := range m.inputs {
		if _, ok := ids[sst.ID]; ok {
			return true
		}
	}
	return false
}

// l0Merger holds the writer's committed flushes and merges them. The
// committer calls add after each commit, pass once per pass, and publishes
// what finished has; one background goroutine at a time does the merging.
type l0Merger struct {
	w      *writer
	target int64
	maxAge time.Duration

	mu sync.Mutex
	// filling takes new copies. closed are runs waiting to be merged, oldest
	// first. held is the bytes in all of them.
	filling l0Run
	closed  []*l0Run
	held    int64
	// running is set while a merge goroutine works through closed; done is
	// closed when it exits.
	running bool
	done    chan struct{}
	// finished are merges uploaded and waiting for the committer to publish,
	// oldest first.
	finished []mergedL0Run
	// retirements are settled merges whose deletion plan the committer has
	// still to write.
	retirements []l0MergeRetirement

	failures       failureRun
	retireFailures failureRun
}

func newL0Merger(w *writer, opts WriterL0MergeOptions) *l0Merger {
	if opts.TargetBytes <= 0 {
		return nil
	}
	m := &l0Merger{w: w, target: opts.TargetBytes, maxAge: opts.MaxAge}
	m.failures.kind = "l0 merges"
	m.retireFailures.kind = "l0 merge retirements"
	return m
}

// enabled reports whether flushes should be copied for merging.
func (m *l0Merger) enabled() bool { return m != nil }

// add keeps a committed flush. data is the object as uploaded. Past the held
// limit the flush is not kept, and the filling run closes: the next flush
// kept would not be adjacent to it.
func (m *l0Merger) add(meta sstMetadata, data []byte) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	size := int64(len(data))
	if size < meta.Size || m.held+size > l0MergeHeldRuns*m.target {
		m.w.metrics.ObserveL0MergeSkipped(meta.Size)
		m.closeFillingLocked()
		m.startLocked()
		return
	}
	m.filling.add(heldSST{meta: meta, data: data})
	m.held += size
	m.w.metrics.ObserveL0MergeHeld(m.held)
	if m.filling.bytes >= m.target || len(m.filling.ssts) >= manifest.MaxRetiredObjectsPerEntry {
		m.closeFillingLocked()
	}
	m.startLocked()
}

// pass is the committer's hook, once per pass: closes the filling run when it
// is old enough and starts a merge if one is due.
func (m *l0Merger) pass(now time.Time) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.filling.empty() && m.maxAge > 0 && now.Sub(m.filling.oldest()) >= m.maxAge {
		m.closeFillingLocked()
	}
	m.startLocked()
}

// nextToPublish is the oldest finished merge, or nil.
func (m *l0Merger) nextToPublish() *mergedL0Run {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.finished) == 0 {
		return nil
	}
	merged := m.finished[0]
	return &merged
}

// published forgets a merge the committer has settled, landed or refused.
func (m *l0Merger) published(commitID string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, merged := range m.finished {
		if merged.commitID == commitID {
			m.finished = append(m.finished[:i], m.finished[i+1:]...)
			return
		}
	}
}

// dropTouching forgets every run and unpublished merge that holds one of ids:
// a maintenance command is about to take those files, and the writer never
// contends for them. It returns how many runs and merges were dropped. A
// merge running over a dropped run is discarded when it finishes.
func (m *l0Merger) dropTouching(ids []string) int {
	if m == nil || len(ids) == 0 {
		return 0
	}
	set := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		set[id] = struct{}{}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	dropped := 0
	if m.filling.touches(set) {
		m.held -= m.filling.bytes
		m.filling = l0Run{}
		dropped++
	}
	kept := m.closed[:0]
	for _, run := range m.closed {
		if run.touches(set) {
			m.held -= run.bytes
			dropped++
			continue
		}
		kept = append(kept, run)
	}
	m.closed = kept
	keptMerges := m.finished[:0]
	for _, merged := range m.finished {
		if merged.touches(set) {
			// Its output will never replace anything: retire it.
			m.retirements = append(m.retirements, l0MergeRetirement{
				intent: merged.intent, outcome: sstDeletionOutcomeOutput,
				appliedAt: time.Now().UTC(), targets: merged.outputTarget(m.w.store),
			})
			dropped++
			continue
		}
		keptMerges = append(keptMerges, merged)
	}
	m.finished = keptMerges
	if dropped > 0 {
		m.w.metrics.ObserveL0MergeHeld(m.held)
		m.w.metrics.ObserveL0MergeDropped(dropped)
	}
	return dropped
}

// heldBytes reports what the writer holds for merging.
func (m *l0Merger) heldBytes() int64 {
	if m == nil {
		return 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.held
}

// wait blocks until no merge goroutine runs. The writer's context must be
// cancelled first, or a merge may still start.
func (m *l0Merger) wait() {
	if m == nil {
		return
	}
	m.mu.Lock()
	done := m.done
	m.mu.Unlock()
	if done != nil {
		<-done
	}
}

func (m *l0Merger) closeFillingLocked() {
	if m.filling.empty() {
		return
	}
	run := m.filling
	m.closed = append(m.closed, &run)
	m.filling = l0Run{}
}

func (m *l0Merger) startLocked() {
	if m.running || len(m.closed) == 0 || m.w.ctx.Err() != nil {
		return
	}
	m.running = true
	m.done = make(chan struct{})
	go m.run(m.done)
}

// run merges closed runs, oldest first, until none is left or one fails. A
// failed run stays closed and is tried again when the committer next looks.
func (m *l0Merger) run(done chan struct{}) {
	defer close(done)
	ctx := m.w.ctx
	for {
		m.mu.Lock()
		if len(m.closed) == 0 || ctx.Err() != nil {
			m.running = false
			m.mu.Unlock()
			return
		}
		run := m.closed[0]
		m.mu.Unlock()

		merged, err := m.merge(ctx, run)
		m.w.metrics.ObserveL0Merge(merged.output.Size, err)
		if err != nil {
			m.mu.Lock()
			m.running = false
			m.mu.Unlock()
			if ctx.Err() == nil {
				m.w.reportFailure(&m.failures, fmt.Errorf("merge l0 run: %w", err))
			}
			return
		}

		m.mu.Lock()
		if i := indexOfRun(m.closed, run); i < 0 {
			// Dropped while merging: a maintenance command took its files.
			// The output will never replace anything: retire it.
			m.retirements = append(m.retirements, l0MergeRetirement{
				intent: merged.intent, outcome: sstDeletionOutcomeOutput,
				appliedAt: time.Now().UTC(), targets: merged.outputTarget(m.w.store),
			})
			m.mu.Unlock()
			continue
		} else {
			m.closed = append(m.closed[:i], m.closed[i+1:]...)
		}
		m.held -= run.bytes
		m.w.metrics.ObserveL0MergeHeld(m.held)
		m.finished = append(m.finished, merged)
		m.mu.Unlock()
		m.w.reportRecovery(&m.failures)
	}
}

func indexOfRun(runs []*l0Run, run *l0Run) int {
	for i, candidate := range runs {
		if candidate == run {
			return i
		}
	}
	return -1
}

// merge builds one SST from a run and uploads it. Inputs are read newest
// first, so the newest version of a key wins as it does in L0. Expired
// entries become tombstones at the oldest input's creation time, as a
// maintenance merge does. The output's name is a hash of the writer fence
// and the inputs, so a retry of the same run uploads to the same name and a
// later writer never writes to it.
func (m *l0Merger) merge(ctx context.Context, run *l0Run) (mergedL0Run, error) {
	w := m.w
	if run.empty() {
		return mergedL0Run{}, errors.New("empty run")
	}

	sstOpts := sstWriterOptions{
		BloomBitsPerKey: w.sstOutput.BloomBitsPerKey,
		BlockSize:       w.sstOutput.BlockBytes,
		Compression:     w.sstOutput.Compression,
	}
	inputs := run.metas()
	cutoff := l0RunExpiryCutoff(inputs)
	var cutoffMillis int64
	if !cutoff.IsZero() {
		cutoffMillis = cutoff.UnixMilli()
	}

	// The output takes an epoch from the writer's counter as a flush does, so
	// the manifest's next epoch moves past it.
	w.mu.Lock()
	epoch := w.epoch
	w.epoch++
	w.mu.Unlock()

	identity, err := m.identity(inputs, epoch, time.Now().UTC(), sstOpts, cutoffMillis)
	if err != nil {
		return mergedL0Run{}, err
	}
	output, err := identity.output(1)
	if err != nil {
		return mergedL0Run{}, err
	}
	// The intent first: from here on an object may exist that only this
	// record and the manifest can account for.
	intent, err := m.writeL0MergeIntent(ctx, inputs, output.ID)
	if err != nil {
		return mergedL0Run{}, err
	}

	readers := make([]*sstable.Reader, 0, len(run.ssts))
	iters := make([]sstable.Iterator, 0, len(run.ssts))
	closeInputs := func() {
		for _, reader := range readers {
			_ = reader.Close()
		}
	}
	for i := len(run.ssts) - 1; i >= 0; i-- {
		sst := run.ssts[i]
		reader, err := sstable.NewReader(ctx, newHeldSSTReadable(sst.data[:sst.meta.Size]), sstable.ReaderOptions{})
		if err != nil {
			closeInputs()
			return mergedL0Run{}, fmt.Errorf("open held sst %s: %w", sst.meta.ID, err)
		}
		readers = append(readers, reader)
		iter, err := reader.NewIter(sstable.NoTransforms, nil, nil, sstable.AssertNoBlobHandles)
		if err != nil {
			closeInputs()
			return mergedL0Run{}, fmt.Errorf("iterate held sst %s: %w", sst.meta.ID, err)
		}
		iters = append(iters, iter)
	}
	merged := newMergeIterator(iters)
	defer func() {
		_ = merged.close()
		closeInputs()
	}()

	upload := blobstore.UploadInOneRequest(run.bytes + run.bytes/8 + 1<<20)
	uploadFn := func(ctx context.Context, sstID string, r io.Reader) error {
		_, err := w.store.WriteReader(ctx, w.store.SSTPath(sstID), r, upload)
		return err
	}
	result, err := writeSSTStreaming(ctx, &mergeIteratorAdapter{iter: merged, nowMs: cutoffMillis},
		sstOpts, output, uploadFn)
	if err != nil {
		return mergedL0Run{}, err
	}
	meta := result.Meta
	meta.Level = 0
	// The output is new but its data is not: it keeps the age of the oldest
	// input, which the L0 age limit reads.
	meta.OldestDataAt = oldestData(inputs)

	slog.Debug("isledb: l0 run merged", "component", "writer", "inputs", len(inputs),
		"input_bytes", run.bytes, "output", meta.ID, "output_bytes", meta.Size)
	return mergedL0Run{commitID: ksuid.New().String(), intent: intent, inputs: inputs, output: meta}, nil
}

// retired names every input of a merge as an object to delete once the merge
// has replaced it.
func (m *l0Merger) retired(merged *mergedL0Run) []manifest.RetiredObject {
	retired := make([]manifest.RetiredObject, len(merged.inputs))
	for i, input := range merged.inputs {
		retired[i] = manifest.RetiredObject{
			Kind: manifest.RetiredObjectSST,
			ID:   input.ID,
			Key:  m.w.store.SSTPath(input.ID),
			Size: input.Size,
		}
	}
	return retired
}

func (m *l0Merger) inputIDs(merged *mergedL0Run) []string {
	ids := make([]string, len(merged.inputs))
	for i, input := range merged.inputs {
		ids[i] = input.ID
	}
	return ids
}

// identity names the merge's output: a hash over the writer fence, the
// inputs in merge order and everything that shapes the output's bytes.
func (m *l0Merger) identity(
	inputs []sstMetadata,
	epoch uint64,
	createdAt time.Time,
	opts sstWriterOptions,
	cutoffMillis int64,
) (sstStreamSetIdentity, error) {
	fence := m.w.fenceToken
	if fence == nil || fence.Epoch == 0 || fence.Owner == "" || fence.ClaimedAt.IsZero() {
		return sstStreamSetIdentity{}, errors.New("incomplete writer fence for l0 merge identity")
	}
	if epoch == 0 {
		return sstStreamSetIdentity{}, errors.New("zero epoch for l0 merge output")
	}

	hash := sha256.New()
	writeUint64 := func(value uint64) {
		_ = binary.Write(hash, binary.BigEndian, value)
	}
	writeInt64 := func(value int64) {
		_ = binary.Write(hash, binary.BigEndian, value)
	}
	writeString := func(value string) {
		writeUint64(uint64(len(value)))
		_, _ = hash.Write([]byte(value))
	}
	writeUint64(l0MergeIdentityVersion)
	writeString(l0MergeIdentityKind)
	writeUint64(fence.Epoch)
	writeString(fence.Owner)
	writeInt64(fence.ClaimedAt.UTC().UnixNano())
	writeInt64(cutoffMillis)
	writeInt64(int64(opts.BloomBitsPerKey))
	writeInt64(int64(opts.BlockSize))
	writeString(strings.ToLower(opts.Compression))
	writeUint64(uint64(len(inputs)))
	for _, sst := range inputs {
		writeString(sst.ID)
		writeString(sst.Checksum)
		writeInt64(sst.Size)
	}
	return sstStreamSetIdentity{
		OutputKey: hex.EncodeToString(hash.Sum(nil)),
		Epoch:     epoch,
		CreatedAt: createdAt.UTC(),
	}, nil
}

// l0RunExpiryCutoff is the oldest input's creation time: no entry in the run
// was written after it, so expirations below it are safe to convert. Zero,
// when any input lacks a time, converts nothing.
func l0RunExpiryCutoff(inputs []sstMetadata) time.Time {
	var cutoff time.Time
	for _, sst := range inputs {
		if sst.CreatedAt.IsZero() {
			return time.Time{}
		}
		if cutoff.IsZero() || sst.CreatedAt.Before(cutoff) {
			cutoff = sst.CreatedAt
		}
	}
	return cutoff.UTC()
}

// publishL0Merges publishes finished merges, oldest first. A merge the
// manifest refuses, because its run is no longer adjacent in L0, is dropped:
// nothing was written, and its output is an object nothing references. Any
// other failure ends the pass, so the next pass retries the same merge first
// and an uncertain CAS result is resolved by its commit ID before anything
// else is published.
func (w *writer) publishL0Merges(ctx context.Context) error {
	for {
		merged := w.l0Merge.nextToPublish()
		if merged == nil {
			return nil
		}
		attemptCtx, cancel := context.WithTimeout(ctx, commitAttemptBase)
		entry, err := w.manifestLog.AppendWriterL0Merge(attemptCtx, manifest.WriterL0Merge{
			ID:       merged.commitID,
			InputIDs: w.l0Merge.inputIDs(merged),
			Output:   merged.output,
			Retired:  w.l0Merge.retired(merged),
		})
		cancel()
		fenced := !w.manifestLog.WriterFenceObservedActive(w.fenceToken)
		switch {
		case err == nil:
			w.l0Merge.published(merged.commitID)
			// The inputs stopped being referenced when the entry was committed.
			w.l0Merge.retire(merged.intent, sstDeletionOutcomeInputs, entry.Timestamp,
				sstDeletionTargets(w.store, merged.inputs))
			w.metrics.ObserveL0MergePublished(nil)
			slog.Debug("isledb: l0 merge published", "component", "writer", "output", merged.output.ID,
				"inputs", len(merged.inputs), "manifest_seq", entry.Seq)
			if w.onL0MergePublished != nil {
				w.onL0MergePublished(*merged, nil)
			}
			if fenced {
				w.markFenced()
				return manifest.ErrFenced
			}
		case errors.Is(err, manifest.ErrInvalidManifest):
			w.l0Merge.published(merged.commitID)
			// Nothing was published: the output never replaced anything.
			w.l0Merge.retire(merged.intent, sstDeletionOutcomeOutput, time.Now(), merged.outputTarget(w.store))
			w.metrics.ObserveL0MergePublished(err)
			slog.Warn("isledb: l0 merge refused; its files are no longer a run in L0",
				"component", "writer", "output", merged.output.ID, "error", err)
			if w.onL0MergePublished != nil {
				w.onL0MergePublished(*merged, err)
			}
		case fenced || isFenceError(err):
			w.markFenced()
			return fmt.Errorf("publish l0 merge: %w", err)
		default:
			return fmt.Errorf("publish l0 merge: %w", err)
		}
	}
}

// heldSSTReadable gives Pebble's SST reader random access to an SST held in
// memory.
type heldSSTReadable struct {
	data []byte
	rh   objstorage.NoopReadHandle
}

func newHeldSSTReadable(data []byte) *heldSSTReadable {
	r := &heldSSTReadable{data: data}
	r.rh = objstorage.MakeNoopReadHandle(r)
	return r
}

func (r *heldSSTReadable) ReadAt(_ context.Context, p []byte, off int64) error {
	if off < 0 || off > int64(len(r.data)) || int64(len(p)) > int64(len(r.data))-off {
		return io.ErrUnexpectedEOF
	}
	copy(p, r.data[off:])
	return nil
}

func (r *heldSSTReadable) Size() int64 { return int64(len(r.data)) }

func (r *heldSSTReadable) NewReadHandle(_ objstorage.ReadBeforeSize) objstorage.ReadHandle {
	return &r.rh
}

func (*heldSSTReadable) Close() error { return nil }
