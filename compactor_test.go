package isledb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/manifest"
)

type blockingReadCurrentStorage struct {
	manifest.Storage
	block   atomic.Bool
	started chan struct{}
	release chan struct{}
}

func (s *blockingReadCurrentStorage) ReadCurrent(ctx context.Context) ([]byte, string, error) {
	if s.block.CompareAndSwap(true, false) {
		close(s.started)
		<-s.release
	}
	return s.Storage.ReadCurrent(ctx)
}

// runCompactorUntilIdle exercises the selected-work executor without keeping
// a second scheduler in production code. Direct executor tests commit work
// immediately, so the helper refreshes and selects until no candidate remains.
func runCompactorUntilIdle(ctx context.Context, c *compactor) error {
	state := manifest.MaintenanceSchedulerState{}
	for iteration := 0; iteration < 100; iteration++ {
		selected, err := c.runSelected(ctx, func(_ *manifest.Current, candidates []compactionCandidate) *compactionCandidate {
			return selectCompactionCandidate(candidates, state)
		})
		if err != nil {
			return err
		}
		if selected == nil {
			return nil
		}
		units := selected.workUnits
		if units == 0 {
			units = 1
		}
		if selected.plan.sourceLevel == 0 {
			state.L0UnitsSinceLower = min(^uint32(0), state.L0UnitsSinceLower+units)
		} else {
			state.L0UnitsSinceLower = 0
			state.NextLowerLevel = selected.plan.sourceLevel + 1
		}
		if c.stageCommand != nil {
			return nil
		}
	}
	return errors.New("compaction test driver exceeded 100 selected work items")
}

func TestCompactor_CompactionOutputIdentityIsStableAcrossRetry(t *testing.T) {
	oldest := time.Date(2026, 8, 24, 10, 0, 0, 0, time.UTC)
	newest := oldest.Add(time.Hour)
	fence := &manifest.FenceToken{
		Epoch:     11,
		Owner:     "maintenance-a",
		ClaimedAt: oldest.Add(-time.Hour),
	}
	plan := &levelCompactionPlan{
		sourceLevel:      0,
		destinationLevel: 1,
		sourceSSTs: []sstMetadata{
			{ID: "source-a.sst", Checksum: "sha256:source-a", Size: 100, CreatedAt: oldest},
			{ID: "source-b.sst", Checksum: "sha256:source-b", Size: 200, CreatedAt: newest},
		},
		destinationSSTs: []sstMetadata{
			{ID: "destination.sst", Checksum: "sha256:destination", Size: 300, CreatedAt: oldest.Add(30 * time.Minute)},
		},
	}
	c := &compactor{
		opts:       normalizeCompactorOptions(compactorOptions{}),
		fenceToken: fence,
	}

	first, firstCutoff, err := c.compactionSSTStreamIdentity(plan, 7, newest.Add(time.Minute))
	if err != nil {
		t.Fatalf("first compaction identity: %v", err)
	}
	retry, retryCutoff, err := c.compactionSSTStreamIdentity(plan, 99, newest.Add(time.Hour))
	if err != nil {
		t.Fatalf("retry compaction identity: %v", err)
	}
	if first.OutputKey != retry.OutputKey {
		t.Fatalf("retry changed output key: first=%q retry=%q", first.OutputKey, retry.OutputKey)
	}
	if firstCutoff != oldest.UnixMilli() || retryCutoff != firstCutoff {
		t.Fatalf("expiry cutoff: first=%d retry=%d want=%d", firstCutoff, retryCutoff, oldest.UnixMilli())
	}
	firstOutput, err := first.output(1)
	if err != nil {
		t.Fatalf("first output identity: %v", err)
	}
	retryOutput, err := retry.output(1)
	if err != nil {
		t.Fatalf("retry output identity: %v", err)
	}
	if firstOutput.ID != retryOutput.ID {
		t.Fatalf("retry changed object ID: first=%q retry=%q", firstOutput.ID, retryOutput.ID)
	}
	if firstOutput.Epoch == retryOutput.Epoch {
		t.Fatalf("test requires manifest epochs to differ: first=%d retry=%d", firstOutput.Epoch, retryOutput.Epoch)
	}

	successor := &compactor{
		opts: c.opts,
		fenceToken: &manifest.FenceToken{
			Epoch:     fence.Epoch + 1,
			Owner:     "maintenance-b",
			ClaimedAt: fence.ClaimedAt.Add(time.Hour),
		},
	}
	successorIdentity, _, err := successor.compactionSSTStreamIdentity(
		plan, 100, newest.Add(2*time.Hour))
	if err != nil {
		t.Fatalf("successor compaction identity: %v", err)
	}
	if successorIdentity.OutputKey == first.OutputKey {
		t.Fatal("successor fence reused the previous compactor's output namespace")
	}

	changedInput := *plan
	changedInput.sourceSSTs = append([]sstMetadata(nil), plan.sourceSSTs...)
	changedInput.sourceSSTs[0].Checksum = "sha256:different-source"
	changed, _, err := c.compactionSSTStreamIdentity(&changedInput, 99, newest.Add(time.Hour))
	if err != nil {
		t.Fatalf("changed-input compaction identity: %v", err)
	}
	if changed.OutputKey == first.OutputKey {
		t.Fatal("changing an input checksum did not change the output key")
	}

	changedOpts := c.opts
	changedOpts.Output.TargetSSTBytes++
	changedPolicy := &compactor{opts: changedOpts, fenceToken: fence}
	changed, _, err = changedPolicy.compactionSSTStreamIdentity(plan, 99, newest.Add(time.Hour))
	if err != nil {
		t.Fatalf("changed-policy compaction identity: %v", err)
	}
	if changed.OutputKey == first.OutputKey {
		t.Fatal("changing the output split policy did not change the output key")
	}

	missingCreationTime := *plan
	missingCreationTime.sourceSSTs = append([]sstMetadata(nil), plan.sourceSSTs...)
	missingCreationTime.sourceSSTs[0].CreatedAt = time.Time{}
	_, cutoff, err := c.compactionSSTStreamIdentity(&missingCreationTime, 99, newest.Add(time.Hour))
	if err != nil {
		t.Fatalf("missing-created-at compaction identity: %v", err)
	}
	if cutoff != 0 {
		t.Fatalf("missing input creation time produced expiry cutoff %d, want 0", cutoff)
	}

	withoutFence := &compactor{opts: c.opts}
	if _, _, err := withoutFence.compactionSSTStreamIdentity(plan, 100, newest); err == nil {
		t.Fatal("compaction identity without an active fence succeeded")
	}
}

func TestCompactor_RejectsNilContext(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("compactor-nil-context")
	defer store.Close()

	manifestStore := newManifestStore(store, nil)
	if _, err := newCompactor(nil, store, manifestStore, compactorOptions{}); !errors.Is(err, ErrNilContext) {
		t.Fatalf("newCompactor(nil) error=%v, want %v", err, ErrNilContext)
	}

	c, err := newCompactor(ctx, store, manifestStore, compactorOptions{})
	if err != nil {
		t.Fatalf("newCompactor: %v", err)
	}
	defer c.Close(ctx)

	if err := runCompactorUntilIdle(nil, c); !errors.Is(err, ErrNilContext) {
		t.Fatalf("runSelected(nil) error=%v, want %v", err, ErrNilContext)
	}
	if err := c.Close(nil); !errors.Is(err, ErrNilContext) {
		t.Fatalf("Close(nil) error=%v, want %v", err, ErrNilContext)
	}
}

func TestCompactor_RunSelectedAfterCloseReturnsClosed(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("compactor-closed")
	defer store.Close()

	manifestStore := newManifestStore(store, nil)
	c, err := newCompactor(ctx, store, manifestStore, compactorOptions{})
	if err != nil {
		t.Fatalf("newCompactor: %v", err)
	}
	if err := c.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := runCompactorUntilIdle(ctx, c); !errors.Is(err, errCompactorClosed) {
		t.Fatalf("runSelected after Close error=%v, want %v", err, errCompactorClosed)
	}
}

func TestCompactor_CloseWaitsForSelectedRun(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("compactor-close-manual-runonce")
	defer store.Close()

	storage := &blockingReadCurrentStorage{
		Storage: manifest.NewBlobStoreBackend(store),
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	manifestStore := manifest.NewStoreWithStorage(storage)
	c, err := newCompactor(ctx, store, manifestStore, compactorOptions{})
	if err != nil {
		t.Fatalf("newCompactor: %v", err)
	}

	storage.block.Store(true)
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- runCompactorUntilIdle(ctx, c)
	}()

	select {
	case <-storage.started:
	case <-time.After(2 * time.Second):
		t.Fatal("selected run did not reach blocking CURRENT read")
	}

	closeCtx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	err = c.Close(closeCtx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first close error=%v, want %v", err, context.DeadlineExceeded)
	}

	close(storage.release)
	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatalf("selected run error=%v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("selected run did not finish after release")
	}

	retryCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := c.Close(retryCtx); err != nil {
		t.Fatalf("retry close: %v", err)
	}
}

func TestCompactor_RunSelectedSerializesConcurrentCalls(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("compactor-runonce-serial")
	defer store.Close()

	storage := &blockingReadCurrentStorage{
		Storage: manifest.NewBlobStoreBackend(store),
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	manifestStore := manifest.NewStoreWithStorage(storage)
	c, err := newCompactor(ctx, store, manifestStore, compactorOptions{})
	if err != nil {
		t.Fatalf("newCompactor: %v", err)
	}
	defer c.Close(ctx)

	storage.block.Store(true)
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- runCompactorUntilIdle(ctx, c)
	}()

	select {
	case <-storage.started:
	case <-time.After(2 * time.Second):
		t.Fatal("first selected run did not reach blocking CURRENT read")
	}

	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	err = runCompactorUntilIdle(waitCtx, c)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second selected run error=%v, want %v", err, context.DeadlineExceeded)
	}

	close(storage.release)
	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatalf("first selected run error=%v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("first selected run did not finish after release")
	}
}

func TestCompactor_L0Compaction(t *testing.T) {
	store := blobstore.NewMemory("test")
	ctx := context.Background()

	manifestStore := newManifestStore(store, nil)

	writerOpts := DefaultWriterOptions()
	writerOpts.Flush.Interval = 0
	writerOpts.Memtable.TargetBytes = 1024

	writer, err := newWriter(ctx, store, manifestStore, writerOpts)
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}

	for batch := 0; batch < 10; batch++ {
		for i := 0; i < 10; i++ {
			key := []byte{byte(batch), byte(i)}
			value := []byte("value")
			if err := writer.put(ctx, key, value); err != nil {
				t.Fatalf("put: %v", err)
			}
		}
		if err := writer.flush(ctx); err != nil {
			t.Fatalf("flush: %v", err)
		}
	}
	writer.close(ctx)

	compactorOpts := defaultCompactorOptions()
	compactorOpts.Trigger.L0SSTCount = 4

	var compactionStarted, compactionEnded bool
	var completedJob compactionJob
	compactorOpts.OnCompactionStart = func(job compactionJob) {
		compactionStarted = true
	}
	compactorOpts.OnCompactionEnd = func(job compactionJob, err error) {
		compactionEnded = true
		completedJob = job
	}

	compactor, err := newCompactor(ctx, store, manifestStore, compactorOpts)
	if err != nil {
		t.Fatalf("newCompactor: %v", err)
	}
	defer compactor.Close(ctx)

	if err := runCompactorUntilIdle(ctx, compactor); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	if !compactionStarted || !compactionEnded {
		t.Errorf("compaction callbacks not called: started=%v ended=%v", compactionStarted, compactionEnded)
	}
	if len(completedJob.OutputSSTs) == 0 {
		t.Fatal("completed compaction has no output summaries")
	}
	for _, output := range completedJob.OutputSSTs {
		if output.ID == "" || output.Bytes <= 0 || output.Level != completedJob.DestinationLevel {
			t.Fatalf("invalid compaction output summary=%+v destination=%d", output, completedJob.DestinationLevel)
		}
	}

	if _, err := compactor.refreshWithCurrent(ctx); err != nil {
		t.Fatalf("refresh: %v", err)
	}

	compactor.mu.Lock()
	m := compactor.manifest.Clone()
	compactor.mu.Unlock()

	if m.L0SSTCount() >= compactorOpts.Trigger.L0SSTCount {
		t.Errorf("L0 still has %d SSTs after compaction", m.L0SSTCount())
	}

	if len(m.Levels) == 0 {
		t.Error("no compacted level created after compaction")
	}

}

func TestCompactor_DataIntegrity(t *testing.T) {
	store := blobstore.NewMemory("test")
	ctx := context.Background()

	manifestStore := newManifestStore(store, nil)

	writerOpts := DefaultWriterOptions()
	writerOpts.Flush.Interval = 0
	writerOpts.Memtable.TargetBytes = 512

	writer, err := newWriter(ctx, store, manifestStore, writerOpts)
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}

	testData := make(map[string]string)
	for batch := 0; batch < 8; batch++ {
		for i := 0; i < 5; i++ {
			key := []byte{byte('a' + batch), byte('0' + i)}
			value := []byte{byte('v'), byte(batch), byte(i)}
			testData[string(key)] = string(value)
			if err := writer.put(ctx, key, value); err != nil {
				t.Fatalf("put: %v", err)
			}
		}
		if err := writer.flush(ctx); err != nil {
			t.Fatalf("flush: %v", err)
		}
	}
	writer.close(ctx)

	compactorOpts := defaultCompactorOptions()
	compactorOpts.Trigger.L0SSTCount = 4

	compactor, err := newCompactor(ctx, store, manifestStore, compactorOpts)
	if err != nil {
		t.Fatalf("newCompactor: %v", err)
	}

	if err := runCompactorUntilIdle(ctx, compactor); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	compactor.Close(ctx)

	reader, err := newReader(ctx, store, readerOptions{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("newReader: %v", err)
	}

	for key, expectedValue := range testData {
		value, found, err := reader.Get(ctx, []byte(key))
		if err != nil {
			t.Errorf("Get(%q): %v", key, err)
			continue
		}
		if !found {
			t.Errorf("Get(%q): not found", key)
			continue
		}
		if string(value) != expectedValue {
			t.Errorf("Get(%q) = %q, want %q", key, value, expectedValue)
		}
	}
}

func TestCompactor_TombstoneHandling(t *testing.T) {
	store := blobstore.NewMemory("test")
	ctx := context.Background()

	manifestStore := newManifestStore(store, nil)

	writerOpts := DefaultWriterOptions()
	writerOpts.Flush.Interval = 0
	writerOpts.Memtable.TargetBytes = 512

	writer, err := newWriter(ctx, store, manifestStore, writerOpts)
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}

	for batch := 0; batch < 4; batch++ {

		for i := 0; i < 5; i++ {
			key := []byte{byte('k'), byte(batch), byte(i)}
			value := []byte("value")
			if err := writer.put(ctx, key, value); err != nil {
				t.Fatalf("put: %v", err)
			}
		}
		if err := writer.flush(ctx); err != nil {
			t.Fatalf("flush: %v", err)
		}

		for i := 0; i < 3; i++ {
			key := []byte{byte('k'), byte(batch), byte(i)}
			if err := writer.delete(ctx, key); err != nil {
				t.Fatalf("delete: %v", err)
			}
		}
		if err := writer.flush(ctx); err != nil {
			t.Fatalf("flush: %v", err)
		}
	}
	writer.close(ctx)

	compactorOpts := defaultCompactorOptions()
	compactorOpts.Trigger.L0SSTCount = 4

	compactor, err := newCompactor(ctx, store, manifestStore, compactorOpts)
	if err != nil {
		t.Fatalf("newCompactor: %v", err)
	}

	if err := runCompactorUntilIdle(ctx, compactor); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	compactor.Close(ctx)

	reader, err := newReader(ctx, store, readerOptions{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("newReader: %v", err)
	}

	for batch := 0; batch < 4; batch++ {

		for i := 0; i < 3; i++ {
			key := []byte{byte('k'), byte(batch), byte(i)}
			_, found, err := reader.Get(ctx, key)
			if err != nil {
				t.Errorf("Get(%q): %v", key, err)
				continue
			}
			if found {
				t.Errorf("Get(%q) should not be found (was deleted)", key)
			}
		}

		for i := 3; i < 5; i++ {
			key := []byte{byte('k'), byte(batch), byte(i)}
			value, found, err := reader.Get(ctx, key)
			if err != nil {
				t.Errorf("Get(%q): %v", key, err)
				continue
			}
			if !found {
				t.Errorf("Get(%q): not found", key)
				continue
			}
			if string(value) != "value" {
				t.Errorf("Get(%q) = %q, want %q", key, value, "value")
			}
		}
	}
}

func TestCompactorRefreshesManifestState(t *testing.T) {
	store := blobstore.NewMemory("test")
	ctx := context.Background()

	manifestStore := newManifestStore(store, nil)

	compactor, err := newCompactor(ctx, store, manifestStore, defaultCompactorOptions())
	if err != nil {
		t.Fatalf("newCompactor: %v", err)
	}
	defer compactor.Close(ctx)

	writer, err := newWriter(ctx, store, manifestStore, DefaultWriterOptions())
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}

	if err := writer.put(ctx, []byte("key"), []byte("value")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := writer.flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	writer.close(ctx)

	if _, err := compactor.refreshWithCurrent(ctx); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	compactor.mu.Lock()
	l0Count := compactor.manifest.L0SSTCount()
	compactor.mu.Unlock()

	if l0Count == 0 {
		t.Error("compactor didn't see new L0 SST after refresh")
	}
}

func TestCompactor_MultipleSSTs(t *testing.T) {
	store := blobstore.NewMemory("test")
	ctx := context.Background()

	manifestStore := newManifestStore(store, nil)

	writerOpts := DefaultWriterOptions()
	writerOpts.Flush.Interval = 0
	writerOpts.Memtable.TargetBytes = 1024
	writerOpts.Memtable.MaxPendingMemtables = 128

	writer, err := newWriter(ctx, store, manifestStore, writerOpts)
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}

	for batch := 0; batch < 10; batch++ {
		for i := 0; i < 100; i++ {
			key := make([]byte, 32)
			key[0] = byte(batch)
			key[1] = byte(i)
			value := make([]byte, 512)
			for j := range value {
				value[j] = byte(batch ^ i ^ j)
			}
			if err := writer.put(ctx, key, value); err != nil {
				t.Fatalf("put: %v", err)
			}
		}
		if err := writer.flush(ctx); err != nil {
			t.Fatalf("flush: %v", err)
		}
	}
	writer.close(ctx)

	compactorOpts := defaultCompactorOptions()
	compactorOpts.Trigger.L0SSTCount = 4
	compactorOpts.Output.TargetSSTBytes = 4 * 1024

	compactor, err := newCompactor(ctx, store, manifestStore, compactorOpts)
	if err != nil {
		t.Fatalf("newCompactor: %v", err)
	}
	defer compactor.Close(ctx)

	if err := runCompactorUntilIdle(ctx, compactor); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	if _, err := compactor.refreshWithCurrent(ctx); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	compactor.mu.Lock()
	m := compactor.manifest.Clone()
	compactor.mu.Unlock()

	if len(m.Levels) == 0 {
		t.Fatal("no compacted level created after compaction")
	}

	totalSSTs := 0
	for _, level := range m.Levels {
		totalSSTs += len(level.SSTs)
	}

	if totalSSTs <= 1 {
		t.Errorf("expected multiple SSTs in compacted levels, got %d", totalSSTs)
	}
}

func TestConsecutiveCompaction_Integration(t *testing.T) {
	store := blobstore.NewMemory("test")
	ctx := context.Background()

	manifestStore := newManifestStore(store, nil)

	compactorOpts := compactorOptions{
		Trigger: compactionTriggerOptions{
			L0SSTCount:          2,
			BaseLevelBytes:      512 * 1024 * 1024,
			LevelSizeMultiplier: 8,
			MaxInputSSTs:        manifest.MaxRetiredObjectsPerEntry,
		},
		Output: compactionOutputOptions{
			BloomBitsPerKey: 10,
			BlockBytes:      1024,
			Compression:     "snappy",
			TargetSSTBytes:  64 * 1024,
		},
	}

	writerOpts := DefaultWriterOptions()
	writerOpts.Flush.Interval = 0
	writerOpts.Memtable.TargetBytes = 512

	expectedData := make(map[string]string)

	t.Run("Phase1_L0CompactionCreatesLevel", func(t *testing.T) {
		writer, err := newWriter(ctx, store, manifestStore, writerOpts)
		if err != nil {
			t.Fatalf("newWriter: %v", err)
		}

		for i := 0; i < 20; i++ {
			key := fmt.Sprintf("key-%03d", i)
			value := fmt.Sprintf("value-%03d-v1", i)
			expectedData[key] = value
			if err := writer.put(ctx, []byte(key), []byte(value)); err != nil {
				t.Fatalf("put: %v", err)
			}
		}
		if err := writer.flush(ctx); err != nil {
			t.Fatalf("flush: %v", err)
		}

		for i := 20; i < 40; i++ {
			key := fmt.Sprintf("key-%03d", i)
			value := fmt.Sprintf("value-%03d-v1", i)
			expectedData[key] = value
			if err := writer.put(ctx, []byte(key), []byte(value)); err != nil {
				t.Fatalf("put: %v", err)
			}
		}
		if err := writer.flush(ctx); err != nil {
			t.Fatalf("flush: %v", err)
		}
		writer.close(ctx)

		compactor, err := newCompactor(ctx, store, manifestStore, compactorOpts)
		if err != nil {
			t.Fatalf("newCompactor: %v", err)
		}

		if err := runCompactorUntilIdle(ctx, compactor); err != nil {
			t.Fatalf("RunOnce: %v", err)
		}

		if _, err := compactor.refreshWithCurrent(ctx); err != nil {
			t.Fatalf("Refresh: %v", err)
		}

		compactor.mu.Lock()
		m := compactor.manifest.Clone()
		compactor.mu.Unlock()
		compactor.Close(ctx)

		if m.L0SSTCount() != 0 {
			t.Errorf("expected 0 L0 SSTs after compaction, got %d", m.L0SSTCount())
		}
		if len(m.Levels) != 1 {
			t.Errorf("expected 1 compacted level, got %d", len(m.Levels))
		}

	})

	t.Run("Phase2_ReadCorrectnessAfterCompaction", func(t *testing.T) {
		reader, err := newReader(ctx, store, readerOptions{CacheDir: t.TempDir()})
		if err != nil {
			t.Fatalf("newReader: %v", err)
		}
		defer reader.Close()

		for key, expectedValue := range expectedData {
			value, found, err := reader.Get(ctx, []byte(key))
			if err != nil {
				t.Errorf("Get(%s): %v", key, err)
				continue
			}
			if !found {
				t.Errorf("Get(%s): not found", key)
				continue
			}
			if string(value) != expectedValue {
				t.Errorf("Get(%s) = %q, want %q", key, value, expectedValue)
			}
		}
	})

	t.Run("Phase3_NewerValuesOverrideOlder", func(t *testing.T) {
		writer, err := newWriter(ctx, store, manifestStore, writerOpts)
		if err != nil {
			t.Fatalf("newWriter: %v", err)
		}

		for i := 0; i < 20; i++ {
			key := fmt.Sprintf("key-%03d", i)
			value := fmt.Sprintf("value-%03d-v2-UPDATED", i)
			expectedData[key] = value
			if err := writer.put(ctx, []byte(key), []byte(value)); err != nil {
				t.Fatalf("put: %v", err)
			}
		}
		if err := writer.flush(ctx); err != nil {
			t.Fatalf("flush: %v", err)
		}

		for i := 0; i < 20; i++ {
			key := fmt.Sprintf("key-%03d", i)
			value := fmt.Sprintf("value-%03d-v3-LATEST", i)
			expectedData[key] = value
			if err := writer.put(ctx, []byte(key), []byte(value)); err != nil {
				t.Fatalf("put: %v", err)
			}
		}
		if err := writer.flush(ctx); err != nil {
			t.Fatalf("flush: %v", err)
		}
		writer.close(ctx)

		compactor, err := newCompactor(ctx, store, manifestStore, compactorOpts)
		if err != nil {
			t.Fatalf("newCompactor: %v", err)
		}

		if err := runCompactorUntilIdle(ctx, compactor); err != nil {
			t.Fatalf("RunOnce: %v", err)
		}

		if _, err := compactor.refreshWithCurrent(ctx); err != nil {
			t.Fatalf("Refresh: %v", err)
		}

		compactor.Close(ctx)

		reader, err := newReader(ctx, store, readerOptions{CacheDir: t.TempDir()})
		if err != nil {
			t.Fatalf("newReader: %v", err)
		}
		defer reader.Close()

		for key, expectedValue := range expectedData {
			value, found, err := reader.Get(ctx, []byte(key))
			if err != nil {
				t.Errorf("Get(%s): %v", key, err)
				continue
			}
			if !found {
				t.Errorf("Get(%s): not found", key)
				continue
			}
			if string(value) != expectedValue {
				t.Errorf("Get(%s) = %q, want %q", key, value, expectedValue)
			}
		}
	})

	t.Run("Phase4_ConsecutiveCompactionMergesSimilarRuns", func(t *testing.T) {
		for batch := 0; batch < 4; batch++ {
			writer, err := newWriter(ctx, store, manifestStore, writerOpts)
			if err != nil {
				t.Fatalf("newWriter: %v", err)
			}

			for i := 0; i < 30; i++ {
				key := fmt.Sprintf("batch%d-key-%03d", batch, i)
				value := fmt.Sprintf("batch%d-value-%03d", batch, i)
				expectedData[key] = value
				if err := writer.put(ctx, []byte(key), []byte(value)); err != nil {
					t.Fatalf("put: %v", err)
				}
			}
			if err := writer.flush(ctx); err != nil {
				t.Fatalf("flush: %v", err)
			}

			for i := 30; i < 60; i++ {
				key := fmt.Sprintf("batch%d-key-%03d", batch, i)
				value := fmt.Sprintf("batch%d-value-%03d", batch, i)
				expectedData[key] = value
				if err := writer.put(ctx, []byte(key), []byte(value)); err != nil {
					t.Fatalf("put: %v", err)
				}
			}
			if err := writer.flush(ctx); err != nil {
				t.Fatalf("flush: %v", err)
			}
			writer.close(ctx)

			compactor, err := newCompactor(ctx, store, manifestStore, compactorOpts)
			if err != nil {
				t.Fatalf("newCompactor: %v", err)
			}

			if err := runCompactorUntilIdle(ctx, compactor); err != nil {
				t.Fatalf("RunOnce: %v", err)
			}
			compactor.Close(ctx)
		}

		compactor, err := newCompactor(ctx, store, manifestStore, compactorOpts)
		if err != nil {
			t.Fatalf("newCompactor: %v", err)
		}

		compactor.Close(ctx)

		reader, err := newReader(ctx, store, readerOptions{CacheDir: t.TempDir()})
		if err != nil {
			t.Fatalf("newReader: %v", err)
		}
		defer reader.Close()

		var errors []string
		for key, expectedValue := range expectedData {
			value, found, err := reader.Get(ctx, []byte(key))
			if err != nil {
				errors = append(errors, fmt.Sprintf("Get(%s): %v", key, err))
				continue
			}
			if !found {
				errors = append(errors, fmt.Sprintf("Get(%s): not found", key))
				continue
			}
			if string(value) != expectedValue {
				errors = append(errors, fmt.Sprintf("Get(%s) = %q, want %q", key, value, expectedValue))
			}
		}

		if len(errors) > 0 {
			t.Errorf("Data integrity errors (%d total):", len(errors))
			for i, e := range errors {
				if i < 10 {
					t.Errorf("  %s", e)
				}
			}
			if len(errors) > 10 {
				t.Errorf("  ... and %d more errors", len(errors)-10)
			}
		}
	})

	t.Run("Phase5_DeletesRespected", func(t *testing.T) {
		writer, err := newWriter(ctx, store, manifestStore, writerOpts)
		if err != nil {
			t.Fatalf("newWriter: %v", err)
		}

		deletedKeys := make(map[string]bool)
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("key-%03d", i)
			deletedKeys[key] = true
			delete(expectedData, key)
			if err := writer.delete(ctx, []byte(key)); err != nil {
				t.Fatalf("delete: %v", err)
			}
		}
		if err := writer.flush(ctx); err != nil {
			t.Fatalf("flush: %v", err)
		}

		for i := 0; i < 20; i++ {
			key := fmt.Sprintf("padding-key-%03d", i)
			value := fmt.Sprintf("padding-value-%03d", i)
			expectedData[key] = value
			if err := writer.put(ctx, []byte(key), []byte(value)); err != nil {
				t.Fatalf("put: %v", err)
			}
		}
		if err := writer.flush(ctx); err != nil {
			t.Fatalf("flush: %v", err)
		}
		writer.close(ctx)

		compactor, err := newCompactor(ctx, store, manifestStore, compactorOpts)
		if err != nil {
			t.Fatalf("newCompactor: %v", err)
		}

		if err := runCompactorUntilIdle(ctx, compactor); err != nil {
			t.Fatalf("RunOnce: %v", err)
		}
		compactor.Close(ctx)

		reader, err := newReader(ctx, store, readerOptions{CacheDir: t.TempDir()})
		if err != nil {
			t.Fatalf("newReader: %v", err)
		}
		defer reader.Close()

		for key := range deletedKeys {
			_, found, err := reader.Get(ctx, []byte(key))
			if err != nil {
				t.Errorf("Get(%s): %v", key, err)
				continue
			}
			if found {
				t.Errorf("Get(%s): should be deleted but was found", key)
			}
		}

		for key, expectedValue := range expectedData {
			value, found, err := reader.Get(ctx, []byte(key))
			if err != nil {
				t.Errorf("Get(%s): %v", key, err)
				continue
			}
			if !found {
				t.Errorf("Get(%s): not found", key)
				continue
			}
			if string(value) != expectedValue {
				t.Errorf("Get(%s) = %q, want %q", key, value, expectedValue)
			}
		}
	})

	t.Run("Phase6_ScanWorksAfterCompaction", func(t *testing.T) {
		reader, err := newReader(ctx, store, readerOptions{CacheDir: t.TempDir()})
		if err != nil {
			t.Fatalf("newReader: %v", err)
		}
		defer reader.Close()

		results, err := reader.Scan(ctx, nil, nil)
		if err != nil {
			t.Fatalf("Scan: %v", err)
		}

		scanResults := make(map[string]string)
		for _, kv := range results {
			scanResults[string(kv.Key)] = string(kv.Value)
		}

		if len(scanResults) != len(expectedData) {
			t.Errorf("Scan returned %d keys, expected %d", len(scanResults), len(expectedData))
		}

		for key, expectedValue := range expectedData {
			if gotValue, ok := scanResults[key]; !ok {
				t.Errorf("Scan missing key: %s", key)
			} else if gotValue != expectedValue {
				t.Errorf("Scan(%s) = %q, want %q", key, gotValue, expectedValue)
			}
		}
	})
}

func TestConsecutiveCompaction_SequenceNumberCorrectness(t *testing.T) {
	store := blobstore.NewMemory("test")
	ctx := context.Background()

	manifestStore := newManifestStore(store, nil)

	compactorOpts := compactorOptions{
		Trigger: compactionTriggerOptions{
			L0SSTCount:          2,
			BaseLevelBytes:      512 * 1024 * 1024,
			LevelSizeMultiplier: 8,
			MaxInputSSTs:        manifest.MaxRetiredObjectsPerEntry,
		},
		Output: compactionOutputOptions{
			BloomBitsPerKey: 10,
			BlockBytes:      512,
			Compression:     "snappy",
			TargetSSTBytes:  64 * 1024,
		},
	}

	writerOpts := DefaultWriterOptions()
	writerOpts.Flush.Interval = 0
	writerOpts.Memtable.TargetBytes = 256
	writerOpts.Memtable.MaxPendingMemtables = 128

	writer1, err := newWriter(ctx, store, manifestStore, writerOpts)
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}

	if err := writer1.put(ctx, []byte("foo"), []byte("v1-old")); err != nil {
		t.Fatalf("put: %v", err)
	}
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("filler1-%03d", i)
		if err := writer1.put(ctx, []byte(key), []byte("filler-value-1")); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	if err := writer1.flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	for i := 50; i < 100; i++ {
		key := fmt.Sprintf("filler1-%03d", i)
		if err := writer1.put(ctx, []byte(key), []byte("filler-value-1")); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	if err := writer1.flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	writer1.close(ctx)

	compactor1, err := newCompactor(ctx, store, manifestStore, compactorOpts)
	if err != nil {
		t.Fatalf("newCompactor: %v", err)
	}
	if err := runCompactorUntilIdle(ctx, compactor1); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	compactor1.Close(ctx)

	writer2, err := newWriter(ctx, store, manifestStore, writerOpts)
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}

	if err := writer2.put(ctx, []byte("foo"), []byte("v2-new")); err != nil {
		t.Fatalf("put: %v", err)
	}
	for i := 0; i < 30; i++ {
		key := fmt.Sprintf("filler2-%03d", i)
		if err := writer2.put(ctx, []byte(key), []byte("short")); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	if err := writer2.flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	for i := 30; i < 60; i++ {
		key := fmt.Sprintf("filler2-%03d", i)
		if err := writer2.put(ctx, []byte(key), []byte("short")); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	if err := writer2.flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	writer2.close(ctx)

	compactor2, err := newCompactor(ctx, store, manifestStore, compactorOpts)
	if err != nil {
		t.Fatalf("newCompactor: %v", err)
	}
	if err := runCompactorUntilIdle(ctx, compactor2); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	compactor2.Close(ctx)

	reader, err := newReader(ctx, store, readerOptions{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("newReader: %v", err)
	}
	defer reader.Close()

	value, found, err := reader.Get(ctx, []byte("foo"))
	if err != nil {
		t.Fatalf("Get(foo): %v", err)
	}
	if !found {
		t.Fatal("Get(foo): not found")
	}
	if string(value) != "v2-new" {
		t.Errorf("Get(foo) = %q, want %q - SEQUENCE NUMBER NOT RESPECTED!", value, "v2-new")
	}
}

func TestCompactor_ValidateSSTChecksum(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("compactor-checksum")
	manifestStore := newManifestStore(store, nil)

	wOpts := DefaultWriterOptions()
	wOpts.Flush.Interval = 0
	w, err := newWriter(ctx, store, manifestStore, wOpts)
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}

	if err := w.put(ctx, []byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := w.put(ctx, []byte("k2"), []byte("v2")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := w.flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	w.close(ctx)

	m, err := manifestStore.Replay(ctx)
	if err != nil {
		t.Fatalf("replay manifest: %v", err)
	}
	if len(m.L0SSTs) == 0 {
		t.Fatalf("expected L0 SSTs in manifest")
	}

	newest := m.L0SSTs[0]
	path := store.SSTPath(newest.ID)
	data, _, err := store.Read(ctx, path)
	if err != nil {
		t.Fatalf("read sst: %v", err)
	}
	if len(data) == 0 {
		t.Fatalf("unexpected empty sst data")
	}
	data[0] ^= 0xFF
	if _, err := store.Write(ctx, path, data); err != nil {
		t.Fatalf("write corrupt sst: %v", err)
	}

	cOpts := defaultCompactorOptions()
	cOpts.Safety.ValidateSSTChecksum = true
	cOpts.Trigger.L0SSTCount = 1
	c, err := newCompactor(ctx, store, manifestStore, cOpts)
	if err != nil {
		t.Fatalf("newCompactor: %v", err)
	}
	defer c.Close(ctx)

	if err := runCompactorUntilIdle(ctx, c); err == nil {
		t.Fatalf("expected compaction to fail on checksum mismatch")
	}
}

func TestConsecutiveCompaction_MergePreservesData(t *testing.T) {
	store := blobstore.NewMemory("test")
	ctx := context.Background()

	manifestStore := newManifestStore(store, nil)

	compactorOpts := compactorOptions{
		Trigger: compactionTriggerOptions{
			L0SSTCount:          1,
			BaseLevelBytes:      512 * 1024 * 1024,
			LevelSizeMultiplier: 8,
			MaxInputSSTs:        manifest.MaxRetiredObjectsPerEntry,
		},
		Output: compactionOutputOptions{
			BloomBitsPerKey: 10,
			BlockBytes:      512,
			Compression:     "snappy",
			TargetSSTBytes:  64 * 1024,
		},
	}

	writerOpts := DefaultWriterOptions()
	writerOpts.Flush.Interval = 0
	writerOpts.Memtable.TargetBytes = 4 * 1024

	expectedData := make(map[string]string)

	for batch := 0; batch < 4; batch++ {
		writer, err := newWriter(ctx, store, manifestStore, writerOpts)
		if err != nil {
			t.Fatalf("newWriter: %v", err)
		}

		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("batch%d-key-%05d", batch, i)
			value := fmt.Sprintf("batch%d-value-%05d", batch, i)
			expectedData[key] = value
			if err := writer.put(ctx, []byte(key), []byte(value)); err != nil {
				t.Fatalf("put: %v", err)
			}
		}
		if err := writer.flush(ctx); err != nil {
			t.Fatalf("flush: %v", err)
		}
		writer.close(ctx)

		compactor, err := newCompactor(ctx, store, manifestStore, compactorOpts)
		if err != nil {
			t.Fatalf("newCompactor: %v", err)
		}
		if err := runCompactorUntilIdle(ctx, compactor); err != nil {
			t.Fatalf("RunOnce: %v", err)
		}

		compactor.Close(ctx)
	}

	reader, err := newReader(ctx, store, readerOptions{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("newReader: %v", err)
	}
	defer reader.Close()

	var missing, wrong int
	for key, expectedValue := range expectedData {
		value, found, err := reader.Get(ctx, []byte(key))
		if err != nil {
			t.Errorf("Get(%s): %v", key, err)
			continue
		}
		if !found {
			missing++
			if missing <= 5 {
				t.Errorf("Get(%s): not found", key)
			}
			continue
		}
		if string(value) != expectedValue {
			wrong++
			if wrong <= 5 {
				t.Errorf("Get(%s) = %q, want %q", key, value, expectedValue)
			}
		}
	}

	if missing > 0 || wrong > 0 {
		t.Errorf("Data integrity issues: %d missing, %d wrong values out of %d total keys",
			missing, wrong, len(expectedData))
	}

	results, err := reader.Scan(ctx, nil, nil)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}

	if len(results) != len(expectedData) {
		t.Errorf("Scan returned %d keys, expected %d", len(results), len(expectedData))
	}

	for i := 1; i < len(results); i++ {
		if bytes.Compare(results[i-1].Key, results[i].Key) >= 0 {
			t.Errorf("Scan results not sorted at position %d: %q >= %q",
				i, results[i-1].Key, results[i].Key)
			break
		}
	}
}

func TestCompactorCommitsRetirementRecords(t *testing.T) {
	store := blobstore.NewMemory("")
	defer store.Close()
	ctx := context.Background()

	manifestStore := newManifestStore(store, nil)

	writerOpts := DefaultWriterOptions()
	writerOpts.Flush.Interval = 0
	writer, err := newWriter(ctx, store, manifestStore, writerOpts)
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}
	defer writer.close(ctx)

	for i := 0; i < 6; i++ {
		if err := writer.put(ctx, []byte("mark-key"), []byte(fmt.Sprintf("value-%03d", i))); err != nil {
			t.Fatalf("put: %v", err)
		}
		if err := writer.flush(ctx); err != nil {
			t.Fatalf("flush: %v", err)
		}
	}

	before, err := manifestStore.Replay(ctx)
	if err != nil {
		t.Fatalf("replay manifest before compaction: %v", err)
	}
	if len(before.L0SSTs) == 0 {
		t.Fatalf("expected L0 SSTs before compaction")
	}

	compactorOpts := defaultCompactorOptions()
	compactorOpts.Trigger.L0SSTCount = 2

	compactor, err := newCompactor(ctx, store, manifestStore, compactorOpts)
	if err != nil {
		t.Fatalf("newCompactor: %v", err)
	}
	defer compactor.Close(ctx)

	if err := runCompactorUntilIdle(ctx, compactor); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	seqs, err := manifestStore.ListEntries(ctx)
	if err != nil {
		t.Fatalf("list manifest entries: %v", err)
	}
	retired := make(map[string]manifest.RetiredObject)
	for _, seq := range seqs {
		entry, err := manifestStore.ReadEntry(ctx, seq)
		if err != nil {
			t.Fatalf("read manifest entry %d: %v", seq, err)
		}
		for _, object := range entry.RetiredObjects {
			retired[object.ID] = object
		}
	}
	for _, sst := range before.L0SSTs {
		object, found := retired[sst.ID]
		if !found {
			t.Fatalf("missing retirement record for %s", sst.ID)
		}
		if object.Key != store.SSTPath(sst.ID) {
			t.Fatalf("invalid retirement record for %s: %+v", sst.ID, object)
		}
	}
}
