package isledb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
)

const (
	maintenancePressureKeyCount    = 1024
	maintenancePressureGenerations = 18
)

// TestMaintenancePressureExactWriteReadCorrectness keeps compaction and
// checkpointing eligible while writes, refreshes, scans, point reads, and a
// pinned snapshot exercise the same database. Every committed generation is
// compared with an immutable in-memory oracle; no eventually-consistent
// allowance is used.
func TestMaintenancePressureExactWriteReadCorrectness(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	store := blobstore.NewMemory("maintenance-pressure-exact")
	defer store.Close()

	output := SSTOutputOptions{
		L0: SSTEncodingOptions{
			Compression:     "none",
			BlockBytes:      1024,
			BloomBitsPerKey: 10,
		},
		Compacted: SSTEncodingOptions{
			Compression:     "zstd",
			BlockBytes:      4 << 10,
			BloomBitsPerKey: 10,
		},
	}
	db, err := openDB(ctx, store, dbOpenOptions{sstOutput: output})
	if err != nil {
		t.Fatalf("open DB: %v", err)
	}
	defer db.Close()

	writerOpts := DefaultWriterOptions()
	writerOpts.OwnerID = "maintenance-pressure-writer"
	writerOpts.Flush.Interval = 0
	writerOpts.Memtable.TargetBytes = 32 << 10
	writerOpts.Memtable.MaxPendingMemtables = 128
	writerOpts.Maintenance.PollInterval = time.Hour
	writer, err := db.OpenWriter(ctx, writerOpts)
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}
	defer func() { _ = writer.Close(context.Background()) }()

	reader, err := db.OpenReader(ctx, ReaderOpenOptions{
		CacheDir:       t.TempDir(),
		SSTCacheSize:   64 << 20,
		BlockCacheSize: 4 << 20,
	})
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	defer reader.Close()

	counters := &maintenancePressureCounters{}
	maintenanceErrors := make(chan error, 1)
	maintenanceOpts := maintenancePressureOptions()
	maintenanceOpts.OnCycle = counters.observe
	maintenanceOpts.OnError = func(err error) {
		select {
		case maintenanceErrors <- err:
		default:
		}
	}
	maintenance, err := db.OpenMaintenance(ctx, maintenanceOpts)
	if err != nil {
		t.Fatalf("open maintenance: %v", err)
	}
	defer func() { _ = maintenance.Close(context.Background()) }()

	runCtx, stopMaintenance := context.WithCancel(ctx)
	maintenanceDone := make(chan error, 1)
	go func() { maintenanceDone <- maintenance.Run(runCtx) }()

	checks := make(chan maintenancePressureCheck)
	checkerDone := make(chan struct{})
	go func() {
		defer close(checkerDone)
		for {
			select {
			case <-ctx.Done():
				return
			case check, ok := <-checks:
				if !ok {
					return
				}
				err := reader.Refresh(ctx)
				if err == nil {
					err = verifyMaintenancePressureReader(ctx, reader, check.expected)
				}
				check.result <- err
			}
		}
	}()

	expected := make(map[string][]byte, maintenancePressureKeyCount)
	var pending *maintenancePressureCheck
	var pinned *Snapshot
	var pinnedExpected map[string][]byte

	for generation := 0; generation < maintenancePressureGenerations; generation++ {
		applyMaintenancePressureGeneration(t, ctx, writer, expected, generation)

		// The next generation is allowed to fill while the reader checks the
		// preceding committed generation. It cannot become visible until this
		// barrier succeeds and the next Flush starts.
		if pending != nil {
			if err := <-pending.result; err != nil {
				t.Fatalf("verify committed generation %d: %v", pending.generation, err)
			}
			if pending.generation == 3 {
				pinned, err = reader.Snapshot(ctx)
				if err != nil {
					t.Fatalf("pin generation %d: %v", pending.generation, err)
				}
				pinnedExpected = cloneMaintenancePressureState(pending.expected)
			}
		}
		assertNoMaintenancePressureError(t, maintenanceErrors)

		if err := writer.Flush(ctx); err != nil {
			t.Fatalf("flush generation %d: %v", generation, err)
		}
		check := maintenancePressureCheck{
			generation: generation,
			expected:   cloneMaintenancePressureState(expected),
			result:     make(chan error, 1),
		}
		checks <- check
		pending = &check
	}
	if pending != nil {
		if err := <-pending.result; err != nil {
			t.Fatalf("verify committed generation %d: %v", pending.generation, err)
		}
	}
	close(checks)
	<-checkerDone
	assertNoMaintenancePressureError(t, maintenanceErrors)

	stopMaintenance()
	if err := <-maintenanceDone; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("background maintenance: %v", err)
	}
	drainMaintenancePressure(t, ctx, maintenance, writer, counters)
	assertNoMaintenancePressureError(t, maintenanceErrors)

	if pinned == nil {
		t.Fatal("pressure workload did not create the pinned generation")
	}
	if err := verifyMaintenancePressureSnapshot(ctx, pinned, pinnedExpected); err != nil {
		t.Fatalf("pinned generation changed after later maintenance: %v", err)
	}
	if err := pinned.Close(); err != nil {
		t.Fatalf("close pinned snapshot: %v", err)
	}

	if err := reader.Refresh(ctx); err != nil {
		t.Fatalf("refresh final reader: %v", err)
	}
	if err := verifyMaintenancePressureReader(ctx, reader, expected); err != nil {
		t.Fatalf("verify final reader: %v", err)
	}
	assertMaintenancePressureExecuted(t, counters)
	assertMaintenancePressureTopology(t, ctx, db, maintenanceOpts)

	if err := maintenance.Close(ctx); err != nil {
		t.Fatalf("close maintenance: %v", err)
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("close reader: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close DB: %v", err)
	}

	reopened, err := openDB(ctx, store, dbOpenOptions{sstOutput: output})
	if err != nil {
		t.Fatalf("reopen DB: %v", err)
	}
	defer reopened.Close()
	reopenedReader, err := reopened.OpenReader(ctx, ReaderOpenOptions{
		CacheDir:       t.TempDir(),
		SSTCacheSize:   64 << 20,
		BlockCacheSize: 4 << 20,
	})
	if err != nil {
		t.Fatalf("open reader after restart: %v", err)
	}
	defer reopenedReader.Close()
	if err := verifyMaintenancePressureReader(ctx, reopenedReader, expected); err != nil {
		t.Fatalf("verify reader after restart: %v", err)
	}
	state, err := reopened.manifestStore.Replay(ctx)
	if err != nil {
		t.Fatalf("replay manifest after restart: %v", err)
	}
	if err := state.ValidateLevels(); err != nil {
		t.Fatalf("validate levels after restart: %v", err)
	}
}

type maintenancePressureCheck struct {
	generation int
	expected   map[string][]byte
	result     chan error
}

type maintenancePressureCounters struct {
	cycles                 atomic.Uint64
	waitingForWriter       atomic.Uint64
	compactionSelections   atomic.Uint64
	l0Selections           atomic.Uint64
	lowerLevelSelections   atomic.Uint64
	criticalL0Selections   atomic.Uint64
	checkpointSelections   atomic.Uint64
	checkpointStaged       atomic.Uint64
	completedCompactions   atomic.Uint64
	maxObservedReplayPages atomic.Uint64
}

func (c *maintenancePressureCounters) observe(stats MaintenanceStats) {
	c.cycles.Add(1)
	if stats.State == MaintenanceWaitingForWriter {
		c.waitingForWriter.Add(1)
	}
	switch stats.Scheduling.Selected {
	case MaintenanceTaskSSTCompaction:
		c.compactionSelections.Add(1)
		if stats.Scheduling.CompactionSourceLevel == 0 {
			c.l0Selections.Add(1)
			if stats.Scheduling.CompactionCritical {
				c.criticalL0Selections.Add(1)
			}
		} else {
			c.lowerLevelSelections.Add(1)
		}
	case MaintenanceTaskManifestCheckpoint:
		c.checkpointSelections.Add(1)
	}
	if stats.ManifestCheckpoint.Staged {
		c.checkpointStaged.Add(1)
	}
	if stats.SSTCompaction.Jobs > 0 {
		c.completedCompactions.Add(uint64(stats.SSTCompaction.Jobs))
	}
	atomicMaxUint64(&c.maxObservedReplayPages, stats.Scheduling.ReplayPages)
}

func maintenancePressureOptions() MaintenanceOptions {
	opts := DefaultMaintenanceOptions()
	opts.IdleInterval = time.Millisecond
	opts.SSTCompaction.ReadConcurrency = 4
	opts.SSTCompaction.L0TriggerSSTs = 4
	opts.SSTCompaction.BaseLevelBytes = 64 << 10
	opts.SSTCompaction.LevelGrowthFactor = 2
	opts.SSTCompaction.MaxInputSSTsPerJob = 24
	opts.SSTCompaction.MaxInputBytesPerJob = 256 << 10
	opts.SSTCompaction.TargetSSTBytes = 64 << 10
	opts.ManifestCheckpoint.TargetReplayPages = 2
	opts.ManifestCheckpoint.TargetReplayBytes = 8 << 10
	return opts
}

func applyMaintenancePressureGeneration(
	t *testing.T,
	ctx context.Context,
	writer *Writer,
	expected map[string][]byte,
	generation int,
) {
	t.Helper()
	for index := 0; index < maintenancePressureKeyCount; index++ {
		key := maintenancePressureKey(index)

		// Hot keys deliberately carry multiple versions in one flush. Only the
		// last sequence is allowed to survive reads and compaction.
		if index < 16 && generation > 0 {
			intermediate := maintenancePressureValue(generation-1, index, 97)
			if err := writer.Put(ctx, key, intermediate); err != nil {
				t.Fatalf("generation %d intermediate Put(%d): %v", generation, index, err)
			}
			if err := writer.Delete(ctx, key); err != nil {
				t.Fatalf("generation %d intermediate Delete(%d): %v", generation, index, err)
			}
		}

		if (index*17+generation*31)%13 == 0 {
			if err := writer.Delete(ctx, key); err != nil {
				t.Fatalf("generation %d Delete(%d): %v", generation, index, err)
			}
			delete(expected, string(key))
			continue
		}

		size := []int{0, 31, 257, 2048}[(index+generation)%4]
		if (index+generation*7)%127 == 0 {
			size = 64 << 10
		}
		value := maintenancePressureValue(generation, index, size)
		var err error
		if (index+generation)%17 == 0 {
			err = writer.PutWithTTL(ctx, key, value, time.Hour)
		} else {
			err = writer.Put(ctx, key, value)
		}
		if err != nil {
			t.Fatalf("generation %d Put(%d,%d bytes): %v", generation, index, size, err)
		}
		expected[string(key)] = append([]byte(nil), value...)
	}
}

func drainMaintenancePressure(
	t *testing.T,
	ctx context.Context,
	maintenance *Maintenance,
	writer *Writer,
	counters *maintenancePressureCounters,
) {
	t.Helper()
	const maxCycles = 2000
	for cycle := 0; cycle < maxCycles; cycle++ {
		stats, err := maintenance.RunOnce(ctx)
		if err != nil {
			t.Fatalf("drain maintenance cycle %d: %v", cycle, err)
		}
		counters.observe(stats)

		head, _, err := maintenance.manifestLog.ReadMaintenanceHead(ctx)
		if err != nil {
			t.Fatalf("read maintenance HEAD at cycle %d: %v", cycle, err)
		}
		if head != nil && head.Pending != nil {
			if err := writer.Flush(ctx); err != nil {
				t.Fatalf("apply maintenance cycle %d: %v", cycle, err)
			}
			continue
		}
		if stats.State == MaintenanceIdle && stats.Scheduling.Selected == MaintenanceTaskNone {
			return
		}
	}
	t.Fatalf("maintenance pressure did not drain after %d cycles", maxCycles)
}

func verifyMaintenancePressureReader(ctx context.Context, reader *Reader, expected map[string][]byte) error {
	rows, err := reader.Scan(ctx, nil, nil)
	if err != nil {
		return fmt.Errorf("Scan: %w", err)
	}
	if err := verifyMaintenancePressureRows(rows, expected); err != nil {
		return err
	}
	for index := 0; index < maintenancePressureKeyCount; index++ {
		key := maintenancePressureKey(index)
		got, found, err := reader.Get(ctx, key)
		if err != nil {
			return fmt.Errorf("Get(%q): %w", key, err)
		}
		want, wantFound := expected[string(key)]
		if found != wantFound || (found && !bytes.Equal(got, want)) {
			return fmt.Errorf("Get(%q): found=%v bytes=%d, want found=%v bytes=%d",
				key, found, len(got), wantFound, len(want))
		}
	}
	if _, found, err := reader.Get(ctx, []byte("pressure/key/missing")); err != nil {
		return fmt.Errorf("Get(missing): %w", err)
	} else if found {
		return errors.New("Get(missing) unexpectedly found a value")
	}
	return nil
}

func verifyMaintenancePressureSnapshot(ctx context.Context, snapshot *Snapshot, expected map[string][]byte) error {
	rows, err := snapshot.ScanLimit(ctx, nil, nil, maintenancePressureKeyCount+1)
	if err != nil {
		return fmt.Errorf("ScanLimit: %w", err)
	}
	if err := verifyMaintenancePressureRows(rows, expected); err != nil {
		return err
	}
	for index := 0; index < maintenancePressureKeyCount; index++ {
		key := maintenancePressureKey(index)
		got, found, err := snapshot.Get(ctx, key)
		if err != nil {
			return fmt.Errorf("Get(%q): %w", key, err)
		}
		want, wantFound := expected[string(key)]
		if found != wantFound || (found && !bytes.Equal(got, want)) {
			return fmt.Errorf("Get(%q): found=%v bytes=%d, want found=%v bytes=%d",
				key, found, len(got), wantFound, len(want))
		}
	}
	return nil
}

func verifyMaintenancePressureRows(rows []KV, expected map[string][]byte) error {
	keys := make([]string, 0, len(expected))
	for key := range expected {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if len(rows) != len(keys) {
		return fmt.Errorf("scan rows=%d, want=%d", len(rows), len(keys))
	}
	for index, key := range keys {
		if string(rows[index].Key) != key {
			return fmt.Errorf("scan key[%d]=%q, want=%q", index, rows[index].Key, key)
		}
		if !bytes.Equal(rows[index].Value, expected[key]) {
			return fmt.Errorf("scan value[%d] for %q has %d bytes, want=%d",
				index, key, len(rows[index].Value), len(expected[key]))
		}
	}
	return nil
}

func assertMaintenancePressureExecuted(t *testing.T, counters *maintenancePressureCounters) {
	t.Helper()
	if counters.cycles.Load() == 0 || counters.waitingForWriter.Load() == 0 {
		t.Fatalf("maintenance did not exercise publication pressure: cycles=%d waiting=%d",
			counters.cycles.Load(), counters.waitingForWriter.Load())
	}
	if counters.compactionSelections.Load() == 0 || counters.completedCompactions.Load() == 0 {
		t.Fatalf("maintenance did not compact: selected=%d completed=%d",
			counters.compactionSelections.Load(), counters.completedCompactions.Load())
	}
	if counters.l0Selections.Load() == 0 || counters.lowerLevelSelections.Load() == 0 {
		t.Fatalf("maintenance did not exercise both L0 and lower levels: l0=%d lower=%d",
			counters.l0Selections.Load(), counters.lowerLevelSelections.Load())
	}
	if counters.criticalL0Selections.Load() == 0 {
		t.Fatal("maintenance never observed critical L0 pressure")
	}
	if counters.checkpointSelections.Load() == 0 || counters.checkpointStaged.Load() == 0 {
		t.Fatalf("maintenance did not checkpoint: selected=%d staged=%d",
			counters.checkpointSelections.Load(), counters.checkpointStaged.Load())
	}
	if counters.maxObservedReplayPages.Load() < 2 {
		t.Fatalf("checkpoint replay pressure never reached threshold: max_pages=%d",
			counters.maxObservedReplayPages.Load())
	}
}

func assertMaintenancePressureTopology(
	t *testing.T,
	ctx context.Context,
	db *DB,
	opts MaintenanceOptions,
) {
	t.Helper()
	state, err := db.manifestStore.Replay(ctx)
	if err != nil {
		t.Fatalf("replay manifest: %v", err)
	}
	if err := state.ValidateLevels(); err != nil {
		t.Fatalf("validate levels: %v", err)
	}
	if state.L0SSTCount() >= opts.SSTCompaction.L0TriggerSSTs {
		t.Fatalf("L0 remained eligible after drain: count=%d trigger=%d",
			state.L0SSTCount(), opts.SSTCompaction.L0TriggerSSTs)
	}
	if len(state.Levels) < 2 {
		t.Fatalf("lower-level pressure did not create multiple levels: levels=%d", len(state.Levels))
	}
	current := db.manifestStore.CurrentData()
	if current == nil || current.Snapshot == nil {
		t.Fatal("checkpoint pressure did not publish a snapshot")
	}
	if current.StateReplayPages >= opts.ManifestCheckpoint.TargetReplayPages ||
		current.StateReplayBytes >= opts.ManifestCheckpoint.TargetReplayBytes {
		t.Fatalf("checkpoint pressure remained after drain: pages=%d/%d bytes=%d/%d",
			current.StateReplayPages, opts.ManifestCheckpoint.TargetReplayPages,
			current.StateReplayBytes, opts.ManifestCheckpoint.TargetReplayBytes)
	}
	head, _, err := db.manifestStore.ReadMaintenanceHead(ctx)
	if err != nil {
		t.Fatalf("read maintenance HEAD: %v", err)
	}
	if head != nil && head.Pending != nil {
		t.Fatalf("maintenance command remained pending after drain: %+v", head.Pending)
	}
}

func maintenancePressureKey(index int) []byte {
	return []byte(fmt.Sprintf("pressure/key/%06d", index))
}

func maintenancePressureValue(generation, index, size int) []byte {
	value := make([]byte, size)
	for offset := range value {
		value[offset] = byte(generation*37 + index*19 + offset*11)
	}
	return value
}

func cloneMaintenancePressureState(state map[string][]byte) map[string][]byte {
	clone := make(map[string][]byte, len(state))
	for key, value := range state {
		clone[key] = append([]byte(nil), value...)
	}
	return clone
}

func assertNoMaintenancePressureError(t *testing.T, errorsCh <-chan error) {
	t.Helper()
	select {
	case err := <-errorsCh:
		t.Fatalf("maintenance cycle error: %v", err)
	default:
	}
}

func atomicMaxUint64(value *atomic.Uint64, candidate uint64) {
	for current := value.Load(); candidate > current; current = value.Load() {
		if value.CompareAndSwap(current, candidate) {
			return
		}
	}
}
