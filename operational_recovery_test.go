package isledb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/manifest"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

var (
	errOperationalPublishFailure = errors.New("injected manifest publish failure")
	errOperationalLostResponse   = errors.New("injected lost manifest response")
)

// operationalCASStorage injects failures at the CURRENT visibility boundary
// while forwarding immutable page operations to the real backend.
type operationalCASStorage struct {
	*manifest.BlobStoreBackend

	mu                    sync.Mutex
	failBeforeNext        error
	failAfterNext         error
	conflictsPerCommit    int
	conflictsRemaining    int
	injectedConflictCount int
}

func (s *operationalCASStorage) WriteCurrentCAS(
	ctx context.Context,
	data []byte,
	expectedETag string,
) (string, error) {
	s.mu.Lock()
	if err := s.failBeforeNext; err != nil {
		s.failBeforeNext = nil
		s.mu.Unlock()
		return "", err
	}
	if s.conflictsRemaining > 0 {
		s.conflictsRemaining--
		s.injectedConflictCount++
		s.mu.Unlock()
		return "", manifest.ErrPreconditionFailed
	}
	s.mu.Unlock()

	etag, err := s.BlobStoreBackend.WriteCurrentCAS(ctx, data, expectedETag)
	if err != nil {
		return "", err
	}

	s.mu.Lock()
	afterErr := s.failAfterNext
	s.failAfterNext = nil
	s.conflictsRemaining = s.conflictsPerCommit
	s.mu.Unlock()
	if afterErr != nil {
		return "", afterErr
	}
	return etag, nil
}

func (s *operationalCASStorage) failBeforeNextCAS(err error) {
	s.mu.Lock()
	s.failBeforeNext = err
	s.mu.Unlock()
}

func (s *operationalCASStorage) failAfterNextCAS(err error) {
	s.mu.Lock()
	s.failAfterNext = err
	s.mu.Unlock()
}

func (s *operationalCASStorage) injectConflictsPerCommit(count int) {
	s.mu.Lock()
	s.conflictsPerCommit = count
	s.conflictsRemaining = count
	s.mu.Unlock()
}

func (s *operationalCASStorage) conflictCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.injectedConflictCount
}

func TestOperationalRecovery_RestartAfterUnpublishedBackgroundFlush(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store := blobstore.NewMemory("operational-restart")
	defer store.Close()
	faults := &operationalCASStorage{BlobStoreBackend: manifest.NewBlobStoreBackend(store)}

	db, err := openDB(ctx, store, dbOpenOptions{manifestStorage: faults})
	if err != nil {
		t.Fatalf("open first db: %v", err)
	}

	backgroundErr := make(chan error, 1)
	opts := DefaultWriterOptions()
	opts.OwnerID = "operational-writer-before-crash"
	opts.Flush.Interval = 5 * time.Millisecond
	opts.SST.Compression = "none"
	opts.OnFlushError = func(err error) { backgroundErr <- err }
	writer, err := db.OpenWriter(ctx, opts)
	if err != nil {
		t.Fatalf("open first writer: %v", err)
	}

	if err := writer.Put(ctx, []byte("stable"), []byte("before-crash")); err != nil {
		t.Fatalf("put stable value: %v", err)
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatalf("flush stable value: %v", err)
	}

	faults.failBeforeNextCAS(errOperationalPublishFailure)
	if err := writer.Put(ctx, []byte("uncommitted"), []byte("must-not-appear")); err != nil {
		t.Fatalf("put uncommitted value: %v", err)
	}

	select {
	case err := <-backgroundErr:
		if !errors.Is(err, ErrWriterFailed) || !errors.Is(err, errOperationalPublishFailure) {
			t.Fatalf("background error=%v, want writer failure wrapping injected failure", err)
		}
	case <-ctx.Done():
		t.Fatalf("wait for background failure: %v", ctx.Err())
	}
	if err := writer.Close(ctx); !errors.Is(err, ErrWriterFailed) {
		t.Fatalf("close failed writer error=%v, want %v", err, ErrWriterFailed)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close first db: %v", err)
	}

	liveBeforeRestart := replayManifestForTest(t, ctx, store)
	physicalBeforeRestart, err := store.ListSSTFiles(ctx)
	if err != nil {
		t.Fatalf("list SSTs before restart: %v", err)
	}
	if got, want := len(liveBeforeRestart.AllSSTIDs()), 1; got != want {
		t.Fatalf("live SSTs before restart=%d, want=%d", got, want)
	}
	if got, want := len(physicalBeforeRestart), 2; got != want {
		t.Fatalf("physical SSTs before restart=%d, want=%d", got, want)
	}

	restarted, err := openDB(ctx, store, dbOpenOptions{})
	if err != nil {
		t.Fatalf("open restarted db: %v", err)
	}
	defer restarted.Close()
	restartedOpts := DefaultWriterOptions()
	restartedOpts.OwnerID = "operational-writer-after-crash"
	restartedOpts.Flush.Interval = 0
	restartedOpts.SST.Compression = "none"
	restartedWriter, err := restarted.OpenWriter(ctx, restartedOpts)
	if err != nil {
		t.Fatalf("open restarted writer: %v", err)
	}
	if err := restartedWriter.Put(ctx, []byte("recovered"), []byte("after-crash")); err != nil {
		t.Fatalf("put recovered value: %v", err)
	}
	if err := restartedWriter.Close(ctx); err != nil {
		t.Fatalf("close restarted writer: %v", err)
	}

	reader := openReaderFromDBForTest(t, ctx, store, DefaultReaderOpenOptions(t.TempDir()))
	defer reader.Close()
	assertReaderValue(t, ctx, reader, "stable", "before-crash", true)
	assertReaderValue(t, ctx, reader, "recovered", "after-crash", true)
	assertReaderValue(t, ctx, reader, "uncommitted", "", false)
}

func TestOperationalRecovery_LostManifestResponseIsIdempotent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store := blobstore.NewMemory("operational-lost-response")
	defer store.Close()
	faults := &operationalCASStorage{BlobStoreBackend: manifest.NewBlobStoreBackend(store)}
	db, err := openDB(ctx, store, dbOpenOptions{manifestStorage: faults})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	opts := DefaultWriterOptions()
	opts.OwnerID = "operational-lost-response-writer"
	opts.Flush.Interval = 0
	opts.SST.Compression = "none"
	writer, err := db.OpenWriter(ctx, opts)
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}
	if err := writer.Put(ctx, []byte("ambiguous"), []byte("committed-once")); err != nil {
		t.Fatalf("put value: %v", err)
	}

	faults.failAfterNextCAS(errOperationalLostResponse)
	if err := writer.Flush(ctx); !errors.Is(err, errOperationalLostResponse) {
		t.Fatalf("first flush error=%v, want %v", err, errOperationalLostResponse)
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatalf("reconcile flush: %v", err)
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	manifestStore := manifest.NewStore(store)
	seqs, err := manifestStore.ListEntries(ctx)
	if err != nil {
		t.Fatalf("list manifest entries: %v", err)
	}
	addEntries := 0
	for _, seq := range seqs {
		entry, err := manifestStore.ReadEntry(ctx, seq)
		if err != nil {
			t.Fatalf("read manifest entry %d: %v", seq, err)
		}
		if entry.Op == manifest.LogOpAddSSTable {
			addEntries++
		}
	}
	if addEntries != 1 {
		t.Fatalf("committed add-SST entries=%d, want=1", addEntries)
	}
	physical, err := store.ListSSTFiles(ctx)
	if err != nil {
		t.Fatalf("list physical SSTs: %v", err)
	}
	if len(physical) != 1 {
		t.Fatalf("physical SSTs=%d, want=1", len(physical))
	}

	reader := openReaderFromDBForTest(t, ctx, store, DefaultReaderOpenOptions(t.TempDir()))
	defer reader.Close()
	assertReaderValue(t, ctx, reader, "ambiguous", "committed-once", true)
}

func TestOperationalRecovery_SustainedCASConflictsAcrossWriteAndMaintenance(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	store := blobstore.NewMemory("operational-cas-contention")
	defer store.Close()
	faults := &operationalCASStorage{BlobStoreBackend: manifest.NewBlobStoreBackend(store)}
	db, err := openDB(ctx, store, dbOpenOptions{manifestStorage: faults})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	opts := DefaultWriterOptions()
	opts.OwnerID = "operational-contention-writer"
	opts.Flush.Interval = 0
	opts.SST.Compression = "none"
	writer, err := db.OpenWriter(ctx, opts)
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}
	faults.injectConflictsPerCommit(2)

	expected := make(map[string][]byte, 64)
	for i := 0; i < 64; i++ {
		key := fmt.Sprintf("key:%03d", i)
		value := []byte(fmt.Sprintf("value:%03d", i))
		if err := writer.Put(ctx, []byte(key), value); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
		if err := writer.Flush(ctx); err != nil {
			t.Fatalf("flush %s: %v", key, err)
		}
		expected[key] = value
	}
	maintenanceOpts := DefaultMaintenanceOptions()
	maintenanceOpts.OwnerID = "operational-contention-maintenance"
	maintenanceOpts.Compaction.L0SSTCount = 4
	maintenanceOpts.Compaction.BaseLevelBytes = 1 << 60
	maintenanceOpts.Compaction.TargetSSTBytes = 1 << 20
	maintenanceOpts.Compaction.Compression = "none"
	maintenanceOpts.GarbageCollection.GracePeriod = time.Nanosecond
	maintenanceOpts.GarbageCollection.DeleteBatchSize = manifest.MaxRetiredObjectsPerEntry
	maintenance, err := db.OpenMaintenance(ctx, maintenanceOpts)
	if err != nil {
		t.Fatalf("open maintenance: %v", err)
	}
	stats := driveMaintenanceToIdle(t, ctx, maintenance, writer)
	if stats.CompactionJobs == 0 {
		t.Fatalf("maintenance performed no compaction: %+v", stats)
	}
	if err := maintenance.Close(ctx); err != nil {
		t.Fatalf("close maintenance: %v", err)
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	if got := faults.conflictCount(); got < 2*64 {
		t.Fatalf("injected CAS conflicts=%d, want at least %d", got, 2*64)
	}

	reader := openReaderFromDBForTest(t, ctx, store, DefaultReaderOpenOptions(t.TempDir()))
	defer reader.Close()
	assertReaderHasAll(t, ctx, reader, expected)
	assertOperationalStorageHealthy(t, ctx, store)
}

func TestOperationalRecovery_StaleWriterIsFenced(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store := blobstore.NewMemory("operational-stale-writer")
	defer store.Close()
	db1, err := openDB(ctx, store, dbOpenOptions{})
	if err != nil {
		t.Fatalf("open first db: %v", err)
	}
	defer db1.Close()
	db2, err := openDB(ctx, store, dbOpenOptions{})
	if err != nil {
		t.Fatalf("open second db: %v", err)
	}
	defer db2.Close()

	opts1 := DefaultWriterOptions()
	opts1.OwnerID = "operational-writer-1"
	opts1.Flush.Interval = 0
	opts1.SST.Compression = "none"
	writer1, err := db1.OpenWriter(ctx, opts1)
	if err != nil {
		t.Fatalf("open first writer: %v", err)
	}
	if err := writer1.Put(ctx, []byte("before-fence"), []byte("visible")); err != nil {
		t.Fatalf("put before fence: %v", err)
	}
	if err := writer1.Flush(ctx); err != nil {
		t.Fatalf("flush before fence: %v", err)
	}

	opts2 := DefaultWriterOptions()
	opts2.OwnerID = "operational-writer-2"
	opts2.Flush.Interval = 0
	opts2.SST.Compression = "none"
	writer2, err := db2.OpenWriter(ctx, opts2)
	if err != nil {
		t.Fatalf("open second writer: %v", err)
	}
	if err := writer1.Put(ctx, []byte("stale"), []byte("must-not-appear")); err != nil {
		t.Fatalf("buffer stale write: %v", err)
	}
	if err := writer1.Flush(ctx); !errors.Is(err, manifest.ErrFenced) {
		t.Fatalf("stale writer flush error=%v, want %v", err, manifest.ErrFenced)
	}
	if err := writer1.Close(ctx); !errors.Is(err, manifest.ErrFenced) {
		t.Fatalf("stale writer close error=%v, want %v", err, manifest.ErrFenced)
	}

	if err := writer2.Put(ctx, []byte("after-fence"), []byte("visible")); err != nil {
		t.Fatalf("put after fence: %v", err)
	}
	if err := writer2.Close(ctx); err != nil {
		t.Fatalf("close second writer: %v", err)
	}

	reader := openReaderFromDBForTest(t, ctx, store, DefaultReaderOpenOptions(t.TempDir()))
	defer reader.Close()
	assertReaderValue(t, ctx, reader, "before-fence", "visible", true)
	assertReaderValue(t, ctx, reader, "after-fence", "visible", true)
	assertReaderValue(t, ctx, reader, "stale", "", false)
}

func TestOperationalSignals_BackpressureCounter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store := blobstore.NewMemory("operational-backpressure-signal")
	defer store.Close()
	db, err := openDB(ctx, store, dbOpenOptions{})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	metrics := DefaultWriterMetrics(nil)
	opts := DefaultWriterOptions()
	opts.OwnerID = "operational-backpressure-writer"
	opts.Memtable.TargetBytes = 512
	opts.Memtable.MaxPendingMemtables = 1
	opts.Flush.Interval = 0
	opts.SST.Compression = "none"
	opts.Metrics = metrics
	writer, err := db.OpenWriter(ctx, opts)
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}

	value := make([]byte, 128)
	var backpressure error
	for i := 0; i < 10_000; i++ {
		backpressure = writer.Put(ctx, []byte(fmt.Sprintf("key:%06d", i)), value)
		if errors.Is(backpressure, ErrBackpressure) {
			break
		}
		if backpressure != nil {
			t.Fatalf("put %d: %v", i, backpressure)
		}
	}
	if !errors.Is(backpressure, ErrBackpressure) {
		t.Fatalf("put error=%v, want %v", backpressure, ErrBackpressure)
	}
	if got := testutil.ToFloat64(metrics.BackPressureTotal); got != 1 {
		t.Fatalf("backpressure metric=%v, want=1", got)
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatalf("flush after backpressure: %v", err)
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatalf("close writer: %v", err)
	}
}

func TestOperationalRecovery_Soak(t *testing.T) {
	rawDuration := os.Getenv("ISLEDB_OPERATIONAL_SOAK")
	if rawDuration == "" {
		t.Skip("set ISLEDB_OPERATIONAL_SOAK, for example 2m, to run the operational soak")
	}
	duration, err := time.ParseDuration(rawDuration)
	if err != nil || duration <= 0 {
		t.Fatalf("invalid ISLEDB_OPERATIONAL_SOAK=%q", rawDuration)
	}

	ctx, cancel := context.WithTimeout(context.Background(), duration+30*time.Second)
	defer cancel()
	store := setupFakeS3StoreWithPrefix(t, fmt.Sprintf("operational-soak-%d", time.Now().UnixNano()))
	defer store.Close()
	faults := &operationalCASStorage{BlobStoreBackend: manifest.NewBlobStoreBackend(store)}
	faults.injectConflictsPerCommit(1)

	deadline := time.Now().Add(duration)
	expected := make(map[string][]byte)
	cacheDir := t.TempDir()
	cycles := 0
	for time.Now().Before(deadline) {
		db, err := openDB(ctx, store, dbOpenOptions{manifestStorage: faults})
		if err != nil {
			t.Fatalf("cycle %d open db: %v", cycles, err)
		}

		writerOpts := DefaultWriterOptions()
		writerOpts.OwnerID = fmt.Sprintf("soak-writer-%06d", cycles)
		writerOpts.Flush.Interval = 0
		writerOpts.SST.Compression = "none"
		writer, err := db.OpenWriter(ctx, writerOpts)
		if err != nil {
			t.Fatalf("cycle %d open writer: %v", cycles, err)
		}
		for i := 0; i < 16; i++ {
			key := fmt.Sprintf("cycle:%06d:key:%02d", cycles, i)
			value := []byte(fmt.Sprintf("value:%06d:%02d", cycles, i))
			if err := writer.Put(ctx, []byte(key), value); err != nil {
				t.Fatalf("cycle %d put %s: %v", cycles, key, err)
			}
			expected[key] = value
		}
		maintenanceOpts := DefaultMaintenanceOptions()
		maintenanceOpts.OwnerID = fmt.Sprintf("soak-maintenance-%06d", cycles)
		maintenanceOpts.Compaction.L0SSTCount = 8
		maintenanceOpts.Compaction.BaseLevelBytes = 1 << 60
		maintenanceOpts.Compaction.TargetSSTBytes = 1 << 20
		maintenanceOpts.Compaction.Compression = "none"
		maintenanceOpts.GarbageCollection.GracePeriod = time.Nanosecond
		maintenanceOpts.GarbageCollection.DeleteBatchSize = manifest.MaxRetiredObjectsPerEntry
		maintenance, err := db.OpenMaintenance(ctx, maintenanceOpts)
		if err != nil {
			t.Fatalf("cycle %d open maintenance: %v", cycles, err)
		}
		driveMaintenanceToIdle(t, ctx, maintenance, writer)
		if err := maintenance.Close(ctx); err != nil {
			t.Fatalf("cycle %d close maintenance: %v", cycles, err)
		}
		if err := writer.Close(ctx); err != nil {
			t.Fatalf("cycle %d close writer: %v", cycles, err)
		}

		reader, err := db.OpenReader(ctx, DefaultReaderOpenOptions(cacheDir))
		if err != nil {
			t.Fatalf("cycle %d open reader: %v", cycles, err)
		}
		assertReaderValue(t, ctx, reader,
			fmt.Sprintf("cycle:%06d:key:%02d", cycles, 0),
			fmt.Sprintf("value:%06d:%02d", cycles, 0), true)
		if err := reader.Close(); err != nil {
			t.Fatalf("cycle %d close reader: %v", cycles, err)
		}
		if err := db.Close(); err != nil {
			t.Fatalf("cycle %d close db: %v", cycles, err)
		}
		cycles++
	}

	finalDB, err := openDB(ctx, store, dbOpenOptions{})
	if err != nil {
		t.Fatalf("open final db: %v", err)
	}
	defer finalDB.Close()
	reader, err := finalDB.OpenReader(ctx, DefaultReaderOpenOptions(cacheDir))
	if err != nil {
		t.Fatalf("open final reader: %v", err)
	}
	assertReaderHasAll(t, ctx, reader, expected)
	if err := reader.Close(); err != nil {
		t.Fatalf("close final reader: %v", err)
	}
	assertOperationalStorageHealthy(t, ctx, store)
	t.Logf("operational soak completed cycles=%d records=%d CAS_conflicts=%d",
		cycles, len(expected), faults.conflictCount())
}

func assertOperationalStorageHealthy(t testing.TB, ctx context.Context, store *blobstore.Store) {
	t.Helper()

	current := readCurrentForTest(t, ctx, store)
	if current.RetirementLogStart != current.NextSeq {
		t.Fatalf("retirement backlog: retained_from=%d head=%d",
			current.RetirementLogStart, current.NextSeq)
	}

	live := replayManifestForTest(t, ctx, store)
	physical, err := store.ListSSTFiles(ctx)
	if err != nil {
		t.Fatalf("list physical SSTs: %v", err)
	}
	liveIDs := live.AllSSTIDs()
	if got, want := len(physical), len(liveIDs); got != want {
		t.Fatalf("physical SST amplification: physical=%d live=%d", got, want)
	}
	liveKeys := make(map[string]struct{}, len(liveIDs))
	for _, id := range liveIDs {
		liveKeys[store.SSTPath(id)] = struct{}{}
	}
	var physicalBytes int64
	for _, object := range physical {
		if _, ok := liveKeys[object.Key]; !ok {
			t.Fatalf("unreachable physical SST remains: %s", object.Key)
		}
		physicalBytes += object.Size
	}
	var livePhysicalBytes int64
	for _, sst := range live.L0SSTs {
		livePhysicalBytes += physicalSSTBytes(sst)
	}
	for _, level := range live.Levels {
		for _, sst := range level.SSTs {
			livePhysicalBytes += physicalSSTBytes(sst)
		}
	}
	if physicalBytes != livePhysicalBytes {
		t.Fatalf("physical byte amplification: physical=%d live=%d", physicalBytes, livePhysicalBytes)
	}
}

func physicalSSTBytes(sst sstMetadata) int64 {
	size := sst.Size + sst.Bloom.Length
	if sst.Bloom.Length > 0 {
		size += bloomTrailerLen
	}
	return size
}
