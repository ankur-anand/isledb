package isledb

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/manifest"
	"github.com/segmentio/ksuid"
)

func runReplayFiltersStaleEntriesAfterNewFenceClaimMultiPhase(t *testing.T, store *blobstore.Store) {
	t.Helper()
	ctx := context.Background()

	manifestStore := newManifestStore(store, nil)

	writerOpts := DefaultWriterOptions()
	writerOpts.FlushInterval = 0
	writerOpts.MemtableSize = 512

	compactorOpts := CompactorOptions{
		L0CompactionThreshold: 1,
		MinSources:            1,
		MaxSources:            4,
		SizeThreshold:         1,
		BloomBitsPerKey:       10,
		BlockSize:             1024,
		Compression:           "snappy",
		TargetSSTSize:         64 * 1024,
		CheckInterval:         time.Hour,
	}

	writer1, err := newWriter(ctx, store, manifestStore, writerOpts)
	if err != nil {
		t.Fatalf("newWriter(1): %v", err)
	}
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key-%03d", i)
		val := fmt.Sprintf("value-%03d", i)
		if err := writer1.put([]byte(key), []byte(val)); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	if err := writer1.flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	_ = writer1.close()

	compactor, err := newCompactor(ctx, store, manifestStore, compactorOpts)
	if err != nil {
		t.Fatalf("newCompactor: %v", err)
	}
	if err := compactor.RunCompaction(ctx); err != nil {
		t.Fatalf("RunCompaction: %v", err)
	}
	_ = compactor.Close()

	writer2, err := newWriter(ctx, store, manifestStore, writerOpts)
	if err != nil {
		t.Fatalf("newWriter(2): %v", err)
	}
	_ = writer2.close()

	compactor2, err := newCompactor(ctx, store, manifestStore, compactorOpts)
	if err != nil {
		t.Fatalf("newCompactor(2): %v", err)
	}
	_ = compactor2.Close()

	backend := manifest.NewBlobStoreBackend(store)
	currentData, currentETag, err := backend.ReadCurrent(ctx)
	if err != nil {
		t.Fatalf("read current: %v", err)
	}
	current, err := manifest.DecodeCurrent(currentData)
	if err != nil {
		t.Fatalf("decode current: %v", err)
	}
	if current == nil {
		t.Fatalf("expected current manifest")
	}

	nextSeq := current.NextSeq
	staleWriterEntry := &manifest.ManifestLogEntry{
		ID:        ksuid.New(),
		Seq:       nextSeq,
		Role:      manifest.FenceRoleWriter,
		Epoch:     1,
		Timestamp: time.Now().UTC(),
		Op:        manifest.LogOpAddSSTable,
		SSTable: &manifest.SSTMeta{
			ID:        "stale.sst",
			Epoch:     1,
			Level:     0,
			CreatedAt: time.Now().UTC(),
		},
	}
	staleCompactorEntry := &manifest.ManifestLogEntry{
		ID:        ksuid.New(),
		Seq:       nextSeq + 1,
		Role:      manifest.FenceRoleCompactor,
		Epoch:     1,
		Timestamp: time.Now().UTC(),
		Op:        manifest.LogOpCompaction,
		Compaction: &manifest.CompactionLogPayload{
			AddSSTables: []manifest.SSTMeta{{
				ID:        "stale-compacted.sst",
				Epoch:     1,
				Level:     1,
				CreatedAt: time.Now().UTC(),
			}},
		},
	}

	entryData, err := manifest.EncodeLogEntry(staleWriterEntry)
	if err != nil {
		t.Fatalf("encode stale writer entry: %v", err)
	}
	logName := fmt.Sprintf("%020d", staleWriterEntry.Seq)
	if _, err := backend.WriteLog(ctx, logName, entryData); err != nil {
		t.Fatalf("write stale writer log entry: %v", err)
	}

	entryData, err = manifest.EncodeLogEntry(staleCompactorEntry)
	if err != nil {
		t.Fatalf("encode stale compactor entry: %v", err)
	}
	logName = fmt.Sprintf("%020d", staleCompactorEntry.Seq)
	if _, err := backend.WriteLog(ctx, logName, entryData); err != nil {
		t.Fatalf("write stale compactor log entry: %v", err)
	}

	if current.LogSeqStart == current.NextSeq {
		current.LogSeqStart = staleWriterEntry.Seq
	}
	current.NextSeq = nextSeq + 2
	if current.NextEpoch <= staleWriterEntry.SSTable.Epoch {
		current.NextEpoch = staleWriterEntry.SSTable.Epoch + 1
	}
	for _, sst := range staleCompactorEntry.Compaction.AddSSTables {
		if current.NextEpoch <= sst.Epoch {
			current.NextEpoch = sst.Epoch + 1
		}
	}
	currentBytes, err := manifest.EncodeCurrent(current)
	if err != nil {
		t.Fatalf("encode current: %v", err)
	}
	if _, err := backend.WriteCurrentCAS(ctx, currentBytes, currentETag); err != nil {
		t.Fatalf("write current: %v", err)
	}

	m, err := manifestStore.Replay(ctx)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if m.LookupSST("stale.sst") != nil {
		t.Fatalf("stale.sst should be filtered after newer fence claim")
	}
	if m.LookupSST("stale-compacted.sst") != nil {
		t.Fatalf("stale-compacted.sst should be filtered after newer fence claim")
	}
	if m.L0SSTCount() == 0 && m.SortedRunCount() == 0 {
		t.Fatalf("expected manifest to contain data from earlier phases")
	}
}

func TestReplay_FiltersStaleEntriesAfterNewFenceClaim_MultiPhase(t *testing.T) {
	store := blobstore.NewMemory("fence-multiphase")
	defer store.Close()
	runReplayFiltersStaleEntriesAfterNewFenceClaimMultiPhase(t, store)
}
