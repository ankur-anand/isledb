package isledb

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/ankur-anand/isledb/blobstore"
)

func TestDBSSTOutputRoutesBySSTClass(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("sst-output-routing")
	defer store.Close()

	want := SSTOutputOptions{
		L0: SSTEncodingOptions{
			Compression:     "none",
			BlockBytes:      1024,
			BloomBitsPerKey: 7,
		},
		Compacted: SSTEncodingOptions{
			Compression:     "zstd",
			BlockBytes:      16 << 10,
			BloomBitsPerKey: 12,
		},
	}
	db, err := openDB(ctx, store, dbOpenOptions{sstOutput: want})
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer db.Close()

	writer, err := db.OpenWriter(ctx, DefaultWriterOptions())
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	if got := writer.w.sstOutput; got != want.L0 {
		t.Fatalf("writer SST output=%+v, want %+v", got, want.L0)
	}

	maintenanceOptions := DefaultMaintenanceOptions()
	maintenance, err := db.OpenMaintenance(ctx, maintenanceOptions)
	if err != nil {
		t.Fatalf("OpenMaintenance: %v", err)
	}
	if got := maintenance.sstOutput; got != want.Compacted {
		t.Fatalf("maintenance SST output=%+v, want %+v", got, want.Compacted)
	}
	output := maintenance.compactor.opts.Output
	if output.Compression != want.Compacted.Compression ||
		output.BlockBytes != want.Compacted.BlockBytes ||
		output.BloomBitsPerKey != want.Compacted.BloomBitsPerKey {
		t.Fatalf("compactor SST output=%+v, want %+v", output, want.Compacted)
	}

	if err := maintenance.Close(ctx); err != nil {
		t.Fatalf("Maintenance.Close: %v", err)
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatalf("Writer.Close: %v", err)
	}
}

func TestSSTOutputOptionsRejectInvalidValues(t *testing.T) {
	tests := []struct {
		name string
		opts SSTOutputOptions
	}{
		{
			name: "negative L0 block size",
			opts: SSTOutputOptions{L0: SSTEncodingOptions{BlockBytes: -1}},
		},
		{
			name: "negative compacted bloom bits",
			opts: SSTOutputOptions{Compacted: SSTEncodingOptions{BloomBitsPerKey: -1}},
		},
		{
			name: "unsupported L0 compression",
			opts: SSTOutputOptions{L0: SSTEncodingOptions{Compression: "gzip"}},
		},
		{
			name: "unsupported compacted compression",
			opts: SSTOutputOptions{Compacted: SSTEncodingOptions{Compression: "brotli"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := normalizeSSTOutputOptions(tt.opts); !errors.Is(err, ErrInvalidDBOptions) {
				t.Fatalf("normalizeSSTOutputOptions error=%v, want %v", err, ErrInvalidDBOptions)
			}
		})
	}
}

func TestMixedSSTOutputEncodingsCompactAndRead(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("mixed-sst-output")
	defer store.Close()

	output := SSTOutputOptions{
		L0: SSTEncodingOptions{
			Compression:     "none",
			BlockBytes:      1024,
			BloomBitsPerKey: 7,
		},
		Compacted: SSTEncodingOptions{
			Compression:     "zstd",
			BlockBytes:      16 << 10,
			BloomBitsPerKey: 12,
		},
	}
	db, err := openDB(ctx, store, dbOpenOptions{sstOutput: output})
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer db.Close()

	writer, err := db.OpenWriter(ctx, DefaultWriterOptions())
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	for generation := 0; generation < 2; generation++ {
		for i := 0; i < 256; i++ {
			key := []byte(fmt.Sprintf("key-%04d", i))
			value := []byte(fmt.Sprintf("generation-%d-value-%04d", generation, i))
			if err := writer.Put(ctx, key, value); err != nil {
				t.Fatalf("Put generation=%d key=%d: %v", generation, i, err)
			}
		}
		if err := writer.Flush(ctx); err != nil {
			t.Fatalf("Flush generation=%d: %v", generation, err)
		}
	}

	maintenanceOptions := DefaultMaintenanceOptions()
	maintenanceOptions.SSTCompaction.L0TriggerSSTs = 2
	maintenanceOptions.SSTCompaction.BaseLevelBytes = 1 << 60
	maintenanceOptions.SSTCompaction.TargetSSTBytes = 1 << 20
	maintenance, err := db.OpenMaintenance(ctx, maintenanceOptions)
	if err != nil {
		t.Fatalf("OpenMaintenance: %v", err)
	}
	stats := driveMaintenanceToIdle(t, ctx, maintenance, writer)
	if stats.SSTCompaction.Jobs == 0 {
		t.Fatalf("compaction did not run: %+v", stats)
	}
	if err := maintenance.Close(ctx); err != nil {
		t.Fatalf("Maintenance.Close: %v", err)
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatalf("Writer.Close: %v", err)
	}

	state := replayManifestForTest(t, ctx, store)
	if state.L0SSTCount() != 0 || len(state.Levels) == 0 {
		t.Fatalf("unexpected compacted state: l0=%d levels=%d", state.L0SSTCount(), len(state.Levels))
	}

	reader, err := db.OpenReader(ctx, DefaultReaderOpenOptions(t.TempDir()))
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer reader.Close()
	for i := 0; i < 256; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		want := fmt.Sprintf("generation-1-value-%04d", i)
		value, found, err := reader.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get key=%d: %v", i, err)
		}
		if !found || string(value) != want {
			t.Fatalf("Get key=%d = (%q, %v), want (%q, true)", i, value, found, want)
		}
	}
}
