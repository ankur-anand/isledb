package isledb

import "testing"

// A metadata-only compaction relocates files by editing the manifest: nothing
// is read, rewritten, or re-uploaded. Counting those bytes as compaction
// output overstates write amplification and object-store cost, so the stats
// have to keep the two apart.
func TestCompactionStatsSeparateMovedBytesFromRewrittenBytes(t *testing.T) {
	m := &Maintenance{currentStats: &MaintenanceStats{}}

	m.recordCompaction(compactionJob{
		MetadataOnly: true,
		InputSSTs:    []string{"a", "b"},
		OutputSSTs: []compactionOutput{
			{ID: "a", Bytes: 300},
			{ID: "b", Bytes: 700},
		},
	}, nil)
	m.recordCompaction(compactionJob{
		InputSSTs:  []string{"c", "d"},
		OutputSSTs: []compactionOutput{{ID: "e", Bytes: 250}},
	}, nil)

	stats := m.currentStats.SSTCompaction
	if stats.Jobs != 2 {
		t.Fatalf("jobs = %d, want 2", stats.Jobs)
	}
	if stats.MovedJobs != 1 {
		t.Fatalf("moved jobs = %d, want 1", stats.MovedJobs)
	}
	if stats.MovedBytes != 1000 {
		t.Fatalf("moved bytes = %d, want 1000", stats.MovedBytes)
	}
	if stats.RewrittenBytes != 250 {
		t.Fatalf("rewritten bytes = %d, want 250", stats.RewrittenBytes)
	}
	if stats.OutputBytes != 1250 {
		t.Fatalf("output bytes = %d, want 1250", stats.OutputBytes)
	}
	// The three must stay consistent, because callers will derive one from the
	// others.
	if stats.MovedBytes+stats.RewrittenBytes != stats.OutputBytes {
		t.Fatalf("moved %d + rewritten %d != output %d",
			stats.MovedBytes, stats.RewrittenBytes, stats.OutputBytes)
	}
}

func TestCompactionStatsIgnoreFailedJobs(t *testing.T) {
	m := &Maintenance{currentStats: &MaintenanceStats{}}
	m.recordCompaction(compactionJob{
		OutputSSTs: []compactionOutput{{ID: "a", Bytes: 100}},
	}, errCompactorClosed)

	if stats := m.currentStats.SSTCompaction; stats.Jobs != 0 || stats.OutputBytes != 0 {
		t.Fatalf("a failed job was counted: %+v", stats)
	}
}

// The GET side of compaction cost. A rewrite reads every input; an unchecked
// move reads nothing; a verified move reads its sources but still writes no
// new object, so it lands in MovedBytes with a non-zero ReadBytes.
func TestCompactionStatsRecordBytesRead(t *testing.T) {
	m := &Maintenance{currentStats: &MaintenanceStats{}}

	m.recordCompaction(compactionJob{
		ReadBytes:  4000,
		InputSSTs:  []string{"a", "b"},
		OutputSSTs: []compactionOutput{{ID: "c", Bytes: 3500}},
	}, nil)
	m.recordCompaction(compactionJob{
		MetadataOnly: true,
		InputSSTs:    []string{"d"},
		OutputSSTs:   []compactionOutput{{ID: "d", Bytes: 900}},
	}, nil)
	m.recordCompaction(compactionJob{
		MetadataOnly: true,
		ReadBytes:    600,
		InputSSTs:    []string{"e"},
		OutputSSTs:   []compactionOutput{{ID: "e", Bytes: 600}},
	}, nil)

	stats := m.currentStats.SSTCompaction
	if stats.ReadBytes != 4600 {
		t.Fatalf("read bytes = %d, want 4600", stats.ReadBytes)
	}
	if stats.RewrittenBytes != 3500 {
		t.Fatalf("rewritten bytes = %d, want 3500", stats.RewrittenBytes)
	}
	if stats.MovedBytes != 1500 || stats.MovedJobs != 2 {
		t.Fatalf("moved = %d bytes over %d jobs, want 1500 over 2",
			stats.MovedBytes, stats.MovedJobs)
	}
	if stats.MovedBytes+stats.RewrittenBytes != stats.OutputBytes {
		t.Fatalf("moved %d + rewritten %d != output %d",
			stats.MovedBytes, stats.RewrittenBytes, stats.OutputBytes)
	}
}
