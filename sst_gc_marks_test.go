package isledb

import (
	"context"
	"testing"

	"github.com/ankur-anand/isledb/blobstore"
)

func TestStoreGCMarkCheckpoint_MonotonicProgress(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("")
	defer store.Close()

	if err := storeGCMarkCheckpoint(ctx, store, &gcMarkCheckpoint{
		LastAppliedSeq:      20,
		LastSeenLogSeqStart: 10,
	}); err != nil {
		t.Fatalf("store initial checkpoint: %v", err)
	}

	if err := storeGCMarkCheckpoint(ctx, store, &gcMarkCheckpoint{
		LastAppliedSeq:      5,
		LastSeenLogSeqStart: 2,
	}); err != nil {
		t.Fatalf("store regressing checkpoint: %v", err)
	}

	got, err := loadGCMarkCheckpoint(ctx, store)
	if err != nil {
		t.Fatalf("load checkpoint: %v", err)
	}
	if got.LastAppliedSeq < 20 {
		t.Fatalf("last_applied_seq regressed: got=%d want>=20", got.LastAppliedSeq)
	}
	if got.LastSeenLogSeqStart < 10 {
		t.Fatalf("last_seen_log_seq_start regressed: got=%d want>=10", got.LastSeenLogSeqStart)
	}
}
