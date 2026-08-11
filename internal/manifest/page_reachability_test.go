package manifest

import (
	"context"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
)

func TestIsPageReachableTraversesOnlyContainingBranch(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("manifest-page-reachability")
	defer store.Close()
	manifestStore := NewStore(store)
	now := time.Now().UTC()

	leaves := make([]PageRef, 4)
	for i := range leaves {
		seq := uint64(i)
		ref, err := manifestStore.writeCommitPage(ctx, &CommitPage{
			LayoutVersion: LayoutVersion,
			PageType:      CommitPageTypeLeaf,
			Level:         0,
			SeqLo:         seq,
			SeqHi:         seq,
			Count:         1,
			Entries:       []ManifestLogEntry{{Seq: seq}},
			CreatedAt:     now,
		})
		if err != nil {
			t.Fatalf("write leaf %d: %v", i, err)
		}
		leaves[i] = ref
	}
	left, err := manifestStore.writeIndexPage(ctx, leaves[:2])
	if err != nil {
		t.Fatalf("write left index: %v", err)
	}
	right, err := manifestStore.writeIndexPage(ctx, leaves[2:])
	if err != nil {
		t.Fatalf("write right index: %v", err)
	}
	root, err := manifestStore.writeIndexPage(ctx, []PageRef{left, right})
	if err != nil {
		t.Fatalf("write root: %v", err)
	}
	current := &Current{IndexFrontier: []PageRef{root}}

	reachable, reads, err := manifestStore.IsPageReachable(ctx, current, leaves[3])
	if err != nil {
		t.Fatalf("IsPageReachable: %v", err)
	}
	if !reachable || reads != 2 {
		t.Fatalf("reachable=%v reads=%d want=true,2", reachable, reads)
	}
	reachable, reads, err = manifestStore.IsPageReachable(ctx, current, root)
	if err != nil || !reachable || reads != 0 {
		t.Fatalf("root reachable=%v reads=%d error=%v want=true,0,nil", reachable, reads, err)
	}
}

func TestIsPageReachableFailsClosedForAmbiguousOrMismatchedReferences(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("manifest-page-reachability-fail-closed")
	defer store.Close()
	manifestStore := NewStore(store)
	now := time.Now().UTC()
	ref, err := manifestStore.writeCommitPage(ctx, &CommitPage{
		LayoutVersion: LayoutVersion,
		PageType:      CommitPageTypeLeaf,
		Level:         0,
		SeqLo:         10,
		SeqHi:         10,
		Count:         1,
		Entries:       []ManifestLogEntry{{Seq: 10}},
		CreatedAt:     now,
	})
	if err != nil {
		t.Fatalf("write page: %v", err)
	}

	sameRangeOrphan := ref
	sameRangeOrphan.Path = store.ManifestPagePath(0, "different")
	reachable, reads, err := manifestStore.IsPageReachable(ctx, &Current{IndexFrontier: []PageRef{ref}}, sameRangeOrphan)
	if err != nil || reachable || reads != 0 {
		t.Fatalf("same-range orphan reachable=%v reads=%d error=%v", reachable, reads, err)
	}

	mismatched := ref
	mismatched.Checksum = "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	if _, _, err := manifestStore.IsPageReachable(ctx, &Current{IndexFrontier: []PageRef{ref}}, mismatched); err == nil {
		t.Fatal("same path with mismatched identity was accepted")
	}

	partial := ref
	partial.Path = store.ManifestPagePath(0, "partial")
	partial.SeqHi = 11
	partial.Count = 2
	if _, _, err := manifestStore.IsPageReachable(ctx, &Current{IndexFrontier: []PageRef{ref}}, partial); err == nil {
		t.Fatal("partially overlapping candidate did not fail closed")
	}
}
