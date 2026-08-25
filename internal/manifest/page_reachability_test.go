package manifest

import (
	"context"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
)

func TestManifestPageObjectKeysOrderBySeqHi(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("manifest-page-key-order")
	defer store.Close()
	manifestStore := NewStore(store)
	now := time.Now().UTC()
	entries := func(lo, hi uint64) []ManifestLogEntry {
		result := make([]ManifestLogEntry, 0, hi-lo+1)
		for seq := lo; seq <= hi; seq++ {
			result = append(result, ManifestLogEntry{Seq: seq})
		}
		return result
	}

	wideEntries := entries(0, 150)
	wide, err := manifestStore.writeCommitPage(ctx, &CommitPage{
		LayoutVersion: LayoutVersion,
		PageType:      CommitPageTypeLeaf,
		Level:         0,
		SeqLo:         0,
		SeqHi:         150,
		Count:         uint32(len(wideEntries)),
		Entries:       wideEntries,
		CreatedAt:     now,
	})
	if err != nil {
		t.Fatalf("write wide page: %v", err)
	}
	narrowEntries := entries(50, 80)
	narrow, err := manifestStore.writeCommitPage(ctx, &CommitPage{
		LayoutVersion: LayoutVersion,
		PageType:      CommitPageTypeLeaf,
		Level:         0,
		SeqLo:         50,
		SeqHi:         80,
		Count:         uint32(len(narrowEntries)),
		Entries:       narrowEntries,
		CreatedAt:     now,
	})
	if err != nil {
		t.Fatalf("write narrow page: %v", err)
	}

	wideName := strings.TrimSuffix(path.Base(wide.Path), ".page.zst")
	narrowName := strings.TrimSuffix(path.Base(narrow.Path), ".page.zst")
	if !strings.HasPrefix(wideName, "h00000000000000000150-l00000000000000000000-") {
		t.Fatalf("wide page name=%q", wideName)
	}
	if !strings.HasPrefix(narrowName, "h00000000000000000080-l00000000000000000050-") {
		t.Fatalf("narrow page name=%q", narrowName)
	}
	if narrow.Path >= wide.Path {
		t.Fatalf("SeqHi ordering wrong: narrow=%q wide=%q", narrow.Path, wide.Path)
	}
}

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
