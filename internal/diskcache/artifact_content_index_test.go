package diskcache

import "testing"

func mustArtifactContentAddress(
	t testing.TB,
	kind ArtifactKind,
	checksum string,
) artifactContentAddress {
	t.Helper()
	address, err := artifactContentAddressFor(ArtifactDescriptor{
		Key:      ArtifactKey{Kind: kind, SSTID: "unused-by-content-address"},
		Checksum: checksum,
	})
	if err != nil {
		t.Fatalf("content address: %v", err)
	}
	return address
}

func TestArtifactContentIndexAccountsUniqueContentOnce(t *testing.T) {
	index, err := newArtifactContentIndex(ArtifactSST)
	if err != nil {
		t.Fatalf("new index: %v", err)
	}
	address := mustArtifactContentAddress(t, ArtifactSST,
		"sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")

	first, inserted, err := index.insertPinned(address, 128)
	if err != nil || !inserted {
		t.Fatalf("first insert: inserted=%t err=%v", inserted, err)
	}
	second, inserted, err := index.insertPinned(address, 128)
	if err != nil || inserted {
		t.Fatalf("duplicate insert: inserted=%t err=%v", inserted, err)
	}
	if first != second {
		t.Fatal("duplicate content did not return the resident entry")
	}
	if got := len(index.entries); got != 1 {
		t.Fatalf("entry count=%d, want 1", got)
	}
	if got := index.residentBytes; got != 128 {
		t.Fatalf("resident bytes=%d, want 128", got)
	}
	if got := first.refs; got != 2 {
		t.Fatalf("references=%d, want 2", got)
	}
}

func TestArtifactContentIndexMaintainsLRUOrder(t *testing.T) {
	index, err := newArtifactContentIndex(ArtifactSST)
	if err != nil {
		t.Fatalf("new index: %v", err)
	}
	firstAddress := mustArtifactContentAddress(t, ArtifactSST,
		"sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	secondAddress := mustArtifactContentAddress(t, ArtifactSST,
		"sha256:1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	first, _, err := index.insertPinned(firstAddress, 64)
	if err != nil {
		t.Fatalf("insert first: %v", err)
	}
	second, _, err := index.insertPinned(secondAddress, 96)
	if err != nil {
		t.Fatalf("insert second: %v", err)
	}

	if got := index.oldest(); got != first {
		t.Fatalf("oldest=%p, want first=%p", got, first)
	}
	if _, ok := index.probe(firstAddress); !ok {
		t.Fatal("probe did not find first entry")
	}
	if got := index.oldest(); got != first {
		t.Fatal("probe changed LRU order")
	}
	if _, ok, err := index.pin(firstAddress, 64); err != nil || !ok {
		t.Fatal("pin did not find first entry")
	}
	if got := index.oldest(); got != second {
		t.Fatalf("oldest after touch=%p, want second=%p", got, second)
	}
}

func TestArtifactContentIndexRemoveRequiresCurrentOwner(t *testing.T) {
	index, err := newArtifactContentIndex(ArtifactBloom)
	if err != nil {
		t.Fatalf("new index: %v", err)
	}
	address := mustArtifactContentAddress(t, ArtifactBloom,
		"sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789")
	entry, _, err := index.insertPinned(address, 32)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	stale := &artifactContentIndexEntry{address: address, size: entry.size}

	if _, detached := index.detach(stale); detached {
		t.Fatal("stale pointer removed the current entry")
	}
	deleteNow, detached := index.detach(entry)
	if !detached {
		t.Fatal("current entry was not removed")
	}
	if deleteNow {
		t.Fatal("pinned entry was marked for immediate deletion")
	}
	if got := len(index.entries); got != 0 {
		t.Fatalf("entry count=%d, want 0", got)
	}
	if got := index.residentBytes; got != 0 {
		t.Fatalf("resident bytes=%d, want 0", got)
	}
	if index.oldest() != nil {
		t.Fatal("removed entry remained in LRU")
	}
	deletionOwed, err := index.release(entry)
	if err != nil {
		t.Fatalf("release: %v", err)
	}
	if !deletionOwed {
		t.Fatal("last release did not request deferred deletion")
	}
}

func TestArtifactContentIndexRejectsBrokenInvariants(t *testing.T) {
	index, err := newArtifactContentIndex(ArtifactSST)
	if err != nil {
		t.Fatalf("new index: %v", err)
	}
	sst := mustArtifactContentAddress(t, ArtifactSST,
		"sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	bloom := mustArtifactContentAddress(t, ArtifactBloom,
		"sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")

	if _, _, err := index.insertPinned(bloom, 1); err == nil {
		t.Fatal("Bloom address was accepted by SST index")
	}
	if _, _, err := index.insertPinned(sst, 0); err == nil {
		t.Fatal("zero-size artifact was accepted")
	}
	if _, _, err := index.insertPinned(sst, 10); err != nil {
		t.Fatalf("insert SST: %v", err)
	}
	if _, _, err := index.insertPinned(sst, 11); err == nil {
		t.Fatal("one checksum was accepted with two sizes")
	}
}

func TestArtifactContentIndexDetachedGenerationCannotHideReplacement(t *testing.T) {
	index, err := newArtifactContentIndex(ArtifactSST)
	if err != nil {
		t.Fatalf("new index: %v", err)
	}
	address := mustArtifactContentAddress(t, ArtifactSST,
		"sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	oldEntry, _, err := index.insertPinned(address, 128)
	if err != nil {
		t.Fatalf("insert old generation: %v", err)
	}
	if deleteNow, detached := index.detach(oldEntry); !detached || deleteNow {
		t.Fatalf("detach old generation: detached=%t deleteNow=%t", detached, deleteNow)
	}

	newEntry, inserted, err := index.insertPinned(address, 128)
	if err != nil || !inserted {
		t.Fatalf("insert replacement: inserted=%t err=%v", inserted, err)
	}
	if newEntry == oldEntry {
		t.Fatal("replacement reused detached entry generation")
	}
	deletionOwed, err := index.release(oldEntry)
	if err != nil || !deletionOwed {
		t.Fatalf("release old generation: deletionOwed=%t err=%v", deletionOwed, err)
	}
	if resident, ok := index.probe(address); !ok || resident != newEntry {
		t.Fatal("old generation release hid the replacement")
	}
}
