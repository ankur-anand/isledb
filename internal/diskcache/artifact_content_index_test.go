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

	first, inserted, err := index.insert(address, 128)
	if err != nil || !inserted {
		t.Fatalf("first insert: inserted=%t err=%v", inserted, err)
	}
	second, inserted, err := index.insert(address, 128)
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
	first, _, err := index.insert(firstAddress, 64)
	if err != nil {
		t.Fatalf("insert first: %v", err)
	}
	second, _, err := index.insert(secondAddress, 96)
	if err != nil {
		t.Fatalf("insert second: %v", err)
	}

	if got := index.oldest(); got != first {
		t.Fatalf("oldest=%p, want first=%p", got, first)
	}
	if _, ok := index.find(firstAddress, false); !ok {
		t.Fatal("probe did not find first entry")
	}
	if got := index.oldest(); got != first {
		t.Fatal("probe changed LRU order")
	}
	if _, ok := index.find(firstAddress, true); !ok {
		t.Fatal("touch did not find first entry")
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
	entry, _, err := index.insert(address, 32)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	stale := &artifactContentIndexEntry{address: address, size: entry.size}

	if index.remove(stale) {
		t.Fatal("stale pointer removed the current entry")
	}
	if !index.remove(entry) {
		t.Fatal("current entry was not removed")
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

	if _, _, err := index.insert(bloom, 1); err == nil {
		t.Fatal("Bloom address was accepted by SST index")
	}
	if _, _, err := index.insert(sst, 0); err == nil {
		t.Fatal("zero-size artifact was accepted")
	}
	if _, _, err := index.insert(sst, 10); err != nil {
		t.Fatalf("insert SST: %v", err)
	}
	if _, _, err := index.insert(sst, 11); err == nil {
		t.Fatal("one checksum was accepted with two sizes")
	}
}
