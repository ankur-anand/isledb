package diskcache

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestArtifactContentTierCoalescesSameContentPublication(t *testing.T) {
	tier, err := newArtifactContentTier(ArtifactSST, 1<<20)
	if err != nil {
		t.Fatalf("new tier: %v", err)
	}
	address := mustArtifactContentAddress(t, ArtifactSST,
		"sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")

	firstPublishing := make(chan struct{})
	allowFirstPublish := make(chan struct{})
	firstDone := make(chan struct{})
	var publications atomic.Int64
	go func() {
		defer close(firstDone)
		_, _, publishErr := tier.publishPinned(address, 128, func() error {
			publications.Add(1)
			close(firstPublishing)
			<-allowFirstPublish
			return nil
		}, func(artifactContentAddress) error { return nil })
		if publishErr != nil {
			t.Errorf("first publication: %v", publishErr)
		}
	}()
	<-firstPublishing

	secondDone := make(chan struct{})
	var secondEntry *artifactContentIndexEntry
	var secondAdmission artifactContentAdmission
	go func() {
		defer close(secondDone)
		secondEntry, secondAdmission, err = tier.publishPinned(address, 128, func() error {
			publications.Add(1)
			return nil
		}, func(artifactContentAddress) error { return nil })
	}()

	select {
	case <-secondDone:
		t.Fatal("second publication passed the active publication")
	case <-time.After(20 * time.Millisecond):
	}
	close(allowFirstPublish)
	<-firstDone
	<-secondDone
	if err != nil {
		t.Fatalf("second publication: %v", err)
	}
	if secondAdmission != artifactContentAlreadyResident {
		t.Fatalf("second admission=%d, want already resident", secondAdmission)
	}
	if got := publications.Load(); got != 1 {
		t.Fatalf("publication callbacks=%d, want 1", got)
	}
	if secondEntry == nil {
		t.Fatal("second publication returned a nil shared entry")
	}
	if secondEntry.refs != 2 {
		t.Fatalf("shared entry references=%d, want 2", secondEntry.refs)
	}
}

func TestArtifactContentTierPublicationFailureLeavesNoEntry(t *testing.T) {
	tier, err := newArtifactContentTier(ArtifactSST, 1<<20)
	if err != nil {
		t.Fatalf("new tier: %v", err)
	}
	address := mustArtifactContentAddress(t, ArtifactSST,
		"sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	wantErr := errors.New("publish failed")

	if _, _, err := tier.publishPinned(address, 128, func() error {
		return wantErr
	}, func(artifactContentAddress) error { return nil }); !errors.Is(err, wantErr) {
		t.Fatalf("publication error=%v, want %v", err, wantErr)
	}
	if _, ok := tier.probe(address); ok {
		t.Fatal("failed publication left a searchable entry")
	}
}

func TestArtifactContentTierReleaseDoesNotDeleteReplacement(t *testing.T) {
	tier, err := newArtifactContentTier(ArtifactSST, 1<<20)
	if err != nil {
		t.Fatalf("new tier: %v", err)
	}
	address := mustArtifactContentAddress(t, ArtifactSST,
		"sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	oldEntry, admission, err := tier.publishPinned(
		address, 128, func() error { return nil }, func(artifactContentAddress) error { return nil })
	if err != nil || admission != artifactContentAdmitted {
		t.Fatalf("publish old entry: admission=%d err=%v", admission, err)
	}
	if detached, err := tier.detach(oldEntry, func(artifactContentAddress) error {
		t.Fatal("pinned old entry was removed immediately")
		return nil
	}); err != nil || !detached {
		t.Fatalf("detach old entry: detached=%t err=%v", detached, err)
	}

	newEntry, admission, err := tier.publishPinned(
		address, 128, func() error { return nil }, func(artifactContentAddress) error { return nil })
	if err != nil || admission != artifactContentAdmitted {
		t.Fatalf("publish replacement: admission=%d err=%v", admission, err)
	}
	var removals atomic.Int64
	if err := tier.release(oldEntry, func(artifactContentAddress) error {
		removals.Add(1)
		return nil
	}); err != nil {
		t.Fatalf("release old entry: %v", err)
	}
	if got := removals.Load(); got != 0 {
		t.Fatalf("old entry removed replacement path %d times", got)
	}
	if resident, ok := tier.probe(address); !ok || resident != newEntry {
		t.Fatal("replacement is not searchable after old entry release")
	}
}

func TestArtifactContentTierReleaseDeletesDetachedFinalGeneration(t *testing.T) {
	tier, err := newArtifactContentTier(ArtifactBloom, 1<<20)
	if err != nil {
		t.Fatalf("new tier: %v", err)
	}
	address := mustArtifactContentAddress(t, ArtifactBloom,
		"sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789")
	entry, _, err := tier.publishPinned(
		address, 32, func() error { return nil }, func(artifactContentAddress) error { return nil })
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if detached, err := tier.detach(entry, func(artifactContentAddress) error {
		t.Fatal("pinned entry was removed immediately")
		return nil
	}); err != nil || !detached {
		t.Fatalf("detach: detached=%t err=%v", detached, err)
	}

	var removals atomic.Int64
	if err := tier.release(entry, func(got artifactContentAddress) error {
		if got != address {
			t.Fatalf("removed address=%v, want %v", got, address)
		}
		removals.Add(1)
		return nil
	}); err != nil {
		t.Fatalf("release: %v", err)
	}
	if got := removals.Load(); got != 1 {
		t.Fatalf("removals=%d, want 1", got)
	}
}

func TestArtifactContentTierOversizedAdmissionBypassesWithoutPublishing(t *testing.T) {
	tier, err := newArtifactContentTier(ArtifactSST, 4)
	if err != nil {
		t.Fatalf("new tier: %v", err)
	}
	address := mustArtifactContentAddress(t, ArtifactSST,
		"sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	var callbacks atomic.Int64
	entry, admission, err := tier.publishPinned(address, 5, func() error {
		callbacks.Add(1)
		return nil
	}, func(artifactContentAddress) error {
		callbacks.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("publish oversized content: %v", err)
	}
	if entry != nil || admission != artifactContentBypassedOversized {
		t.Fatalf("entry=%p admission=%d, want nil oversized bypass", entry, admission)
	}
	if got := callbacks.Load(); got != 0 {
		t.Fatalf("callbacks=%d, want 0", got)
	}
}

func TestArtifactContentTierPinnedCapacityBypassPreservesAllEntries(t *testing.T) {
	tier, err := newArtifactContentTier(ArtifactSST, 10)
	if err != nil {
		t.Fatalf("new tier: %v", err)
	}
	firstAddress := mustArtifactContentAddress(t, ArtifactSST,
		"sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	pinnedAddress := mustArtifactContentAddress(t, ArtifactSST,
		"sha256:1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	incomingAddress := mustArtifactContentAddress(t, ArtifactSST,
		"sha256:2123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	noRemove := func(artifactContentAddress) error { return nil }

	first, _, err := tier.publishPinned(firstAddress, 4, func() error { return nil }, noRemove)
	if err != nil {
		t.Fatalf("publish first: %v", err)
	}
	if err := tier.release(first, noRemove); err != nil {
		t.Fatalf("release first: %v", err)
	}
	pinned, _, err := tier.publishPinned(pinnedAddress, 6, func() error { return nil }, noRemove)
	if err != nil {
		t.Fatalf("publish pinned: %v", err)
	}

	var callbacks atomic.Int64
	entry, admission, err := tier.publishPinned(incomingAddress, 8, func() error {
		callbacks.Add(1)
		return nil
	}, func(artifactContentAddress) error {
		callbacks.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("publish under pinned pressure: %v", err)
	}
	if entry != nil || admission != artifactContentBypassedPinnedCapacity {
		t.Fatalf("entry=%p admission=%d, want nil pinned-capacity bypass", entry, admission)
	}
	if got := callbacks.Load(); got != 0 {
		t.Fatalf("callbacks=%d, want 0", got)
	}
	if _, ok := tier.probe(firstAddress); !ok {
		t.Fatal("failed admission detached a partially useful victim set")
	}
	if resident, ok := tier.probe(pinnedAddress); !ok || resident != pinned {
		t.Fatal("failed admission removed the pinned entry")
	}
}

func TestArtifactContentTierCapacityEvictsLeastRecentlyUsedUnpinnedEntry(t *testing.T) {
	tier, err := newArtifactContentTier(ArtifactSST, 8)
	if err != nil {
		t.Fatalf("new tier: %v", err)
	}
	firstAddress := mustArtifactContentAddress(t, ArtifactSST,
		"sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	secondAddress := mustArtifactContentAddress(t, ArtifactSST,
		"sha256:1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	incomingAddress := mustArtifactContentAddress(t, ArtifactSST,
		"sha256:2123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	noRemove := func(artifactContentAddress) error { return nil }

	first, _, err := tier.publishPinned(firstAddress, 4, func() error { return nil }, noRemove)
	if err != nil {
		t.Fatalf("publish first: %v", err)
	}
	second, _, err := tier.publishPinned(secondAddress, 4, func() error { return nil }, noRemove)
	if err != nil {
		t.Fatalf("publish second: %v", err)
	}
	if err := tier.release(first, noRemove); err != nil {
		t.Fatalf("release first: %v", err)
	}
	if err := tier.release(second, noRemove); err != nil {
		t.Fatalf("release second: %v", err)
	}
	first, ok, err := tier.pin(firstAddress, 4)
	if err != nil || !ok {
		t.Fatalf("touch first: ok=%t err=%v", ok, err)
	}
	if err := tier.release(first, noRemove); err != nil {
		t.Fatalf("release touched first: %v", err)
	}

	var removed artifactContentAddress
	_, admission, err := tier.publishPinned(incomingAddress, 4, func() error { return nil },
		func(address artifactContentAddress) error {
			removed = address
			return nil
		})
	if err != nil || admission != artifactContentAdmitted {
		t.Fatalf("publish incoming: admission=%d err=%v", admission, err)
	}
	if removed != secondAddress {
		t.Fatalf("removed address=%v, want LRU second=%v", removed, secondAddress)
	}
	if _, ok := tier.probe(firstAddress); !ok {
		t.Fatal("recently used first entry was evicted")
	}
	if _, ok := tier.probe(secondAddress); ok {
		t.Fatal("least recently used second entry remains searchable")
	}

	tier.indexMu.Lock()
	residentBytes := tier.index.residentBytes
	tier.indexMu.Unlock()
	if residentBytes != 8 {
		t.Fatalf("resident bytes=%d, want 8", residentBytes)
	}
}

func TestArtifactContentTierCapacityCleanupRunsAllVictimsBeforeBypass(t *testing.T) {
	tier, err := newArtifactContentTier(ArtifactSST, 8)
	if err != nil {
		t.Fatalf("new tier: %v", err)
	}
	firstAddress := mustArtifactContentAddress(t, ArtifactSST,
		"sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	secondAddress := mustArtifactContentAddress(t, ArtifactSST,
		"sha256:1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	incomingAddress := mustArtifactContentAddress(t, ArtifactSST,
		"sha256:2123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	noRemove := func(artifactContentAddress) error { return nil }

	first, _, err := tier.publishPinned(firstAddress, 4, func() error { return nil }, noRemove)
	if err != nil {
		t.Fatalf("publish first: %v", err)
	}
	second, _, err := tier.publishPinned(secondAddress, 4, func() error { return nil }, noRemove)
	if err != nil {
		t.Fatalf("publish second: %v", err)
	}
	if err := tier.release(first, noRemove); err != nil {
		t.Fatalf("release first: %v", err)
	}
	if err := tier.release(second, noRemove); err != nil {
		t.Fatalf("release second: %v", err)
	}

	wantErr := errors.New("remove failed")
	var removals atomic.Int64
	var publications atomic.Int64
	_, admission, err := tier.publishPinned(incomingAddress, 8, func() error {
		publications.Add(1)
		return nil
	}, func(address artifactContentAddress) error {
		removals.Add(1)
		if address == firstAddress {
			return wantErr
		}
		return nil
	})
	if admission != artifactContentBypassedPublicationFailure || !errors.Is(err, wantErr) {
		t.Fatalf("admission=%d error=%v, want publication-failure bypass wrapping %v",
			admission, err, wantErr)
	}
	if got := removals.Load(); got != 2 {
		t.Fatalf("removal callbacks=%d, want 2", got)
	}
	if got := publications.Load(); got != 0 {
		t.Fatalf("publication callbacks=%d, want 0", got)
	}
	if _, ok := tier.probe(firstAddress); ok {
		t.Fatal("first detached victim remains searchable")
	}
	if _, ok := tier.probe(secondAddress); ok {
		t.Fatal("second detached victim remains searchable")
	}
}
