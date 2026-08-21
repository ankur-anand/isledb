package diskcache

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestArtifactContentTierCoalescesSameContentPublication(t *testing.T) {
	tier, err := newArtifactContentTier(ArtifactSST)
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
		})
		if publishErr != nil {
			t.Errorf("first publication: %v", publishErr)
		}
	}()
	<-firstPublishing

	secondDone := make(chan struct{})
	var secondEntry *artifactContentIndexEntry
	var secondInserted bool
	go func() {
		defer close(secondDone)
		secondEntry, secondInserted, err = tier.publishPinned(address, 128, func() error {
			publications.Add(1)
			return nil
		})
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
	if secondInserted {
		t.Fatal("same content was inserted twice")
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
	tier, err := newArtifactContentTier(ArtifactSST)
	if err != nil {
		t.Fatalf("new tier: %v", err)
	}
	address := mustArtifactContentAddress(t, ArtifactSST,
		"sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	wantErr := errors.New("publish failed")

	if _, _, err := tier.publishPinned(address, 128, func() error {
		return wantErr
	}); !errors.Is(err, wantErr) {
		t.Fatalf("publication error=%v, want %v", err, wantErr)
	}
	if _, ok := tier.probe(address); ok {
		t.Fatal("failed publication left a searchable entry")
	}
}

func TestArtifactContentTierReleaseDoesNotDeleteReplacement(t *testing.T) {
	tier, err := newArtifactContentTier(ArtifactSST)
	if err != nil {
		t.Fatalf("new tier: %v", err)
	}
	address := mustArtifactContentAddress(t, ArtifactSST,
		"sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	oldEntry, inserted, err := tier.publishPinned(address, 128, func() error { return nil })
	if err != nil || !inserted {
		t.Fatalf("publish old entry: inserted=%t err=%v", inserted, err)
	}
	if detached, err := tier.detach(oldEntry, func(artifactContentAddress) error {
		t.Fatal("pinned old entry was removed immediately")
		return nil
	}); err != nil || !detached {
		t.Fatalf("detach old entry: detached=%t err=%v", detached, err)
	}

	newEntry, inserted, err := tier.publishPinned(address, 128, func() error { return nil })
	if err != nil || !inserted {
		t.Fatalf("publish replacement: inserted=%t err=%v", inserted, err)
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
	tier, err := newArtifactContentTier(ArtifactBloom)
	if err != nil {
		t.Fatalf("new tier: %v", err)
	}
	address := mustArtifactContentAddress(t, ArtifactBloom,
		"sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789")
	entry, _, err := tier.publishPinned(address, 32, func() error { return nil })
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
