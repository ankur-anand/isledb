package diskcache

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestArtifactContentAddressUsesChecksumRatherThanSSTID(t *testing.T) {
	checksum := "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	first, err := artifactContentAddressFor(ArtifactDescriptor{
		Key:      ArtifactKey{Kind: ArtifactSST, SSTID: "sst-a"},
		Checksum: checksum,
	})
	if err != nil {
		t.Fatalf("first address: %v", err)
	}
	second, err := artifactContentAddressFor(ArtifactDescriptor{
		Key:      ArtifactKey{Kind: ArtifactSST, SSTID: "sst-b"},
		Checksum: checksum,
	})
	if err != nil {
		t.Fatalf("second address: %v", err)
	}

	if first != second {
		t.Fatalf("same content produced different addresses: %v != %v", first, second)
	}
	want := filepath.Join("sst", "01", checksum[len("sha256:"):]+".sst")
	if got := first.relativePath(); got != want {
		t.Fatalf("relative path=%q, want %q", got, want)
	}
}

func TestArtifactContentAddressSeparatesDifferentContentForSameSSTID(t *testing.T) {
	first, err := artifactContentAddressFor(ArtifactDescriptor{
		Key:      ArtifactKey{Kind: ArtifactSST, SSTID: "sst-a"},
		Checksum: "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
	})
	if err != nil {
		t.Fatalf("first address: %v", err)
	}
	second, err := artifactContentAddressFor(ArtifactDescriptor{
		Key:      ArtifactKey{Kind: ArtifactSST, SSTID: "sst-a"},
		Checksum: "sha256:1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
	})
	if err != nil {
		t.Fatalf("second address: %v", err)
	}

	if first == second {
		t.Fatal("different content produced the same address")
	}
}

func TestArtifactContentAddressSeparatesArtifactKinds(t *testing.T) {
	checksum := "sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
	sst, err := artifactContentAddressFor(ArtifactDescriptor{
		Key:      ArtifactKey{Kind: ArtifactSST, SSTID: "sst-a"},
		Checksum: checksum,
	})
	if err != nil {
		t.Fatalf("SST address: %v", err)
	}
	bloom, err := artifactContentAddressFor(ArtifactDescriptor{
		Key:      ArtifactKey{Kind: ArtifactBloom, SSTID: "sst-a"},
		Checksum: checksum,
	})
	if err != nil {
		t.Fatalf("Bloom address: %v", err)
	}

	if sst == bloom {
		t.Fatal("SST and Bloom produced the same address")
	}
	if got, want := bloom.relativePath(), filepath.Join(
		"bloom", "ab", checksum[len("sha256:"):]+".bloom",
	); got != want {
		t.Fatalf("Bloom relative path=%q, want %q", got, want)
	}
}

func TestArtifactContentAddressCanonicalizesAndValidatesChecksum(t *testing.T) {
	upper, err := artifactContentAddressFor(ArtifactDescriptor{
		Key:      ArtifactKey{Kind: ArtifactSST, SSTID: "sst-a"},
		Checksum: "sha256:ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789",
	})
	if err != nil {
		t.Fatalf("uppercase checksum: %v", err)
	}
	if got, want := filepath.Base(upper.relativePath()),
		"abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789.sst"; got != want {
		t.Fatalf("canonical filename=%q, want %q", got, want)
	}

	_, err = artifactContentAddressFor(ArtifactDescriptor{
		Key:      ArtifactKey{Kind: ArtifactSST, SSTID: "sst-a"},
		Checksum: "sha256:not-a-checksum",
	})
	if !errors.Is(err, ErrInvalidArtifactDescriptor) {
		t.Fatalf("invalid checksum error=%v, want %v", err, ErrInvalidArtifactDescriptor)
	}
}
