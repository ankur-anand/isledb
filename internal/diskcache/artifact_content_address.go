package diskcache

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path/filepath"
)

// artifactContentAddress is the persistent identity of an immutable cached
// artifact. It is independent of an SST ID because SST IDs are object names,
// not content addresses.
type artifactContentAddress struct {
	kind     ArtifactKind
	checksum [sha256.Size]byte
}

func artifactContentAddressFor(desc ArtifactDescriptor) (artifactContentAddress, error) {
	if !desc.Key.Kind.valid() {
		return artifactContentAddress{}, fmt.Errorf(
			"%w: kind=%d", ErrInvalidArtifactDescriptor, desc.Key.Kind)
	}
	checksum, err := parseSHA256Checksum(desc.Checksum)
	if err != nil {
		return artifactContentAddress{}, fmt.Errorf("%w: %v", ErrInvalidArtifactDescriptor, err)
	}
	return artifactContentAddress{kind: desc.Key.Kind, checksum: checksum}, nil
}

// relativePath returns a path relative to the cache's format-version root.
// Choosing and maintaining that root belongs to cache initialization, not
// artifact identity.
func (address artifactContentAddress) relativePath() string {
	checksum := hex.EncodeToString(address.checksum[:])
	return filepath.Join(
		address.kind.dirName(),
		checksum[:2],
		checksum+address.kind.extension(),
	)
}
