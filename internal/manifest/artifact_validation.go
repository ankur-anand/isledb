package manifest

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"strings"
)

// ValidateArtifacts verifies the framing and integrity metadata required to
// load every immutable SST and Bloom artifact referenced by the manifest. It
// performs no object-store reads and does not verify the artifact bytes.
func (m *Manifest) ValidateArtifacts() error {
	if m == nil {
		return fmt.Errorf("%w: nil manifest", ErrInvalidManifest)
	}
	for index := range m.L0SSTs {
		if err := validateSSTArtifactMetadata(m.L0SSTs[index]); err != nil {
			return fmt.Errorf("%w: L0 SST %q: %v",
				ErrInvalidManifest, m.L0SSTs[index].ID, err)
		}
	}
	for levelIndex := range m.Levels {
		level := &m.Levels[levelIndex]
		for sstIndex := range level.SSTs {
			if err := validateSSTArtifactMetadata(level.SSTs[sstIndex]); err != nil {
				return fmt.Errorf("%w: L%d SST %q: %v",
					ErrInvalidManifest, level.Number, level.SSTs[sstIndex].ID, err)
			}
		}
	}
	return nil
}

func validateSSTArtifactMetadata(sst SSTMeta) error {
	if sst.ID == "" {
		return fmt.Errorf("missing ID")
	}
	if sst.Size <= 0 {
		return fmt.Errorf("invalid size %d", sst.Size)
	}
	if err := validateArtifactSHA256("SST checksum", sst.Checksum); err != nil {
		return err
	}

	bloom := sst.Bloom
	if bloom.Offset != sst.Size {
		return fmt.Errorf("Bloom offset %d does not equal SST payload size %d",
			bloom.Offset, sst.Size)
	}
	if bloom.Length < 0 {
		return fmt.Errorf("invalid Bloom length %d", bloom.Length)
	}
	if bloom.Length == 0 {
		if bloom.Checksum != "" {
			return fmt.Errorf("Bloom checksum is present without Bloom bytes")
		}
		return nil
	}
	if bloom.Offset > math.MaxInt64-bloom.Length {
		return fmt.Errorf("Bloom extent overflows int64")
	}
	if bloom.BitsPerKey <= 0 {
		return fmt.Errorf("invalid Bloom bits_per_key %d", bloom.BitsPerKey)
	}
	if bloom.K <= 0 {
		return fmt.Errorf("invalid Bloom probe count %d", bloom.K)
	}
	return validateArtifactSHA256("Bloom checksum", bloom.Checksum)
}

func validateArtifactSHA256(field, checksum string) error {
	const prefix = "sha256:"
	if !strings.HasPrefix(checksum, prefix) ||
		len(checksum) != len(prefix)+hex.EncodedLen(sha256.Size) {
		return fmt.Errorf("invalid %s %q", field, checksum)
	}
	if _, err := hex.DecodeString(checksum[len(prefix):]); err != nil {
		return fmt.Errorf("invalid %s %q", field, checksum)
	}
	return nil
}
