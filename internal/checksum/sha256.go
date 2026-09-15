package checksum

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
)

var ErrInvalidSHA256 = errors.New("invalid SHA-256 checksum")

// ParseSHA256 decodes the canonical algorithm-prefixed checksum used by
// immutable IsleDB artifacts. Hex digits are case-insensitive.
func ParseSHA256(value string) ([sha256.Size]byte, error) {
	var sum [sha256.Size]byte
	const prefix = "sha256:"
	if !strings.HasPrefix(value, prefix) || len(value) != len(prefix)+hex.EncodedLen(sha256.Size) {
		return sum, ErrInvalidSHA256
	}
	if _, err := hex.Decode(sum[:], []byte(value[len(prefix):])); err != nil {
		return sum, ErrInvalidSHA256
	}
	return sum, nil
}
