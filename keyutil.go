package isledb

import "encoding/binary"

// incrementKey returns the next lexicographic key after the given key.
// Used to convert an inclusive upper bound into an exclusive bound.
func incrementKey(key []byte) []byte {
	if len(key) == 0 {
		return nil
	}

	orig := make([]byte, len(key))
	copy(orig, key)
	result := make([]byte, len(key))
	copy(result, orig)

	for i := len(result) - 1; i >= 0; i-- {
		if result[i] < 0xFF {
			result[i]++
			return result
		}
		result[i] = 0
	}

	return append(orig, 0)
}

// BigEndianUint64KeyPositionExtractor decodes an 8-byte big-endian key into a
// uint64 position. It is intended for monotonic keyspaces where key order
// matches logical position order.
func BigEndianUint64KeyPositionExtractor(key []byte) (uint64, bool) {
	if len(key) != 8 {
		return 0, false
	}
	return binary.BigEndian.Uint64(key), true
}
