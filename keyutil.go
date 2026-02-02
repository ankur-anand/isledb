package isledb

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
