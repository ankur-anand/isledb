package internal

import "bytes"

// OverlapsRange reports whether [aMin, aMax] overlaps with [bMin, bMax].
// Empty bMin/bMax means unbounded on that side.
func OverlapsRange(aMin, aMax, bMin, bMax []byte) bool {
	if len(aMin) == 0 || len(aMax) == 0 {
		return false
	}
	if len(bMin) == 0 && len(bMax) == 0 {
		return true
	}
	if len(bMin) == 0 {
		return bytes.Compare(aMin, bMax) <= 0
	}
	if len(bMax) == 0 {
		return bytes.Compare(bMin, aMax) <= 0
	}
	return bytes.Compare(aMin, bMax) <= 0 && bytes.Compare(bMin, aMax) <= 0
}
