package isledb

import "bytes"

// KeyRange describes a half-open key range: [Min, Max).
//
// A nil or empty Min means the beginning of the keyspace. A nil or empty Max
// means the end of the keyspace.
type KeyRange struct {
	Min []byte
	Max []byte
}

// PrefixRange returns the half-open key range containing keys with prefix.
func PrefixRange(prefix []byte) KeyRange {
	min := append([]byte(nil), prefix...)
	return KeyRange{
		Min: min,
		Max: prefixUpperBound(prefix),
	}
}

func (r KeyRange) isZero() bool {
	return len(r.Min) == 0 && len(r.Max) == 0
}

func prefixUpperBound(prefix []byte) []byte {
	if len(prefix) == 0 {
		return nil
	}
	upper := append([]byte(nil), prefix...)
	for i := len(upper) - 1; i >= 0; i-- {
		if upper[i] != 0xff {
			upper[i]++
			return upper[:i+1]
		}
	}
	return nil
}

// sstOverlapsHalfOpenRange compares an SST's closed manifest span with a
// caller-visible half-open key range.
func sstOverlapsHalfOpenRange(sst sstMetadata, r KeyRange) bool {
	if len(sst.MinKey) == 0 || len(sst.MaxKey) == 0 {
		return false
	}
	if len(r.Min) > 0 && bytes.Compare(sst.MaxKey, r.Min) < 0 {
		return false
	}
	if len(r.Max) > 0 && bytes.Compare(sst.MinKey, r.Max) >= 0 {
		return false
	}
	return true
}
