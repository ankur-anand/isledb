package isledb

import (
	"encoding/binary"
	"errors"
	"io"

	"github.com/cespare/xxhash/v2"
	"github.com/dgraph-io/ristretto/v2/z"
)

const bloomTrailerMagic = "ISLEBLM1"
const bloomTrailerLen = 16

func bloomHashKey(key []byte) uint64 {
	return xxhash.Sum64(key)
}

func bloomProbes(bitsPerKey int) int {
	k := int(float64(bitsPerKey) * 0.69)
	if k < 1 {
		k = 1
	}
	// Pebble caps probes at 6 for cache-line tuning once bits/key is high.
	// See pebble/bloom calculateProbes and related simulation notes.
	if k > 6 {
		k = 6
	}
	return k
}

func buildBloomBytes(hashes []uint64, bitsPerKey int) ([]byte, int, error) {
	if bitsPerKey <= 0 || len(hashes) == 0 {
		return nil, 0, nil
	}
	k := bloomProbes(bitsPerKey)
	if k <= 0 {
		return nil, 0, errors.New("invalid bloom probes")
	}
	bloom := z.NewBloomFilter(float64(len(hashes)), float64(k))
	for _, h := range hashes {
		bloom.Add(h)
	}
	return bloom.JSONMarshal(), k, nil
}

func parseBloomFilter(data []byte) (*z.Bloom, error) {
	return z.JSONUnmarshal(data)
}

func appendBloomTrailer(w io.Writer, bloomLen int64) error {
	if bloomLen <= 0 {
		return nil
	}
	var buf [bloomTrailerLen]byte
	copy(buf[:8], bloomTrailerMagic)
	binary.LittleEndian.PutUint64(buf[8:], uint64(bloomLen))
	_, err := w.Write(buf[:])
	return err
}
