package isledb

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
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

// maxBloomSidecarBytes bounds both parsing work and the allocation performed
// by ristretto's bloom decoder. Real filters are much smaller than this; an
// object above the limit is corrupt rather than a useful SST sidecar.
const maxBloomSidecarBytes = 64 << 20

// bloomSidecar mirrors the stable JSON shape emitted by z.Bloom.JSONMarshal.
// It is decoded once here so hostile fields can be rejected before reaching
// z.JSONUnmarshal, whose SetLocs==0 path attempts an effectively unbounded
// allocation in ristretto v2.4.0.
type bloomSidecar struct {
	FilterSet []byte
	SetLocs   uint64
}

func parseBloomFilter(data []byte) (*z.Bloom, error) {
	if len(data) == 0 {
		return nil, errors.New("empty bloom filter")
	}
	if len(data) > maxBloomSidecarBytes {
		return nil, fmt.Errorf("bloom filter bytes=%d max=%d", len(data), maxBloomSidecarBytes)
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var sidecar bloomSidecar
	if err := decoder.Decode(&sidecar); err != nil {
		return nil, fmt.Errorf("decode bloom filter: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return nil, errors.New("bloom filter has trailing JSON")
		}
		return nil, fmt.Errorf("decode bloom filter trailer: %w", err)
	}
	if len(sidecar.FilterSet) == 0 {
		return nil, errors.New("bloom filter has no bit vector")
	}
	// The writer uses at most six probes. The wider bound permits compatible
	// historical writers while preventing a corrupt sidecar from turning each
	// lookup into unbounded CPU work.
	if sidecar.SetLocs == 0 || sidecar.SetLocs > 64 {
		return nil, fmt.Errorf("bloom filter probes=%d outside [1,64]", sidecar.SetLocs)
	}

	filter, err := z.JSONUnmarshal(data)
	if err != nil {
		return nil, fmt.Errorf("decode bloom filter payload: %w", err)
	}
	if filter == nil {
		return nil, errors.New("bloom filter decoded to nil")
	}
	return filter, nil
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
