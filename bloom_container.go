package isledb

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/cespare/xxhash/v2"
)

const bloomTrailerMagic = "ISLEBLM1"
const bloomTrailerLen = 16

// maxBloomSidecarBytes bounds a sidecar's size, and so the work to build or
// parse one. Real filters are far smaller; a larger object is corrupt rather
// than a useful sidecar.
const maxBloomSidecarBytes = 64 << 20

func bloomHashKey(key []byte) uint64 {
	return xxhash.Sum64(key)
}

func bloomChecksum(data []byte) string {
	if len(data) == 0 {
		return ""
	}
	sum := sha256.Sum256(data)
	return fmt.Sprintf("sha256:%x", sum[:])
}

func validateBloomChecksum(expected string, data []byte) error {
	if expected == "" {
		return errors.New("missing bloom checksum")
	}
	algo, _, ok := strings.Cut(expected, ":")
	if !ok || algo != "sha256" {
		return fmt.Errorf("unsupported bloom checksum %q", expected)
	}
	if actual := bloomChecksum(data); actual != expected {
		return errors.New("bloom checksum mismatch")
	}
	return nil
}

// writeBloomSidecar appends a filter and its trailer after an SST payload. An
// empty filter writes nothing.
func writeBloomSidecar(w io.Writer, filter []byte) error {
	if len(filter) == 0 {
		return nil
	}
	if _, err := w.Write(filter); err != nil {
		return err
	}
	return appendBloomTrailer(w, int64(len(filter)))
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
