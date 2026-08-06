package diskcache

import (
	"os"

	"github.com/dgraph-io/ristretto/v2/z"
)

// MmapFile memory-maps a file for read-only access.
func MmapFile(f *os.File) ([]byte, error) {
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() == 0 {
		return nil, nil
	}
	return z.Mmap(f, false, info.Size())
}

// Munmap unmaps previously mapped memory.
func Munmap(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	return z.Munmap(data)
}
