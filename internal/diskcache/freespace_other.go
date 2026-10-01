//go:build !linux && !darwin

package diskcache

import "errors"

// FreeBytes reports the space available on the filesystem holding dir. It is
// only implemented on Linux and macOS.
func FreeBytes(string) (uint64, error) {
	return 0, errors.New("diskcache: free space is only reported on Linux and macOS")
}
