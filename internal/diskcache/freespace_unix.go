//go:build linux || darwin

package diskcache

import "golang.org/x/sys/unix"

// FreeBytes reports the space available to unprivileged users on the
// filesystem holding dir.
func FreeBytes(dir string) (uint64, error) {
	var stat unix.Statfs_t
	if err := unix.Statfs(dir, &stat); err != nil {
		return 0, err
	}
	return stat.Bavail * uint64(stat.Bsize), nil
}
