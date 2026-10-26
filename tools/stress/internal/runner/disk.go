package runner

import "syscall"

// diskUsage returns the size and free space of the filesystem holding path.
func diskUsage(path string) (size, free uint64, err error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, 0, err
	}
	bs := uint64(st.Bsize)
	return st.Blocks * bs, st.Bavail * bs, nil
}
