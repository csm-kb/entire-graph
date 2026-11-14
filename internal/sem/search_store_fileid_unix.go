//go:build !windows

package sem

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

// openedFileID is the open file's identity, <device>:<inode>, from the
// FileInfo the caller already took of it.
func openedFileID(_ *os.File, info os.FileInfo) (string, error) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return "", errors.New("file identity unavailable on this platform")
	}
	return fmt.Sprintf("%d:%d", uint64(stat.Dev), uint64(stat.Ino)), nil //nolint:unconvert // Dev and Ino are narrower than uint64 on some platforms
}
