//go:build windows

package sem

import (
	"fmt"
	"os"
	"syscall"
)

// openedFileID is the open file's identity, <volume serial>:<file index>.
// Go's Windows FileInfo does not expose it, so it comes from
// GetFileInformationByHandle, as openedFileLinkCount does.
func openedFileID(file *os.File, _ os.FileInfo) (string, error) {
	var info syscall.ByHandleFileInformation
	if err := syscall.GetFileInformationByHandle(syscall.Handle(file.Fd()), &info); err != nil {
		return "", err
	}
	index := uint64(info.FileIndexHigh)<<32 | uint64(info.FileIndexLow)
	return fmt.Sprintf("%x:%x", info.VolumeSerialNumber, index), nil
}
