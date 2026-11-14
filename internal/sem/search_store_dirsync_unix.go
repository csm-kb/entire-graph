//go:build !windows

package sem

import "os"

// syncSearchStoreDirectory flushes directory's entries, so a name created or
// renamed in it survives a crash. A rename to a fresh name gets no
// auto_da_alloc flush from ext4 the way a rename over an existing file did.
func syncSearchStoreDirectory(directory *os.Root) error {
	handle, err := directory.Open(".")
	if err != nil {
		return err
	}
	if err := handle.Sync(); err != nil {
		_ = handle.Close()
		return err
	}
	return handle.Close()
}
