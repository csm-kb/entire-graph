//go:build windows

package sem

import "os"

// syncSearchStoreDirectory is a no-op on Windows, where directory durability
// rests on NTFS journaling.
func syncSearchStoreDirectory(*os.Root) error {
	return nil
}
