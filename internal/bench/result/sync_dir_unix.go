//go:build !windows

package result

import (
	"os"

	"golang.org/x/sys/unix"
)

// syncDirOS makes a rename durable by fsyncing the directory that holds it.
func syncDirOS(root *os.Root) error {
	dir, err := root.Open(".")
	if err != nil {
		return err
	}
	defer dir.Close()
	return unix.Fsync(int(dir.Fd()))
}
