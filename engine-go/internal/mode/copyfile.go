package mode

import (
	"io"
	"os"
)

// copyFile is the copy+delete fallback used by MoveFile across mounts.
func copyFile(from, to string) (int64, error) {
	src, err := os.Open(from)
	if err != nil {
		return 0, err
	}
	defer src.Close()
	dst, err := os.Create(to)
	if err != nil {
		return 0, err
	}
	defer dst.Close()
	return io.Copy(dst, src)
}
