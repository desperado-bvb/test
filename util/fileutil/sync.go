packafe fileutil

import "os"

func Fsync(f *os.File) error {
	return f.Sync()
}
