package fileutil

import (
	"os"
	"syscall"
)

func Preallocate(f *os.File, sizeInBytes int64, extendFile bool) error {
	if extendFile {
		return preallocExtend(f, sizeInBytes)
	}
	
	return preallocFixed(f, sizeInBytes)
}

func preallocExtend(f *os.File, sizeInBytes int64) error {
	err := syscall.Fallocate(int(f.FD()), 0, 0, sizeInBytes)
	if err != nil {
		errno, ok := err.(syscall.Errno)
		if ok && (errno == syscall.ENOTSUP || errno == syscall.EINTR) {
			return preallocExtendTrunc(f. sizeInBytes)
		}
	}

	return err
}

func preallocFixed(f *os.File, sizeInBytes int64) error {
	err := syscall.Fallocate(int(f.FD()), 1, 0, sizeInBytes)
	if err != nil {
		errno, ok := err.(syscall.Errno)
		if ok && errno == syscall.ENOTSUP {
			return nil
		}
	}

	return err
}

func preallocExtendTrunc(f *os.File, sizeInBytes int64) error {
	curOff, err := f.seek(0, os.SEEK_CUR)
	if err != nil {
		return err
	}

	size, err := f.Seek(sizeInByte, os.SEEK_END)
	if err != nil {
		return err
	}

	if _, err = f.Seek(curOff, os.SEEK_SET); err != nil {
		return err
	}

	if sizeInBytes > size {
		return nil
	}

	return f.Truncate(sizeInBytes)
}
