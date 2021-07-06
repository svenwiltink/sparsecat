// +build darwin dragonfly freebsd linux netbsd openbsd solaris

package sparsecat

import (
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
	"io"
	"os"
	"syscall"
)

const (
	SEEK_DATA = 3
	SEEK_HOLE = 4
)

// detectDataSection detects the start and end of the next section containing data. This
// skips any sparse sections. The implementation and supported filesystems are listed
// here https://man7.org/linux/man-pages/man2/lseek.2.html
func detectDataSection(file *os.File, offset int64) (start int64, end int64, err error) {
	var syserr syscall.Errno

	startOfData, err := unix.Seek(int(file.Fd()), offset, SEEK_DATA)
	if errors.As(err, &syserr) {
		if syserr == syscall.ENXIO {
			return 0, 0, io.EOF
		}
		return 0, 0, fmt.Errorf("error seeking to data: %w", err)
	}

	if err != nil {
		return 0, 0, fmt.Errorf("error seeking to data: %w", err)
	}

	endOfData, err := unix.Seek(int(file.Fd()), startOfData, SEEK_HOLE)
	if errors.As(err, &syserr) {
		if syserr == syscall.ENXIO {
			return 0, 0, io.EOF
		}
		return 0, 0, fmt.Errorf("error seeking to hole: %w", err)
	}

	if err != nil {
		return 0, 0, fmt.Errorf("error seeking to hole: %w", err)
	}

	return startOfData, endOfData, err
}

func SparseTruncate(file *os.File, size int64) error {
	return file.Truncate(size)
}
