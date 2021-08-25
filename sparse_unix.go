// +build darwin dragonfly freebsd linux netbsd openbsd solaris

package sparsecat

import (
	"bytes"
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

	BLK_READ_BUFFER = 4_000_000 // 4MB
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

// slowDetectDataSection detects data sections by reading a buffer at the time, discarding any that don't contain
// data. Only returns EOF when there is no data to be copied anymore
func slowDetectDataSection(file io.Reader, currentOffset int64) (start int64, end int64, reader io.Reader, err error) {
	var buf [BLK_READ_BUFFER]byte

	for {
		read, err := file.Read(buf[:])
		if err != nil && !errors.Is(err, io.EOF) {
			return 0, 0, nil, err
		}

		if read == 0 && errors.Is(err, io.EOF) {
			return 0, 0, nil, err
		}

		// buffer is empty, discard data but advance offset unless EOF
		if isBufferEmpty(buf[:read]) {
			currentOffset += int64(read)
			continue
		}

		return currentOffset, currentOffset + int64(read), bytes.NewReader(buf[:read]), nil
	}
}

func isBufferEmpty(buf []byte) bool {
	for _, b := range buf {
		if b !=0 {
			return false
		}
	}
	return true
}

func getBlockDeviceSize(file *os.File) (int, error) {
	return unix.IoctlGetInt(int(file.Fd()), unix.BLKGETSIZE64)
}

func SparseTruncate(file *os.File, size int64) error {
	return file.Truncate(size)
}
