package main

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

func DetectDataSection(file *os.File, offset int64) (start int64, end int64, err error) {
	var syserr syscall.Errno

	startOfData, err := unix.Seek(int(file.Fd()), offset, SEEK_DATA)
	if errors.As(err, &syserr) {
		if syserr == syscall.ENXIO {
			return -1, -1, io.EOF
		}
		return -1, -1, fmt.Errorf("error seeking to data: %w", err)
	}

	if err != nil {
		return -1, -1, fmt.Errorf("error seeking to data: %w", err)
	}

	endOfData, err := unix.Seek(int(file.Fd()), startOfData, SEEK_HOLE)
	if errors.As(err, &syserr) {
		if syserr == syscall.ENXIO {
			return -1, -1, io.EOF
		}
		return -1, -1, fmt.Errorf("error seeking to hole: %w", err)
	}

	if err != nil {
		return -1, -1, fmt.Errorf("error seeking to hole: %w", err)
	}

	return startOfData, endOfData, err
}
