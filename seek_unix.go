package main

import (
	"errors"
	"golang.org/x/sys/unix"
	"io"
	"log"
	"os"
	"syscall"
)

const (
	SEEK_DATA = 3
	SEEK_HOLE = 4
)

func DetectDataSection(file *os.File, offset int64) (start int64, end int64, err error) {
	var syserr syscall.Errno

	log.Println("seeking to data. current offset ", offset)
	startOfData, err := unix.Seek(int(file.Fd()), offset, SEEK_DATA)
	if errors.As(err, &syserr) {
		if syserr == syscall.ENXIO {
			return -1, -1, io.EOF
		}
		return -1, -1, err
	}

	if err != nil {
		return -1, -1, err
	}

	log.Println("seeking to hole. current offset ", offset)
	endOfData, err := unix.Seek(int(file.Fd()), startOfData, SEEK_HOLE)
	if errors.As(err, &syserr) {
		if syserr == syscall.ENXIO {
			return -1, -1, io.EOF
		}
		return -1, -1, err
	}

	return startOfData, endOfData, err
}
