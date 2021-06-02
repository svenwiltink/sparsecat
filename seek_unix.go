package main

import "os"
import "golang.org/x/sys/unix"

const (
	SEEK_DATA = 3
	SEEK_HOLE = 4
)

func SeekData(file *os.File, offset int64) (off int64, err error) {
	return unix.Seek(int(file.Fd()), offset, SEEK_DATA)
}

func SeekHole(file *os.File, offset int64) (off int64, err error) {
	return unix.Seek(int(file.Fd()), offset, SEEK_HOLE)
}
