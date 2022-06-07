package sparsecat

import (
	"os"

	"golang.org/x/sys/unix"
)

func getBlockDeviceSize(file *os.File) (size int, err error) {
	conn, err := file.SyscallConn()
	if err != nil {
		return 0, err
	}

	connerr := conn.Control(func(fd uintptr) {
		size, err = unix.IoctlGetInt(int(fd), unix.BLKGETSIZE64)
	})

	if connerr != nil {
		return 0, connerr
	}

	return size, err
}
