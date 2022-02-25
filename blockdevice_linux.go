package sparsecat

import (
	"os"

	"golang.org/x/sys/unix"
)

func getBlockDeviceSize(file *os.File) (int, error) {
	return unix.IoctlGetInt(int(file.Fd()), unix.BLKGETSIZE64)
}
