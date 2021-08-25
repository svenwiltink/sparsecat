package sparsecat

import (
	"os"
)

func isBlockDevice(fi os.FileInfo) bool {
	return fi.Mode() & os.ModeDevice == os.ModeDevice
}