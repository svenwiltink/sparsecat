//go:build !linux
// +build !linux

package sparsecat

import (
	"errors"
	"os"
)

func getBlockDeviceSize(f *os.File) (int64, error) {
	return 0, errors.New("operation not supported")
}
