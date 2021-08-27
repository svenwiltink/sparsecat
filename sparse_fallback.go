// +build !darwin,!dragonfly,!freebsd,!linux,!netbsd,!openbsd,!solaris,!windows

package sparsecat

import "os"

// fallback implementations for operating systems that don't support SEEK_HOLE and SEEK_DATA. It returns
// the offset and the end of the file.
func detectDataSection(file *os.File, offset int64) (start int64, end int64, err error) {
	fi, err := file.Stat()
	if err != nil {
		return 0, 0, err
	}

	return offset, fi.Size(), nil
}

func supportsSeekHole(f *os.File) bool {
	return true
}

func getBlockDeviceSize() (int64, error) {
	return 0, errors.New("operation not supported")
}
