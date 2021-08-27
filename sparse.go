package sparsecat

import (
	"bytes"
	"errors"
	"io"
	"os"
)

const  	BLK_READ_BUFFER = 4_000_000 // 4MB

func isBlockDevice(fi os.FileInfo) bool {
	return fi.Mode()&os.ModeDevice == os.ModeDevice
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
		if b != 0 {
			return false
		}
	}
	return true
}
