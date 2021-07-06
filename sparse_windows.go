package sparsecat

import (
	"errors"
	"golang.org/x/sys/windows"
	"io"
	"os"
	"syscall"
	"unsafe"
)

const (
	queryAllocRanges = 0x000940CF
	setSparse        = 0x000900c4
)

// detectDataSection detects the start and end of the next section containing data. This
// skips any sparse sections.
func detectDataSection(file *os.File, offset int64) (start int64, end int64, err error) {
	// typedef struct _FILE_ALLOCATED_RANGE_BUFFER {
	//  LARGE_INTEGER FileOffset;
	//  LARGE_INTEGER Length;
	//} FILE_ALLOCATED_RANGE_BUFFER, *PFILE_ALLOCATED_RANGE_BUFFER;
	type allocRangeBuffer struct{ offset, length int64 }

	// TODO: prevent this stat call
	s, err := file.Stat()
	if err != nil {
		return 0, 0, err
	}

	queryRange := allocRangeBuffer{offset, s.Size()}
	allocRanges := make([]allocRangeBuffer, 1)

	var bytesReturned uint32
	err = windows.DeviceIoControl(
		windows.Handle(file.Fd()), queryAllocRanges,
		(*byte)(unsafe.Pointer(&queryRange)), uint32(unsafe.Sizeof(queryRange)),
		(*byte)(unsafe.Pointer(&allocRanges[0])), uint32(len(allocRanges)*int(unsafe.Sizeof(allocRanges[0]))),
		&bytesReturned, nil,
	)

	if err != nil {
		if !errors.Is(err, syscall.ERROR_MORE_DATA) {
			panic(err)
		}
	}

	// no error and nothing returned, assume EOF
	if bytesReturned == 0 {
		return 0, 0, io.EOF
	}

	return allocRanges[0].offset, allocRanges[0].offset + allocRanges[0].length, nil
}

func SparseTruncate(file *os.File, size int64) error {
	err := windows.DeviceIoControl(
		windows.Handle(file.Fd()), setSparse,
		nil, 0,
		nil, 0,
		nil, nil,
	)

	if err != nil {
		return err
	}

	err = file.Truncate(size)
	if err != nil {
		return nil
	}
	return err
}
