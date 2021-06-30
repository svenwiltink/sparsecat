package sparsecat

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
)

const (
	sizeIndicator byte = 's'
	dataIndicator byte = 'w'
	endIndicator  byte = 'e'
)

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	for index := range p {
		p[index] = 0
	}

	return len(p), nil
}

func NewDecoder(reader io.Reader) *SparseDecoder {
	return &SparseDecoder{reader: reader}
}

// SparseDecoder decodes an incoming sparsecat stream. It is able to convert it to a 'normal'
// stream of data using the WriteTo method. An optimized is used  when the target of an io.Copy
// is an *os.File (not a pipe or socket)
type SparseDecoder struct {
	reader io.Reader
}

func (s *SparseDecoder) Read(p []byte) (int, error) {
	panic("not implemented")
}

func (s *SparseDecoder) WriteTo(writer io.Writer) (int64, error) {
	log.Println("decoder fast path!")
	var currentOffset int64 = 0
	size, err := getFileSize(s.reader)

	if err != nil {
		return 0, fmt.Errorf("error determining target file size: %w", err)
	}

	// use 8 + 8 here as that is the maximum buffer size we need for parsing getting
	// the data size. The two int64 for writing data sections.
	var segmentHeader [8 + 8]byte
	var written int64 = 0

	file, isFile := writer.(*os.File)
	if isFile {
		fi, err := file.Stat()
		if err != nil {
			return 0, fmt.Errorf("error getting file stat: %w", err)
		}

		// check if the file is actually a file. Seeking is not supported for pipes and sockets
		if fi.Mode()&(fs.ModeNamedPipe|fs.ModeSocket) != 0 {
			log.Println("not a regular file, falling back to basic normal byte stream")
			isFile = false
		} else {
			err = SparseTruncate(file, size)
			if err != nil {
				return 0, fmt.Errorf("error truncating target file: %w", err)
			}
		}
	} else {
		log.Println("no file detected, falling back to normal file stream")
	}

	for {
		// first byte contains the segment type
		_, err := io.ReadFull(s.reader, segmentHeader[0:1])
		if err != nil {
			return written, fmt.Errorf("error reading segmentHeader header: %w", err)
		}

		switch segmentHeader[0] {
		case endIndicator:
			if !isFile {
				var copied int64
				copied, err = io.Copy(writer, io.LimitReader(zeroReader{}, size-currentOffset))
				written += copied
			}
			return written, err
		case dataIndicator:
			_, err = io.ReadFull(s.reader, segmentHeader[:])
			if err != nil {
				return written, fmt.Errorf("error reading data header: %w", err)
			}

			offset := binary.LittleEndian.Uint64(segmentHeader[:9])
			length := binary.LittleEndian.Uint64(segmentHeader[8:])

			if isFile {
				_, err = file.Seek(int64(offset), io.SeekStart)
				if err != nil {
					return written, fmt.Errorf("error seeking to start of data section: %w", err)
				}
			} else {
				// instead of seeking we fill the stream with enough empty data
				copied, err := io.Copy(writer, io.LimitReader(zeroReader{}, int64(offset)-currentOffset))
				written += copied
				if err != nil {
					return written, fmt.Errorf("error copying data: %w", err)
				}
			}

			copied, err := io.Copy(writer, io.LimitReader(s.reader, int64(length)))
			written += copied
			if err != nil {
				return written, fmt.Errorf("error copying data: %w", err)
			}

			currentOffset = int64(offset + length)
		default:
			return written, fmt.Errorf("invalid section type: %b", segmentHeader[0])
		}
	}
}

func NewEncoder(file *os.File) *Encoder {
	return &Encoder{file: file}
}

// Encoder encodes a file to a stream of sparsecat data.
type Encoder struct {
	file *os.File
}

func (e *Encoder) Read(p []byte) (int, error) {
	panic("not implemented")
}

func (e *Encoder) WriteTo(writer io.Writer) (int64, error) {
	log.Println("encoder fast path")
	info, err := e.file.Stat()
	if err != nil {
		return 0, fmt.Errorf("error running stat: %w", err)
	}

	var written int64 = 0
	var offset int64 = 0

	writtenSize, err := writeSizeSection(writer, info.Size())
	written += int64(writtenSize)
	if err != nil {
		return written, fmt.Errorf("error writing size section: %w", err)
	}

	for {
		start, end, err := detectDataSection(e.file, offset)
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return written, fmt.Errorf("error detecting data section: %w", err)
		}

		copied, err := writeDataSection(writer, e.file, start, end)
		written += copied
		if err != nil {
			return written, fmt.Errorf("error writing data: %w", err)
		}

		offset = end
	}

	endSectionWritten, err := writeEndSection(writer)
	written += int64(endSectionWritten)
	if err != nil {
		return written, fmt.Errorf("error writing end section: %w", err)
	}

	return written, nil
}

// TODO: remove once Read has been implemented
// Currently exists so net/http doens't wrap this in a noopcloser
func (Encoder) Close() error {
	return nil
}

func writeEndSection(w io.Writer) (int, error) {
	return w.Write([]byte{endIndicator})
}

func writeSizeSection(w io.Writer, size int64) (int, error) {
	var buf [9]byte
	buf[0] = sizeIndicator

	binary.LittleEndian.PutUint64(buf[1:], uint64(size))

	return w.Write(buf[:])
}

func writeDataSection(w io.Writer, r io.ReadSeeker, start int64, end int64) (int64, error) {
	_, err := r.Seek(start, io.SeekStart)
	if err != nil {
		return 0, err
	}

	var buf [1 + 8 + 8]byte
	buf[0] = dataIndicator

	length := end - start

	binary.LittleEndian.PutUint64(buf[1:], uint64(start))
	binary.LittleEndian.PutUint64(buf[1+8:], uint64(length))

	written, err := w.Write(buf[:])
	if err != nil {
		return int64(written), err
	}

	copied, err := io.Copy(w, io.LimitReader(r, length))
	return int64(written) + copied, err
}

func getFileSize(input io.Reader) (int64, error) {
	// 1 byte for segment type. 8 bytes for int64
	var header [1 + 8]byte
	_, err := io.ReadFull(input, header[:])
	if err != nil {
		return 0, err
	}

	if header[0] != sizeIndicator {
		return 0, fmt.Errorf("invalid header. Expected size segment but got %s", string(header[0]))
	}

	size := binary.LittleEndian.Uint64(header[1:])
	return int64(size), nil
}
