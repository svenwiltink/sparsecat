package sparsecat

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

const (
	sizeIndicator byte = 's'
	dataIndicator byte = 'w'
	endIndicator  byte = 'e'
)

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	for index, _ := range p {
		p[index] = 0
	}

	return len(p), nil
}

// ToNormalReader converts a sparsecat stream into a normal data stream.
// This can be useful when combining sparsecat with other readers. When
// serving a sparsecat file on a webserver for example.
//
// Returns an io.ReadCloser for early cancellation in case of an error
// on the reader side
func ToNormalReader(input io.Reader) io.ReadCloser {
	reader, writer := io.Pipe()

	go func() {
		var currentOffset int64 = 0
		size, err := getFileSize(input)

		if err != nil {
			writer.CloseWithError(fmt.Errorf("error determining target file size: %w", err))
			return
		}

		// use 8 + 8 here as that is the maximum buffer size we need for parsing getting
		// the data size. The two int64 for writing data sections.
		var segmentHeader [8 + 8]byte

		for {
			// first byte contains the segment type
			_, err := io.ReadFull(input, segmentHeader[0:1])
			if err != nil {
				writer.CloseWithError(fmt.Errorf("error reading segmentHeader header: %w", err))
				return
			}

			switch segmentHeader[0] {
			case endIndicator:
				_, err = io.Copy(writer, io.LimitReader(zeroReader{}, size-currentOffset))
				writer.Close()
				return
			case dataIndicator:
				_, err = io.ReadFull(input, segmentHeader[:])
				if err != nil {
					writer.CloseWithError(fmt.Errorf("error reading data header: %w", err))
					return
				}

				offset := binary.LittleEndian.Uint64(segmentHeader[:9])
				length := binary.LittleEndian.Uint64(segmentHeader[8:])

				// instead of seeking we fill the stream with enough empty data
				_, err = io.Copy(writer, io.LimitReader(zeroReader{}, int64(offset)-currentOffset))
				_, err = io.Copy(writer, io.LimitReader(input, int64(length)))
				if err != nil {
					writer.CloseWithError(fmt.Errorf("error copying data: %w", err))
					return
				}

				currentOffset = int64(offset + length)
			default:
				writer.CloseWithError(fmt.Errorf("invalid section type: %b", segmentHeader[0]))
				return
			}
		}
	}()

	return reader
}

// ReceiveSparseFile parses the input stream and writes the data to the target file
// the target file is first truncated to the correct size. Data that already exists
// in the target file is _not_ removed yet. This means the target file should be
// empty in order for the sparse file to be received correctly.
func ReceiveSparseFile(output *os.File, input io.Reader) error {
	size, err := getFileSize(input)
	if err != nil {
		return fmt.Errorf("error determining target file size: %w", err)
	}

	err = output.Truncate(size)
	if err != nil {
		return fmt.Errorf("error resizing target file: %w", err)
	}

	// use 8 + 8 here as that is the maximum buffer size we need for parsing getting
	// the data size. The two int64 for writing data sections.
	var segmentHeader [8 + 8]byte

	for {
		// first byte contains the segment type
		_, err := io.ReadFull(input, segmentHeader[0:1])
		if err != nil {
			return fmt.Errorf("error reading segmentHeader header: %w", err)
		}

		switch segmentHeader[0] {
		case endIndicator:
			return nil
		case dataIndicator:
			_, err = io.ReadFull(input, segmentHeader[:])
			if err != nil {
				return fmt.Errorf("error reading data header: %w", err)
			}

			offset := binary.LittleEndian.Uint64(segmentHeader[:9])
			length := binary.LittleEndian.Uint64(segmentHeader[8:])

			_, err = output.Seek(int64(offset), io.SeekStart)
			if err != nil {
				return fmt.Errorf("error seeking to start of data section: %w", err)
			}

			_, err = io.Copy(output, io.LimitReader(input, int64(length)))
			if err != nil {
				return fmt.Errorf("error copying data: %w", err)
			}
		default:
			return fmt.Errorf("invalid section type: %v", segmentHeader[0])
		}
	}
}

// SendSparseFile sends the sparse file to the output reader in a simple binary format.
// Starting with the size of the file followed by section of data indicating the offset and
// length of data to be written. The format is binary compatible with ceph rbd import-diff.
func SendSparseFile(input *os.File, output io.Writer) error {
	var offset int64 = 0

	info, err := input.Stat()
	if err != nil {
		return fmt.Errorf("error running stat: %w", err)
	}

	err = writeSizeSection(output, info.Size())
	if err != nil {
		return fmt.Errorf("error writing size section: %w", err)
	}

	for {
		start, end, err := detectDataSection(input, offset)
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return fmt.Errorf("error detecting data section: %w", err)
		}

		err = writeDataSection(output, input, start, end)
		if err != nil {
			return fmt.Errorf("error writing data: %w", err)
		}

		offset = end
	}

	err = writeEndSection(output)
	if err != nil {
		return fmt.Errorf("error writing end section: %w", err)
	}

	return nil
}

func writeEndSection(w io.Writer) error {
	_, err := w.Write([]byte{endIndicator})
	return err
}

func writeSizeSection(w io.Writer, size int64) error {
	var buf [9]byte
	buf[0] = sizeIndicator

	binary.LittleEndian.PutUint64(buf[1:], uint64(size))

	_, err := w.Write(buf[:])
	return err
}

func writeDataSection(w io.Writer, r io.ReadSeeker, start int64, end int64) error {
	_, err := r.Seek(start, io.SeekStart)
	if err != nil {
		return err
	}

	var buf [1 + 8 + 8]byte
	buf[0] = dataIndicator

	length := end - start

	binary.LittleEndian.PutUint64(buf[1:], uint64(start))
	binary.LittleEndian.PutUint64(buf[1+8:], uint64(length))

	_, err = w.Write(buf[:])
	if err != nil {
		return err
	}

	_, err = io.Copy(w, io.LimitReader(r, length))
	return err
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
