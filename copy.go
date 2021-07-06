package sparsecat

import (
	"bytes"
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

type onlyReader struct {
	io.Reader
}

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

// SparseDecoder decodes an incoming sparsecat stream. It is able to convert it to a 'normal'
// stream of data using the WriteTo method. An optimized path is used  when the target of an io.Copy
// is an *os.File (not a pipe or socket)
type SparseDecoder struct {
	reader io.Reader

	fileSize      int64
	currentOffset int64

	currentSection       io.Reader
	currentSectionLength int64
	currentSectionRead   int

	done bool
}

// Read is the slow path of the decoder. It output the entire sparse file.
func (s *SparseDecoder) Read(p []byte) (int, error) {
	var err error
	if s.currentSection == nil {
		s.fileSize, err = getFileSize(s.reader)
		if err != nil {
			return 0, fmt.Errorf("error determining target file size: %w", err)
		}

		err = s.parseSection()
		if err != nil {
			return 0, err
		}
	}

	read, err := s.currentSection.Read(p)
	s.currentSectionRead += read
	s.currentOffset += int64(read)

	if err == nil {
		return read, nil
	}
	if !errors.Is(err, io.EOF) {
		return read, err
	}

	// current section has ended. Was it expected?
	if s.currentSectionLength != int64(s.currentSectionRead) {
		return read, fmt.Errorf("read size doesn't equal section size. %d vs %d. %w", s.currentSectionRead, s.currentSectionLength, io.ErrUnexpectedEOF)
	}

	// EOF was expected. Are there more sections?
	if s.done {
		return read, err
	}

	// there are more sections to read. Reset counter and get next section
	s.currentSectionRead = 0

	// get next section
	err = s.parseSection()
	return read, err
}

func (s *SparseDecoder) parseSection() error {
	// use 8 + 8 here as that is the maximum buffer size we need for parsing getting
	// the data size. The two int64 for writing data sections.
	var segmentHeader [8 + 8]byte

	// first byte contains the segment type
	_, err := io.ReadFull(s.reader, segmentHeader[0:1])
	if err != nil {
		return fmt.Errorf("error reading segmentHeader header: %w", err)
	}

	switch segmentHeader[0] {
	case endIndicator:
		s.currentSectionLength = s.fileSize - s.currentOffset
		s.currentSection = io.LimitReader(zeroReader{}, s.currentSectionLength)
		s.done = true
	case dataIndicator:
		_, err = io.ReadFull(s.reader, segmentHeader[:])
		if err != nil {
			return fmt.Errorf("error reading data header: %w", err)
		}

		offset := binary.LittleEndian.Uint64(segmentHeader[:9])
		length := int64(binary.LittleEndian.Uint64(segmentHeader[8:]))
		padding := int64(offset) - s.currentOffset

		s.currentSectionLength = padding + length

		paddingReader := io.LimitReader(zeroReader{}, padding)
		dataReader := io.LimitReader(s.reader, length)

		s.currentSection = io.MultiReader(paddingReader, dataReader)
	default:
		return fmt.Errorf(`invalid section type: "%d:" %x`, segmentHeader[0], segmentHeader[0])
	}

	return nil
}

// WriteTo is the fast path optimisation of SparseDecoder.Read. If the target of io.Copy is an *os.File that is
// capable of seeking WriteTo will be used. It preserves the sparseness of the target file and does not need
// to write the entire file. Only section of the file containing data will be written.
func (s *SparseDecoder) WriteTo(writer io.Writer) (int64, error) {
	file, isFile := s.isSeekableFile(writer)
	if !isFile {
		return io.Copy(writer, onlyReader{s})
	}

	size, err := getFileSize(s.reader)

	if err != nil {
		return 0, fmt.Errorf("error determining target file size: %w", err)
	}

	err = SparseTruncate(file, size)
	if err != nil {
		return 0, fmt.Errorf("error truncating target file: %w", err)
	}

	// use 8 + 8 here as that is the maximum buffer size we need for parsing getting
	// the data size. The two int64 for writing data sections.
	var segmentHeader [8 + 8]byte
	var written int64 = 0

	for {
		// first byte contains the segment type
		_, err := io.ReadFull(s.reader, segmentHeader[0:1])
		if err != nil {
			return written, fmt.Errorf("error reading segmentHeader header: %w", err)
		}

		switch segmentHeader[0] {
		case endIndicator:
			return written, err
		case dataIndicator:
			_, err = io.ReadFull(s.reader, segmentHeader[:])
			if err != nil {
				return written, fmt.Errorf("error reading data header: %w", err)
			}

			offset := binary.LittleEndian.Uint64(segmentHeader[:9])
			length := binary.LittleEndian.Uint64(segmentHeader[8:])

			_, err = file.Seek(int64(offset), io.SeekStart)
			if err != nil {
				return written, fmt.Errorf("error seeking to start of data section: %w", err)
			}

			copied, err := io.Copy(writer, io.LimitReader(s.reader, int64(length)))
			written += copied
			if err != nil {
				return written, fmt.Errorf("error copying data: %w", err)
			}
		default:
			return written, fmt.Errorf("invalid section type: %b", segmentHeader[0])
		}
	}
}

func (s *SparseDecoder) isSeekableFile(writer io.Writer) (*os.File, bool) {
	file, isFile := writer.(*os.File)
	if isFile {
		// not all files are actually seekable. pipes aren't for example
		_, err := file.Seek(0, io.SeekCurrent)
		return file, err == nil
	}
	return nil, false
}

func NewEncoder(file *os.File) *Encoder {
	return &Encoder{file: file}
}

// Encoder encodes a file to a stream of sparsecat data.
type Encoder struct {
	file     *os.File
	fileSize int64

	currentOffset        int64
	currentSection       io.Reader
	currentSectionLength int64
	currentSectionEnd    int64
	currentSectionRead   int

	done bool
}

func (e *Encoder) Read(p []byte) (int, error) {
	if e.currentSection == nil {
		info, err := e.file.Stat()
		if err != nil {
			return 0, fmt.Errorf("error running stat: %w", err)
		}

		buf := make([]byte, 9)
		buf[0] = sizeIndicator
		binary.LittleEndian.PutUint64(buf[1:], uint64(info.Size()))
		e.currentSection = bytes.NewReader(buf)
		e.currentSectionLength = 9
	}

	read, err := e.currentSection.Read(p)
	e.currentSectionRead += read

	if err == nil {
		return read, err
	}

	if !errors.Is(err, io.EOF) {
		return read, err
	}

	// current section has ended. Was it expected?
	if e.currentSectionLength != int64(e.currentSectionRead) {
		return read, fmt.Errorf("read size doesn't equal section size. %d vs %d. %w", e.currentSectionRead, e.currentSectionLength, io.ErrUnexpectedEOF)
	}

	// are there more sections to come?
	if e.done {
		return read, io.EOF
	}

	e.currentOffset += e.currentSectionEnd
	e.currentSectionRead = 0

	err = e.parseSection()
	if err != nil {
		return read, err
	}

	return 0, nil

}

func (e *Encoder) parseSection() error {
	start, end, err := detectDataSection(e.file, e.currentOffset)
	if errors.Is(err, io.EOF) {
		e.currentSection = bytes.NewReader([]byte{endIndicator})
		e.currentSectionLength = 1
		e.done = true
		return nil
	}

	if err != nil {
		return fmt.Errorf("error detecting data section: %w", err)
	}

	// char + int64 + int64
	const headerSize = 1 + 8 + 8

	length := end - start
	e.currentSectionLength = length + headerSize // + 15 because of the header size
	e.currentSectionEnd = end

	_, err = e.file.Seek(start, io.SeekStart)
	if err != nil {
		return err
	}

	buf := make([]byte, headerSize)
	buf[0] = dataIndicator

	binary.LittleEndian.PutUint64(buf[1:], uint64(start))
	binary.LittleEndian.PutUint64(buf[1+8:], uint64(length))

	headerReader := bytes.NewReader(buf[:])
	fileReader := io.LimitReader(e.file, length)

	e.currentSection = io.MultiReader(headerReader, fileReader)

	return nil
}
