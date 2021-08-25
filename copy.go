package sparsecat

import (
	"errors"
	"fmt"
	"github.com/svenwiltink/sparsecat/format"
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

func NewDecoder(reader io.Reader) *Decoder {
	return &Decoder{reader: reader, Format: format.RbdDiffv1}
}

// Decoder decodes an incoming sparsecat stream. It is able to convert it to a 'normal'
// stream of data using the WriteTo method. An optimized path is used  when the target of an io.Copy
// is an *os.File (not a pipe or socket)
type Decoder struct {
	Format format.Format
	reader io.Reader

	fileSize      int64
	currentOffset int64

	currentSection       io.Reader
	currentSectionLength int64
	currentSectionRead   int

	done bool
}

// Read is the slow path of the decoder. It output the entire sparse file.
func (s *Decoder) Read(p []byte) (int, error) {
	var err error
	if s.currentSection == nil {
		s.fileSize, err = s.Format.ReadFileSize(s.reader)
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

func (s *Decoder) parseSection() error {
	// use 8 + 8 here as that is the maximum buffer size we need for parsing getting
	// the data size. The two int64 for writing data sections.
	var segmentHeader [8 + 8]byte

	// first byte contains the segment type
	_, err := io.ReadFull(s.reader, segmentHeader[0:1])
	if err != nil {
		return fmt.Errorf("error reading segmentHeader header: %w", err)
	}

	section, err := s.Format.ReadSectionHeader(s.reader)
	if err == io.EOF {
		s.currentSectionLength = s.fileSize - s.currentOffset
		s.currentSection = io.LimitReader(zeroReader{}, s.currentSectionLength)
		s.done = true
		return nil
	}

	if err != nil {
		return err
	}

	padding := section.Offset - s.currentOffset
	s.currentSectionLength = padding + section.Length

	paddingReader := io.LimitReader(zeroReader{}, padding)
	dataReader := io.LimitReader(s.reader, section.Length)
	s.currentSection = io.MultiReader(paddingReader, dataReader)

	return nil
}

// WriteTo is the fast path optimisation of Decoder.Read. If the target of io.Copy is an *os.File that is
// capable of seeking WriteTo will be used. It preserves the sparseness of the target file and does not need
// to write the entire file. Only section of the file containing data will be written.
func (s *Decoder) WriteTo(writer io.Writer) (int64, error) {
	file, isFile := s.isSeekableFile(writer)
	if !isFile {
		return io.Copy(writer, onlyReader{s})
	}

	size, err := s.Format.ReadFileSize(s.reader)

	if err != nil {
		return 0, fmt.Errorf("error determining target file size: %w", err)
	}

	err = SparseTruncate(file, size)
	if err != nil {
		return 0, fmt.Errorf("error truncating target file: %w", err)
	}

	var written int64 = 0

	for {
		section, err := s.Format.ReadSectionHeader(s.reader)
		if errors.Is(err, io.EOF) {
			return written, err
		}

		if err != nil {
			return written, err
		}

		_, err = file.Seek(section.Offset, io.SeekStart)
		if err != nil {
			return written, fmt.Errorf("error seeking to start of data section: %w", err)
		}

		copied, err := io.Copy(writer, io.LimitReader(s.reader, section.Length))
		written += copied
		if err != nil {
			return written, fmt.Errorf("error copying data: %w", err)
		}
	}
}

func (s *Decoder) isSeekableFile(writer io.Writer) (*os.File, bool) {
	file, isFile := writer.(*os.File)
	if isFile {
		// not all files are actually seekable. pipes aren't for example
		_, err := file.Seek(0, io.SeekCurrent)
		return file, err == nil
	}
	return nil, false
}

func NewEncoder(file *os.File) *Encoder {
	return &Encoder{file: file, Format: format.RbdDiffv1}
}

// Encoder encodes a file to a stream of sparsecat data.
type Encoder struct {
	file     *os.File
	Format   format.Format
	fileSize int64

	currentOffset        int64
	currentSection       io.Reader
	currentSectionLength int64
	currentSectionEnd    int64
	currentSectionRead   int

	isBlockDevice bool

	done bool
}

func (e *Encoder) Read(p []byte) (int, error) {
	if e.currentSection == nil {
		info, err := e.file.Stat()
		if err != nil {
			return 0, fmt.Errorf("error running stat: %w", err)
		}

		size := uint64(info.Size())
		if isBlockDevice(info) {
			e.isBlockDevice = true

			bsize, err := getBlockDeviceSize(e.file)
			if err != nil {
				return 0, fmt.Errorf("error determining size of block device: %w", err)
			}

			size = uint64(int64(bsize))
		}

		e.currentSection, e.currentSectionLength = e.Format.GetFileSizeReader(size)
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

	e.currentOffset = e.currentSectionEnd
	e.currentSectionRead = 0

	err = e.parseSection()
	return read, err
}

func (e *Encoder) parseSection() error {
	if e.isBlockDevice {
		fmt.Println("reading block device")
		start, end, reader, err := slowDetectDataSection(e.file, e.currentOffset)
		if errors.Is(err, io.EOF) {
			e.currentSection, e.currentSectionLength = e.Format.GetEndTagReader()
			e.done = true
			return nil
		}

		if err != nil {
			return fmt.Errorf("error detecting data section for block device: %w", err)
		}

		length := end - start
		e.currentSectionEnd = end

		e.currentSection, e.currentSectionLength = e.Format.GetSectionReader(reader, format.Section{
			Offset: start,
			Length: length,
		})

		return nil
	}

	start, end, err := detectDataSection(e.file, e.currentOffset)
	if errors.Is(err, io.EOF) {
		e.currentSection, e.currentSectionLength = e.Format.GetEndTagReader()
		e.done = true
		return nil
	}

	if err != nil {
		return fmt.Errorf("error detecting data section: %w", err)
	}

	length := end - start
	e.currentSectionEnd = end

	_, err = e.file.Seek(start, io.SeekStart)
	if err != nil {
		return err
	}

	e.currentSection, e.currentSectionLength = e.Format.GetSectionReader(e.file, format.Section{
		Offset: start,
		Length: length,
	})

	return nil
}
