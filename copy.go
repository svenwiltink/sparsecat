package sparsecat

import (
	"errors"
	"fmt"
	"github.com/svenwiltink/sparsecat/format"
	"io"
	"os"
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
	DisableSparseWriting bool

	reader io.Reader

	fileSize      int64
	currentOffset int64

	currentSection       io.Reader
	currentSectionLength int64
	currentSectionRead   int

	done bool
}

// Read is the slow path of the decoder. It output the entire sparse file.
func (d *Decoder) Read(p []byte) (int, error) {
	var err error
	if d.currentSection == nil {
		d.fileSize, err = d.Format.ReadFileSize(d.reader)
		if err != nil {
			return 0, fmt.Errorf("error determining target file size: %w", err)
		}

		err = d.parseSection()
		if err != nil {
			return 0, fmt.Errorf("error reading first section: %w", err)
		}
	}

	read, err := d.currentSection.Read(p)
	d.currentSectionRead += read
	d.currentOffset += int64(read)

	if err == nil {
		return read, nil
	}
	if !errors.Is(err, io.EOF) {
		return read, err
	}

	// current section has ended. Was it expected?
	if d.currentSectionLength != int64(d.currentSectionRead) {
		return read, fmt.Errorf("read size doesn't equal section size. %d vs %d. %w", d.currentSectionRead, d.currentSectionLength, io.ErrUnexpectedEOF)
	}

	// EOF was expected. Are there more sections?
	if d.done {
		return read, err
	}

	// there are more sections to read. Reset counter and get next section
	d.currentSectionRead = 0

	// get next section
	err = d.parseSection()
	return read, err
}

func (d *Decoder) parseSection() error {
	section, err := d.Format.ReadSectionHeader(d.reader)
	if errors.Is(err, io.EOF) {
		d.currentSectionLength = d.fileSize - d.currentOffset
		d.currentSection = io.LimitReader(zeroReader{}, d.currentSectionLength)
		d.done = true
		return nil
	}

	if err != nil {
		return err
	}

	padding := section.Offset - d.currentOffset
	d.currentSectionLength = padding + section.Length

	paddingReader := io.LimitReader(zeroReader{}, padding)
	dataReader := io.LimitReader(d.reader, section.Length)
	d.currentSection = io.MultiReader(paddingReader, dataReader)

	return nil
}

// WriteTo is the fast path optimisation of Decoder.Read. If the target of io.Copy is an *os.File that is
// capable of seeking WriteTo will be used. It preserves the sparseness of the target file and does not need
// to write the entire file. Only section of the file containing data will be written. When s.DisableSparseWriting
// has been set this falls back to io.Copy with only the s.Read function exposed
func (d *Decoder) WriteTo(writer io.Writer) (int64, error) {
	if d.DisableSparseWriting {
		return io.Copy(writer, onlyReader{d})
	}

	file, isFile := d.isSeekableFile(writer)
	if !isFile {
		return io.Copy(writer, onlyReader{d})
	}

	size, err := d.Format.ReadFileSize(d.reader)

	if err != nil {
		return 0, fmt.Errorf("error determining target file size: %w", err)
	}

	err = SparseTruncate(file, size)
	if err != nil {
		return 0, fmt.Errorf("error truncating target file: %w", err)
	}

	var written int64 = 0

	for {
		section, err := d.Format.ReadSectionHeader(d.reader)
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

		copied, err := io.Copy(writer, io.LimitReader(d.reader, section.Length))
		written += copied
		if err != nil {
			return written, fmt.Errorf("error copying data: %w", err)
		}
	}
}

func (d *Decoder) isSeekableFile(writer io.Writer) (*os.File, bool) {
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

	supportsHoleDetection bool

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
			e.supportsHoleDetection = false
			bsize, err := getBlockDeviceSize(e.file)
			if err != nil {
				return 0, fmt.Errorf("error determining size of block device: %w", err)
			}

			size = uint64(bsize)
		} else {
			e.supportsHoleDetection = supportsSeekHole(e.file)
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
	if !e.supportsHoleDetection {
		return e.slowDetectSection()
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

func (e *Encoder) slowDetectSection() error {
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
