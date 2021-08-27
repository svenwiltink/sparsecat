package format

import "io"

type Section struct {
	Offset, Length int64
}

// Format defines the wire format function. ReadFileSize and ReadSectionHeader are used
// for parsing incoming data whereas the GetFileSizeReader, GetSectionReader and GetEndTagReader
// functions are used to create readers that can be used by io.Copy. The length returned by these
// functions must be the amount of bytes the reader will return before reaching io.EOF.
type Format interface {
	// ReadFileSize reads the file size from an incoming stream
	ReadFileSize(reader io.Reader) (int64, error)
	// ReadSectionHeader reads a data section header from the incoming stream.
	// When the incoming stream receives an end tag an empty section must be returned
	// in combination with an io.EOF error.
	ReadSectionHeader(reader io.Reader) (Section, error)

	GetFileSizeReader(size uint64) (reader io.Reader, length int64)
	GetSectionReader(source io.Reader, section Section) (reader io.Reader, length int64)
	GetEndTagReader() (reader io.Reader, length int64)
}

var formats = map[string]Format{
	"rbd-diff-v1": RbdDiffv1,
	"rbd-diff-v2": RbdDiffv2,
}

func GetByName(name string) (format Format, exists bool) {
	format, exists = formats[name]
	return
}
