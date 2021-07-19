package format

import "io"

type Section struct {
	Offset, Length int64
}

type Format interface {
	ReadFileSize(reader io.Reader) (int64, error)
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
