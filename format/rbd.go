package format

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

const (
	sizeIndicator byte = 's'
	dataIndicator byte = 'w'
	endIndicator  byte = 'e'
)

// RbdDiffv1 implements the rbd diff v1 wire format as described by https://github.com/ceph/ceph/blob/master/doc/dev/rbd-diff.rst#header.
// Only the Size, UpdatedData and End sections are implemented. Zero data is simply not transmitted.
var RbdDiffv1 rbdDiffv1

type rbdDiffv1 struct{}

func (r rbdDiffv1) ReadFileSize(reader io.Reader) (int64, error) {
	// 1 byte for segment type. 8 bytes for int64
	var header [1 + 8]byte
	_, err := io.ReadFull(reader, header[:])
	if err != nil {
		return 0, err
	}

	if header[0] != sizeIndicator {
		return 0, fmt.Errorf("invalid header. Expected size segment but got %s", string(header[0]))
	}

	size := binary.LittleEndian.Uint64(header[1:])
	return int64(size), nil
}

func (r rbdDiffv1) ReadSectionHeader(reader io.Reader) (Section, error) {
	// use 8 + 8 here as that is the maximum buffer size we need for parsing getting
	// the data size. The two int64 for writing data sections.
	var segmentHeader [8 + 8]byte

	// first byte contains the segment type
	_, err := io.ReadFull(reader, segmentHeader[0:1])
	if err != nil {
		return Section{}, fmt.Errorf("error reading segmentHeader header: %w", err)
	}

	switch segmentHeader[0] {
	case endIndicator:
		return Section{}, io.EOF
	case dataIndicator:
		_, err = io.ReadFull(reader, segmentHeader[:])
		if err != nil {
			return Section{}, fmt.Errorf("error reading data header: %w", err)
		}

		offset := int64(binary.LittleEndian.Uint64(segmentHeader[:9]))
		length := int64(binary.LittleEndian.Uint64(segmentHeader[8:]))

		return Section{
			Offset: offset,
			Length: length,
		}, nil
	}

	return Section{}, fmt.Errorf(`invalid section type: "%d:" %x`, segmentHeader[0], segmentHeader[0])
}

func (r rbdDiffv1) GetFileSizeReader(size uint64) (reader io.Reader, length int64) {
	buf := make([]byte, 1+8)
	buf[0] = sizeIndicator
	binary.LittleEndian.PutUint64(buf[1:], size)
	return bytes.NewReader(buf), 1 + 8
}

func (r rbdDiffv1) GetSectionReader(source io.Reader, section Section) (reader io.Reader, length int64) {
	// char + int64 + int64
	const headerSize = 1 + 8 + 8

	buf := make([]byte, headerSize)
	buf[0] = dataIndicator

	binary.LittleEndian.PutUint64(buf[1:], uint64(section.Offset))
	binary.LittleEndian.PutUint64(buf[1+8:], uint64(section.Length))

	headerReader := bytes.NewReader(buf[:])
	fileReader := io.LimitReader(source, section.Length)

	return io.MultiReader(headerReader, fileReader), headerSize + section.Length
}

func (r rbdDiffv1) GetEndTagReader() (reader io.Reader, length int64) {
	return bytes.NewReader([]byte{endIndicator}), 1
}

// RbdDiffv2 implements the rbd diff v2 wire format as described by https://github.com/ceph/ceph/blob/master/doc/dev/rbd-diff.rst#header-1.
// Only the Size, UpdatedData and End sections are implemented. Zero data is simply not transmitted.
var RbdDiffv2 rbdDiffv2

type rbdDiffv2 struct{}

func (r rbdDiffv2) ReadFileSize(reader io.Reader) (int64, error) {
	// 1 byte for segment type. 8 bytes for int64
	var header [1 + 8 + 8]byte
	_, err := io.ReadFull(reader, header[:])
	if err != nil {
		return 0, err
	}

	if header[0] != sizeIndicator {
		return 0, fmt.Errorf("invalid header. Expected size segment but got %s", string(header[0]))
	}

	size := binary.LittleEndian.Uint64(header[9:])
	return int64(size), nil
}

func (r rbdDiffv2) ReadSectionHeader(reader io.Reader) (Section, error) {
	// use 8 + 8 + 8 here as that is the maximum buffer size we need for parsing getting
	// the data size. The three int64 for writing data sections.
	var segmentHeader [8 + 8 + 8]byte

	// first byte contains the segment type
	_, err := io.ReadFull(reader, segmentHeader[0:1])
	if err != nil {
		return Section{}, fmt.Errorf("error reading segmentHeader header: %w", err)
	}

	switch segmentHeader[0] {
	case endIndicator:
		return Section{}, io.EOF
	case dataIndicator:
		_, err = io.ReadFull(reader, segmentHeader[:])
		if err != nil {
			return Section{}, fmt.Errorf("error reading data header: %w", err)
		}

		// ignore the first int64 as we don't actually need that
		offset := int64(binary.LittleEndian.Uint64(segmentHeader[8:17]))
		length := int64(binary.LittleEndian.Uint64(segmentHeader[16:]))

		return Section{
			Offset: offset,
			Length: length,
		}, nil
	}

	return Section{}, fmt.Errorf(`invalid section type: "%d:" %x`, segmentHeader[0], segmentHeader[0])
}

func (r rbdDiffv2) GetFileSizeReader(size uint64) (reader io.Reader, length int64) {
	buf := make([]byte, 1+8+8)
	buf[0] = sizeIndicator
	binary.LittleEndian.PutUint64(buf[1:], 8)
	binary.LittleEndian.PutUint64(buf[1+8:], size)
	return bytes.NewReader(buf), 1 + 8 + 8
}

func (r rbdDiffv2) GetSectionReader(source io.Reader, section Section) (reader io.Reader, length int64) {
	// char + int64 + int64 + int64
	const headerSize = 1 + 8 + 8 + 8

	buf := make([]byte, headerSize)
	buf[0] = dataIndicator

	binary.LittleEndian.PutUint64(buf[1:], 16+uint64(section.Length))
	binary.LittleEndian.PutUint64(buf[1+8:], uint64(section.Offset))
	binary.LittleEndian.PutUint64(buf[1+8+8:], uint64(section.Length))

	headerReader := bytes.NewReader(buf[:])
	fileReader := io.LimitReader(source, section.Length)

	return io.MultiReader(headerReader, fileReader), headerSize + section.Length
}

func (r rbdDiffv2) GetEndTagReader() (reader io.Reader, length int64) {
	return bytes.NewReader([]byte{endIndicator}), 1
}
