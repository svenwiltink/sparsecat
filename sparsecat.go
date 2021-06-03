package main

import (
	"encoding/binary"
	"errors"
	"flag"
	"io"
	"log"
	"os"
)

func main() {
	filename := flag.String("file", "test.raw", "file to cat")
	flag.Parse()

	file, err := os.Open(*filename)
	if err != nil {
		log.Fatalf("unable to open file: %s", err)
	}

	var offset int64 = 0

	info, err := file.Stat()
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("file size: %d", info.Size())

	err = writeSizeSection(os.Stdout, info.Size())
	if err != nil {
		log.Fatal(err)
	}

	for {
		start, end, err := DetectDataSection(file, offset)
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			log.Fatal(err)
		}

		err = writeDataSection(os.Stdout, file, start, end)
		if err != nil {
			log.Fatal(err)
		}

		offset = end
	}

	err = writeEndSection(os.Stdout)
	if err != nil {
		log.Fatal(err)
	}
}

func writeEndSection(w io.Writer) error {
	_, err := w.Write([]byte("e"))
	return err
}

func writeSizeSection(w io.Writer, size int64) error {
	buf := make([]byte, 9)
	buf[0] = 's'

	binary.LittleEndian.PutUint64(buf[1:], uint64(size))

	_, err := w.Write(buf)
	return err
}

func writeDataSection(w io.Writer, r io.ReadSeeker, start int64, end int64) error {
	_, err := r.Seek(start, io.SeekStart)
	if err != nil {
		return err
	}

	buf := make([]byte, 1+8+8)
	buf[0] = 'w'

	length := end - start

	binary.LittleEndian.PutUint64(buf[1:], uint64(start))
	binary.LittleEndian.PutUint64(buf[1+8:], uint64(length))

	_, err = w.Write(buf)
	if err != nil {
		return nil
	}

	_, err = io.Copy(w, io.LimitReader(r, length))
	return err
}
