package main

import (
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
)

type OperationType int

const (
	Send OperationType = iota
	Receive
)

func main() {
	inputFileName := flag.String("if", "", "input inputFile. '-' for stdin")
	outputFileName := flag.String("of", "", "output inputFile. '-' for stdout")
	receive := flag.Bool("r", false, "receive a file instead of transmitting")
	flag.Parse()

	log.SetFlags(0)

	operation := Send
	if *receive {
		operation = Receive
	}

	// apply defaults
	if operation == Send && *outputFileName == "" {
		*outputFileName = "-"
	}

	if operation == Receive && *inputFileName == "" {
		*inputFileName = "-"
	}

	inputFile, outputFile := setupFiles(operation, *inputFileName, *outputFileName)

	defer inputFile.Close()
	defer outputFile.Close()

	if operation == Send {
		sendSparseFile(inputFile, outputFile)
		return
	}

	receiveSparseFile(inputFile, outputFile)
}

func receiveSparseFile(input io.Reader, output *os.File) {
	size, err := getFileSize(input)
	if err != nil {
		log.Fatal(err)
	}

	err = output.Truncate(size)
	if err != nil {
		log.Fatal("error resizing target file: ", err)
	}

	for {
		// use 8 + 8 here as that is the maximum buffer size we need for parsing getting
		//the data size
		var segment [8+8]byte
		_, err := io.ReadFull(input, segment[0:1])
		if err != nil {
			log.Fatal("error reading segment header: ", err)
		}

		if segment[0] == 'e' {
			return
		}

		_, err = io.ReadFull(input, segment[:])
		if err != nil {
			log.Fatal("error reading data header: ", err)
		}

		offset := binary.LittleEndian.Uint64(segment[:9])
		length := binary.LittleEndian.Uint64(segment[8:])

		_, err = output.Seek(int64(offset), io.SeekStart)
		if err != nil {
			log.Fatal("error seeking: ", err)
		}

		_, err = io.Copy(output, io.LimitReader(input, int64(length)))
		if err != nil {
			log.Fatal("error copying data: ", err)
		}
	}
}

func getFileSize(input io.Reader) (int64, error) {
	var header [9]byte
	_, err := io.ReadFull(input, header[:])
	if err != nil {
		return 0, err
	}

	if header[0] != 's' {
		return 0, fmt.Errorf("invalid header. Expected size segment but got %s", string(header[0]))
	}

	size := binary.LittleEndian.Uint64(header[1:])
	return int64(size), nil
}

func sendSparseFile(input *os.File, output io.Writer) {
	var offset int64 = 0

	info, err := input.Stat()
	if err != nil {
		log.Fatal(err)
	}

	err = writeSizeSection(output, info.Size())
	if err != nil {
		log.Fatalf("error writing size section: %s", err)
	}

	for {
		start, end, err := DetectDataSection(input, offset)
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			log.Fatal("error detecting data section ", err)
		}

		err = writeDataSection(output, input, start, end)
		if err != nil {
			log.Fatal("error writing data: ", err)
		}

		offset = end
	}

	err = writeEndSection(output)
	if err != nil {
		log.Fatal(err)
	}
}

func setupFiles(operation OperationType, inputFileName string, outputFileName string) (*os.File, *os.File) {
	if inputFileName == "" {
		flag.Usage()
		log.Fatal("input inputFile required")
	}

	var inputFile *os.File
	var outputFile *os.File
	var err error

	if inputFileName == "-" {
		if operation == Send {
			log.Fatal("input must be a file when sending data")
		}
		inputFile = os.Stdin
	} else {
		inputFile, err = os.Open(inputFileName)
		if err != nil {
			log.Fatalf("unable to open inputFile: %s", err)
		}
	}

	if outputFileName == "-" {
		outputFile = os.Stdout
		if operation == Receive {
			log.Fatal("input must be a file when receiving data")
		}
	} else {
		outputFile, err = os.Create(outputFileName)
		if err != nil {
			log.Fatalf("unable to create outputFile: %s", err)
		}
	}

	return inputFile, outputFile
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

	var buf [1 + 8 + 8]byte
	buf[0] = 'w'

	length := end - start

	binary.LittleEndian.PutUint64(buf[1:], uint64(start))
	binary.LittleEndian.PutUint64(buf[1+8:], uint64(length))

	_, err = w.Write(buf[:])
	if err != nil {
		return nil
	}

	_, err = io.Copy(w, io.LimitReader(r, length))
	return err
}
