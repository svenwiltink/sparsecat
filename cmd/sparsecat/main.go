package main

import (
	"flag"
	"github.com/svenwiltink/sparsecat"
	"github.com/svenwiltink/sparsecat/format"
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
	formatName := flag.String("format", "rbd-diff-v1", "the wire format to use. Currently either rbd-diff-v1 or rbd-diff-v2")
	receive := flag.Bool("r", false, "receive a file instead of transmitting")
	disableSparseTarget := flag.Bool("disable-sparse-target", false, "disable sparse writing the target file")

	flag.Parse()

	log.SetFlags(0)

	f, exists := format.GetByName(*formatName)
	if !exists {
		log.Fatalf("Format %s doesn't exist", *formatName)
	}

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
		encoder := sparsecat.NewEncoder(inputFile)
		encoder.Format = f
		_, err := io.Copy(outputFile, encoder)
		if err != nil {
			log.Fatal(err)
		}
		return
	}

	decoder := sparsecat.NewDecoder(inputFile)
	decoder.Format = f
	decoder.DisableSparseWriting = *disableSparseTarget

	_, err := io.Copy(outputFile, decoder)
	if err != nil {
		log.Fatal(err)
	}
}

func setupFiles(operation OperationType, inputFileName string, outputFileName string) (*os.File, *os.File) {
	if inputFileName == "" {
		flag.Usage()
		os.Exit(1)
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
	} else {
		outputFile, err = os.Create(outputFileName)
		if err != nil {
			log.Fatalf("unable to create outputFile: %s", err)
		}
	}

	return inputFile, outputFile
}
