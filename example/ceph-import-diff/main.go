package main

import (
	"encoding/binary"
	"github.com/svenwiltink/sparsecat"
	"github.com/svenwiltink/sparsecat/format"
	"io"
	"log"
	"os"
)

const (
	RBDImageHeader = "rbd image v2\n"
	RBDImageEndTag = "E"

	RBDImageDiffsV2Header = "rbd image diffs v2\n"
	RBDImageDiffV2Header  = "rbd diff v2\n"
)

func main() {
	log.SetFlags(log.Llongfile)
	if len(os.Args) != 2 {
		log.Fatalln(os.Args[0], " <file to import>")
	}

	file, err := os.Open(os.Args[1])
	if err != nil {
		panic(err)
	}
	defer file.Close()

	_, err = os.Stdout.WriteString(RBDImageHeader)
	if err != nil {
		panic(err)
	}

	_, err = os.Stdout.WriteString(RBDImageEndTag)
	if err != nil {
		panic(err)
	}

	_, err = os.Stdout.WriteString(RBDImageDiffsV2Header)
	if err != nil {
		panic(err)
	}

	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], 1)

	_, err = os.Stdout.Write(buf[:])
	if err != nil {
		panic(err)
	}

	_, err = os.Stdout.WriteString(RBDImageDiffV2Header)
	if err != nil {
		panic(err)
	}

	encoder := sparsecat.NewEncoder(file)
	encoder.Format = format.RbdDiffv2
	encoder.MaxSectionSize = 16_000_000

	_, err = io.Copy(os.Stdout, encoder)
	if err != nil {
		panic(err)
	}
}
