package main

import (
	"errors"
	"flag"
	"io"
	"log"
	"os"
	"syscall"
)

func main() {
	filename := flag.String("file", "test.raw", "file to cat")
	flag.Parse()

	file, err := os.Open(*filename)
	if err != nil {
		log.Fatalf("unable to open file: %s", err)
	}

	var offset int64 = 0
	var syserr syscall.Errno
	var totalbytes int64 = 0

	defer func() {
		log.Println("copied %d bytes total", totalbytes)
	}()

	for {
		startOfData, err := SeekData(file, offset)
		if errors.As(err, &syserr) {
			if syserr == syscall.ENXIO {
				log.Printf("seek to data returned ENXIO. End of file? Current offset: %d\n", offset)
				return
			}
			log.Fatalf("error seeking to data: %d, %s", syserr, syserr)
		} else if err != nil {
			log.Fatalf("error seeking to data: %s", err)
		}

		log.Printf("offset %d start of data\n", startOfData)

		endOfData, err := SeekHole(file, startOfData)
		if errors.As(err, &syserr) {
			if syserr == syscall.ENXIO {
				log.Printf("seek to hole returned ENXIO. End of file?\nCurrent offset: %d", offset)
				return
			}
			log.Fatalf("error seeking to hole: %d, %s", syserr, syserr)
		} else if err != nil {
			log.Fatalf("error seeking to hole: %s", err)
		}

		log.Printf("offset %d end of data\n", endOfData)

		secionSize := endOfData - startOfData
		log.Printf("section size: %d", secionSize)

		_, err = file.Seek(startOfData, io.SeekStart)
		if err != nil {
			log.Fatalf("error seeking to start of section")
		}

		sectionReader := io.LimitReader(file, secionSize)
		copied, err := io.Copy(os.Stdout, sectionReader)
		if err != nil {
			log.Fatalf("error copying data: %s", err)
		}

		totalbytes += copied

		log.Printf("copied %d bytes", copied)

		offset = endOfData
	}
}
