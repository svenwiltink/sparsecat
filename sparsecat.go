package main

import (
	"flag"
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

	SeekThroughFile(file)
}
