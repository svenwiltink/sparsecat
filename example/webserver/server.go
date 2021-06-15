package main

import (
	"compress/gzip"
	"github.com/svenwiltink/sparsecat"
	"io"
	"net/http"
	"os"
)

func main() {
	http.HandleFunc("/store", func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost {
			http.NotFound(writer, request)
			return
		}

		sparseReader := sparsecat.NewDecoder(request.Body)
		target, err := os.Create("based.raw")
		if err != nil {
			panic(err)
		}

		_, err = io.Copy(target, sparseReader)
		if err != nil {
			panic(err)
		}
	})

	http.HandleFunc("/store-zipped", func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost {
			http.NotFound(writer, request)
			return
		}

		sparseReader := sparsecat.NewDecoder(request.Body)
		target, err := os.Create("based.raw.gz")
		if err != nil {
			panic(err)
		}

		zw := gzip.NewWriter(target)
		_, err = io.Copy(zw, sparseReader)
		zw.Close()

		if err != nil {
			panic(err)
		}
	})

	err := http.ListenAndServe("localhost:6969", nil)
	if err != nil {
		panic(err)
	}
}
