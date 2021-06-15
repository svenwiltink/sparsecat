package main

import (
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

		sparseReader := sparsecat.ToNormalReader(request.Body)
		defer sparseReader.Close()

		target, err := os.Create("based.raw")
		if err != nil {
			panic(err)
		}

		_, err = io.Copy(target, sparseReader)
		if err != nil {
			panic(err)
		}
	})

	err := http.ListenAndServe("localhost:6969", nil)
	if err != nil {
		panic(err)
	}
}
