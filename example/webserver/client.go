package main

import (
	"github.com/svenwiltink/sparsecat"
	"net/http"
	"net/http/httputil"
	"os"
)

func main() {
	source, err := os.Open("image.raw")
	if err != nil {
		panic(err)
	}
	defer source.Close()

	sparseEncoder := sparsecat.NewEncoder(source)

	resp, err := http.Post("http://localhost:6969/store", "application/octet-stream", sparseEncoder)
	if err != nil {
		panic(err)
	}

	httputil.DumpResponse(resp, false)

	defer resp.Body.Close()
}
