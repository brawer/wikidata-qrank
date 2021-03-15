// SPDX-License-Identifier: MIT

package main

import (
	"compress/gzip"
	"io"
	"os"

	"github.com/andybalholm/brotli"
)

func readBrotliFile(path string) string {
	f, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	reader := brotli.NewReader(f)
	b, err := io.ReadAll(reader)
	if err != nil {
		panic(err)
	}

	return string(b)
}

func readGzipFile(path string) string {
	f, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	reader, err := gzip.NewReader(f)
	if err != nil {
		panic(err)
	}

	b, err := io.ReadAll(reader)
	if err != nil {
		panic(err)
	}

	return string(b)
}

func writeBrotli(path string, content string) {
	f, err := os.Create(path)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	s := brotli.NewWriterLevel(f, 1)
	s.Write([]byte(content))
	if err := s.Close(); err != nil {
		panic(err)
	}
	if err := f.Close(); err != nil {
		panic(err)
	}
}

func writeGzipFile(path string, content string) {
	f, err := os.Create(path)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	s, err := gzip.NewWriterLevel(f, 1)
	if err != nil {
		panic(err)
	}
	s.Write([]byte(content))
	if err := s.Close(); err != nil {
		panic(err)
	}
	if err := f.Close(); err != nil {
		panic(err)
	}
}
