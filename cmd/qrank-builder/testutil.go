// SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"compress/gzip"
	"os"
)

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
