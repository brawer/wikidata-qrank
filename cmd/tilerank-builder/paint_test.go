// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/andybalholm/brotli"
)

func TestPaint(t *testing.T) {
	file, err := os.Open(filepath.Join("testdata", "zurich-2021-W47.br"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	readers := []io.Reader{brotli.NewReader(file)}
	if err := paint("", 17, readers, context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestPaint_TooManyCountsForSameTile(t *testing.T) {
	ctx := context.Background()
	readers := []io.Reader{
		// TODO: Uncomment once k-way merging is implemented.
		//strings.NewReader("4/4/10 3\n7/39/87 11\n"),
		strings.NewReader("4/2/1 2\n7/39/87 22\n7/39/87 33\n7/39/87 44\n"),
	}
	var got string
	if err := paint("", 16, readers, ctx); err != nil {
		got = err.Error()
	}
	want := "tile 7/39/87 appears more than 1 times in input"
	if got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
}
