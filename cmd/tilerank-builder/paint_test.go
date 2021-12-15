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
	path := filepath.Join(t.TempDir(), "zurich.tif")
	if err := paint(path, 9, readers, context.Background()); err != nil {
		t.Fatal(err)
	}
}

// Make sure we can handle view counts at deep zoom levels even if not all
// parent tiles have been viewed.
func TestPaint_ParentNotLogged(t *testing.T) {
	readers := []io.Reader{strings.NewReader("3/1/1 3\n18/137341/91897 1\n")}
	path := filepath.Join(t.TempDir(), "notlogged.tif")
	if err := paint(path, 11, readers, context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestPaint_TooManyCountsForSameTile(t *testing.T) {
	readers := []io.Reader{
		// TODO: Uncomment once k-way merging is implemented.
		//strings.NewReader("4/4/10 3\n7/39/87 11\n"),
		strings.NewReader("4/2/1 2\n7/39/87 22\n7/39/87 33\n7/39/87 44\n"),
	}
	path := filepath.Join(t.TempDir(), "toomanycounts.tif")
	var got string
	if err := paint(path, 16, readers, context.Background()); err != nil {
		got = err.Error()
	}
	want := "tile 7/39/87 appears more than 1 times in input"
	if got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
}
