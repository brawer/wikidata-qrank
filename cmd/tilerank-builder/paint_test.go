// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"io"
	"strings"
	"testing"
)

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
