// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestBuildQRank(t *testing.T) {
	qviews := filepath.Join(t.TempDir(), "TestQRank-qviews.br")
	writeBrotli(qviews, "Q1 1\nQ2 42\nQ3 1\nQ4 77\nQ5 42\n")

	path, err := buildQRank(time.Now(), qviews, t.TempDir(), context.Background())
	if err != nil {
		t.Error(err)
		return
	}

	expected := "Q4\t77\nQ2\t42\nQ5\t42\nQ1\t1\nQ3\t1\n"
	got := readGzipFile(path)
	if expected != got {
		t.Errorf("expected %q, got %q", expected, got)
	}
}
