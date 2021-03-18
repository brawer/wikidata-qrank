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

	expected := "Entity,QRank\n" +
		"Q4,77\n" +
		"Q2,42\n" +
		"Q5,42\n" +
		"Q1,1\n" +
		"Q3,1\n"
	got := readGzipFile(path)
	if expected != got {
		t.Errorf("expected %q, got %q", expected, got)
	}
}
