// SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"errors"
	"io"
	"strings"
	"testing"
)

func TestLineMerger(t *testing.T) {
	m := NewLineMerger([]io.Reader{
		strings.NewReader("C1\nD1"),
		strings.NewReader("B2\nE2"),
		strings.NewReader("A3\nB3"),
		strings.NewReader(""),
		strings.NewReader("B5"),
	})
	result := make([]string, 0, 5)
	for m.Advance() {
		result = append(result, m.Line())
	}
	if err := m.Err(); err != nil {
		t.Error(err)
		return
	}
	got := strings.Join(result, "|")
	expected := "A3|B2|B3|B5|C1|D1|E2"
	if expected != got {
		t.Errorf("expected %q, got %q", expected, got)
	}
}

type errReader struct{}

var testErr = errors.New("test error")

func (e *errReader) Read(p []byte) (n int, err error) {
	return 0, testErr
}

func TestLineMergerError(t *testing.T) {
	m := NewLineMerger([]io.Reader{&errReader{}})
	if m.Advance() {
		t.Error("expected m.Advance()=false, got true")
		return
	}
	if err := m.Err(); err != testErr {
		t.Errorf("expected test error, got %q", err)
	}
}
