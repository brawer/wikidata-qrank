// SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"bufio"
	"bytes"
	"errors"
	"log"
	"strings"
	"testing"
)

func TestLineMerger(t *testing.T) {
	logger = log.New(&bytes.Buffer{}, "", log.Lshortfile)
	m := NewLineMerger([]LineScanner{
		bufio.NewScanner(strings.NewReader("C1\nD1")),
		bufio.NewScanner(strings.NewReader("B2\nE2")),
		bufio.NewScanner(strings.NewReader("A3\nB3")),
		bufio.NewScanner(strings.NewReader("")),
		bufio.NewScanner(strings.NewReader("B5")),
	}, []string{"S1", "S2", "S3", "S4", "S5"})
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

type errReader struct {
	numReads int
	maxReads int
}

var testErr = errors.New("test error")

func (e *errReader) Read(p []byte) (n int, err error) {
	if e.numReads >= e.maxReads {
		return 0, testErr
	}
	e.numReads += 1
	p[0] = '.'
	p[1] = '\n'
	return 2, nil
}

func TestLineMerger_ErrorAtStart(t *testing.T) {
	var logfile bytes.Buffer
	logger = log.New(&logfile, "", log.Lshortfile)
	reader := &errReader{numReads: 0, maxReads: 0}
	m := NewLineMerger([]LineScanner{bufio.NewScanner(reader)}, []string{"🐞"})
	if m.Advance() {
		t.Error("expected m.Advance()=false, got true")
		return
	}
	if err := m.Err(); err != testErr {
		t.Errorf("expected test error, got %q", err)
	}
	gotLog := string(logfile.Bytes())
	if !strings.Contains(gotLog, `scanner "🐞" failed to scan first line, err=test error`) {
		t.Errorf("name of failing input scanner should be logged, got %s", gotLog)
	}
}

func TestLineMerger_ErrorAtRead(t *testing.T) {
	var logfile bytes.Buffer
	logger = log.New(&logfile, "", log.Lshortfile)
	reader := &errReader{numReads: 0, maxReads: 1}
	m := NewLineMerger([]LineScanner{bufio.NewScanner(reader)}, []string{"🐞"})
	if !m.Advance() {
		t.Error("expected first m.Advance()=true, got false")
		return
	}
	if m.Advance() {
		t.Error("expected second m.Advance()=false, got true")
		return
	}
	if err := m.Err(); err != testErr {
		t.Errorf("expected test error, got %q", err)
	}
	gotLog := string(logfile.Bytes())
	if !strings.Contains(gotLog, `scanner "🐞" failed, err=test error`) {
		t.Errorf("name of failing input scanner should be logged, got %s", gotLog)
	}
}
