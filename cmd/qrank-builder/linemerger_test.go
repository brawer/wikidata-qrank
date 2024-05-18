// SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"log"
	"strings"
	"testing"
)

func TestLineMerger(t *testing.T) {
	logger = log.New(&bytes.Buffer{}, "", log.Lshortfile)
	for tcIndex, tc := range []struct {
		inputs string
		want   string
	}{
		{"C1|D1 - B2|E2 A3|B3 B5", "A3|B2|B3|B5|C1|D1|E2"},

		// TODO: Add more test cases.
		//
		// In particular, add a test case that triggers a call
		// to LineMerger.Advance() where the current top of heap
		// is reaching the end of its stream.
		// https://github.com/brawer/wikidata-qrank/issues/40
	} {
		scanners := make([]LineScanner, 0, 10)
		names := make([]string, 0, 10)
		for i, input := range strings.Split(tc.inputs, " ") {
			lines := strings.Join(strings.Split(input, "|"), "\n") + "\n"
			if input == "-" {
				lines = ""
			}
			scanner := bufio.NewScanner(strings.NewReader(lines))
			scanners = append(scanners, scanner)
			names = append(names, fmt.Sprintf("S%d", i))
		}
		merger := NewLineMerger(scanners, names)
		result := make([]string, 0, 5)
		for merger.Advance() {
			result = append(result, merger.Line())
		}
		if err := merger.Err(); err != nil {
			t.Errorf("test case %d failed; err=%v", tcIndex, err)
		}
		got := strings.Join(result, "|")
		if got != tc.want {
			t.Errorf("test case %d: got %q, want %q", tcIndex, got, tc.want)
		}
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
