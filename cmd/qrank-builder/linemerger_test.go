// SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"strings"
	"testing"
)

type testLineReader struct {
	lines []string
}

func (r *testLineReader) Read(p []byte) (n int, err error) {
	if len(r.lines) == 0 {
		return 0, io.EOF
	}
	line := r.lines[0]
	if line == "<err>" {
		return 0, fmt.Errorf("test error")
	}
	// Technically we should handle len(p) < len(line),
	// but this does not happen during unit tests because
	// our test strings are so small.
	for i := 0; i < len(line); i++ {
		p[i] = line[i]
	}
	p[len(line)] = '\n'
	r.lines = r.lines[1:len(r.lines)]
	return len(line) + 1, nil
}

func TestLineMerger(t *testing.T) {
	logger = log.New(&bytes.Buffer{}, "", log.Lshortfile)
	for tcIndex, tc := range []struct {
		inputs string
		want   string
	}{
		{"C1|D1 <empty> B2|E2 A3|B3 B5", "A3|B2|B3|B5|C1|D1|E2"},

		{"A B <err>", "<err>"},   // error at start
		{"A|<err> B", "A|<err>"}, // error not at start

		// Trigger calls to LineMerger.Advance() where the current
		// top of heap is reaching the end of its input stream.
		{"A A|A", "A|A|A"},
		{"C1|C2|C3 B1|B2|B3 A1|A2", "A1|A2|B1|B2|B3|C1|C2|C3"},
	} {
		scanners := make([]LineScanner, 0, 10)
		names := make([]string, 0, 10)
		for i, input := range strings.Split(tc.inputs, " ") {
			lines := strings.Split(input, "|")
			if input == "<empty>" {
				lines = []string{}
			}
			scanner := bufio.NewScanner(&testLineReader{lines: lines})
			scanners = append(scanners, scanner)
			names = append(names, fmt.Sprintf("S%d", i))
		}
		merger := NewLineMerger(scanners, names)
		result := make([]string, 0, 5)
		for merger.Advance() {
			result = append(result, merger.Line())
		}
		if err := merger.Err(); err != nil {
			if err.Error() == "test error" {
				result = append(result, "<err>")
			} else {
				t.Errorf("test case %d failed; err=%v", tcIndex, err)
			}
		}
		got := strings.Join(result, "|")
		if got != tc.want {
			t.Errorf("test case %d: got %q, want %q", tcIndex, got, tc.want)
		}
	}
}
