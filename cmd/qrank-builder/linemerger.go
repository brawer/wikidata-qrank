// SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"container/heap"
	"fmt"
)

// Merges the lines of a multiple io.Readers whose content is in sorted order.
type LineMerger struct {
	heap   lineMergerHeap
	err    error
	inited bool
}

// LineScanner is implemented by bufio.Scanner and our own pageSignalsScanner.
type LineScanner interface {
	Scan() bool
	Err() error
	Bytes() []byte
	Text() string
}

// NewLineMerger creates an iterator that merges multiple sorted files,
// returning their lines in sort order. The passed names identify the
// scanners, and are part of the error message in case of failures.
// Being able to identify the failing input is useful for debugging.
// https://github.com/brawer/wikidata-qrank/issues/40
func NewLineMerger(r []LineScanner, names []string) *LineMerger {
	if len(r) != len(names) {
		panic(fmt.Sprintf("len(r) must be len(names), got %d vs %d", len(r), len(names)))
	}

	m := &LineMerger{}
	m.heap = make(lineMergerHeap, 0, len(r))
	for i, rr := range r {
		item := &mergee{scanner: rr, name: names[i]}
		if item.scanner.Scan() {
			m.heap = append(m.heap, item)
		}
		if err := item.scanner.Err(); err != nil {
			logger.Printf(`LineMerger: scanner "%s" failed to scan first line, err=%v`, item.name, err)
			m.err = err
			return m
		}
	}
	return m
}

func (m *LineMerger) Advance() bool {
	if m.err != nil {
		return false
	}
	if len(m.heap) == 0 {
		return false
	}
	if !m.inited {
		heap.Init(&m.heap)
		m.inited = true
		return true
	}
	item := m.heap[0]
	if item.scanner.Scan() {
		heap.Fix(&m.heap, 0)
	} else {
		heap.Remove(&m.heap, 0)
	}
	if err := item.scanner.Err(); err != nil {
		m.err = err
		logger.Printf(`LineMerger: scanner "%s" failed, err=%v`, item.name, err)
		return false
	}
	return len(m.heap) > 0
}

func (m *LineMerger) Err() error {
	return m.err
}

func (m *LineMerger) Line() string {
	n := len(m.heap)
	if n > 0 {
		return m.heap[0].scanner.Text()
	} else {
		return ""
	}
}

type mergee struct {
	scanner LineScanner
	name    string
}

type lineMergerHeap []*mergee

func (h lineMergerHeap) Len() int { return len(h) }

func (h lineMergerHeap) Less(i, j int) bool {
	return bytes.Compare(h[i].scanner.Bytes(), h[j].scanner.Bytes()) < 0
}

func (h lineMergerHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *lineMergerHeap) Push(x interface{}) {
	*h = append(*h, x.(*mergee))
}

func (h *lineMergerHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	*h = old[0 : n-1]
	return item
}
