// SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"bufio"
	"bytes"
	"container/heap"
	"io"
)

// Merges the lines of a multiple io.Readers whose content is in sorted order.
type LineMerger struct {
	heap   lineMergerHeap
	err    error
	inited bool
}

func NewLineMerger(r []io.Reader) *LineMerger {
	m := &LineMerger{}
	m.heap = make(lineMergerHeap, 0, len(r))
	for _, rr := range r {
		item := &mergee{scanner: bufio.NewScanner(rr)}
		if item.scanner.Scan() {
			m.heap = append(m.heap, item)
		}
		if err := item.scanner.Err(); err != nil {
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
	scanner *bufio.Scanner
	index   int
}

type lineMergerHeap []*mergee

func (h lineMergerHeap) Len() int { return len(h) }

func (h lineMergerHeap) Less(i, j int) bool {
	return bytes.Compare(h[i].scanner.Bytes(), h[j].scanner.Bytes()) < 0
}

func (h lineMergerHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *lineMergerHeap) Push(x interface{}) {
	n := len(*h)
	item := x.(*mergee)
	item.index = n
	*h = append(*h, item)
}

func (h *lineMergerHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	item.index = -1
	*h = old[0 : n-1]
	return item
}
