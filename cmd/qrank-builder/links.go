// SPDX-FileCopyrightText: 2024 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/lanrat/extsort"
)

type Link struct {
	Source int64
	Target int64
}

func (link Link) ToBytes() []byte {
	buf := make([]byte, binary.MaxVarintLen64*2)
	p := binary.PutVarint(buf, link.Source)
	p += binary.PutVarint(buf[p:], link.Target)
	return buf[0:p]
}

func LinkFromBytes(b []byte) extsort.SortType {
	source, pos := binary.Varint(b)
	target, _ := binary.Varint(b[pos:])
	return Link{Source: source, Target: target}
}

func LinkLess(a, b extsort.SortType) bool {
	aa, bb := a.(Link), b.(Link)
	if aa.Source < bb.Source {
		return true
	} else if aa.Source > bb.Source {
		return false
	}

	if aa.Target < bb.Target {
		return true
	} else if aa.Target > bb.Target {
		return false
	}

	return false
}

type LinkWriter struct {
	out        *bufio.Writer
	lastSource int64
	lastTarget int64
}

func NewLinkWriter(w io.Writer) *LinkWriter {
	return &LinkWriter{out: bufio.NewWriter(w)}
}

func (w *LinkWriter) Write(link Link) error {
	if link.Source == w.lastSource && link.Target == w.lastTarget {
		return nil
	}

	if link.Source == link.Target {
		return nil
	}

	line := fmt.Sprintf("Q%d,Q%d\n", link.Source, link.Target)
	if _, err := w.out.WriteString(line); err != nil {
		return err
	}

	w.lastSource = link.Source
	w.lastTarget = link.Target
	return nil
}

func (w *LinkWriter) Flush() error {
	return w.out.Flush()
}
