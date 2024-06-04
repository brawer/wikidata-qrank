// SPDX-FileCopyrightText: 2024 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"encoding/binary"
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
