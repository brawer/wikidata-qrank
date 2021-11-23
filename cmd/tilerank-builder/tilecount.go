// SPDX-License-Identifier: MIT

package main

import (
	"encoding/binary"
	"github.com/lanrat/extsort"
)

type TileCount struct {
	X, Y  uint32
	Count uint64
}

func (c TileCount) ToBytes() []byte {
	buf := make([]byte, binary.MaxVarintLen32*2+binary.MaxVarintLen64)
	pos := binary.PutUvarint(buf, uint64(c.X))
	pos += binary.PutUvarint(buf[pos:], uint64(c.Y))
	pos += binary.PutUvarint(buf[pos:], c.Count)
	return buf[0:pos]
}

func TileCountFromBytes(b []byte) extsort.SortType {
	x, pos := binary.Uvarint(b)
	y, len := binary.Uvarint(b[pos:])
	pos += len
	count, _ := binary.Uvarint(b[pos:])
	return TileCount{X: uint32(x), Y: uint32(y), Count: count}
}

func TileCountLess(a, b extsort.SortType) bool {
	c := a.(TileCount)
	d := b.(TileCount)

	if c.X < d.X {
		return true
	} else if c.X > d.X {
		return false
	}

	if c.Y < d.Y {
		return true
	} else if c.Y > d.Y {
		return false
	}

	return c.Count < d.Count
}
