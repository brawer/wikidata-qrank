// SPDX-License-Identifier: MIT

package main

import (
	"encoding/binary"
	"github.com/lanrat/extsort"
)

type TileCount struct {
	X, Y  uint32
	Count uint64
	Zoom  uint8
}

func (c TileCount) ToBytes() []byte {
	buf := make([]byte, binary.MaxVarintLen32*2+binary.MaxVarintLen64+1)
	pos := binary.PutUvarint(buf, uint64(c.X))
	pos += binary.PutUvarint(buf[pos:], uint64(c.Y))
	pos += binary.PutUvarint(buf[pos:], c.Count)
	buf[pos] = c.Zoom
	pos += 1
	return buf[0:pos]
}

func TileCountFromBytes(b []byte) extsort.SortType {
	x, pos := binary.Uvarint(b)
	y, len := binary.Uvarint(b[pos:])
	pos += len
	count, len := binary.Uvarint(b[pos:])
	pos += len
	zoom := b[pos]
	return TileCount{X: uint32(x), Y: uint32(y), Count: count, Zoom: zoom}
}

func TileCountLess(a, b extsort.SortType) bool {
	aa := a.(TileCount)
	aKey := uint64(aa.Y) << (64 - aa.Zoom)
	aKey = aKey | uint64(aa.X)<<(44-aa.Zoom) | uint64(aa.Zoom)

	bb := b.(TileCount)
	bKey := uint64(bb.Y) << (64 - bb.Zoom)
	bKey = bKey | uint64(bb.X)<<(44-bb.Zoom) | uint64(bb.Zoom)

	if aKey != bKey {
		return aKey < bKey
	} else {
		return aa.Count < bb.Count
	}
}
