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
	aKey := MakeTileKey(aa.Zoom, aa.X, aa.Y)

	bb := b.(TileCount)
	bKey := MakeTileKey(bb.Zoom, bb.X, bb.Y)

	if aKey != bKey {
		return aKey < bKey
	} else {
		return aa.Count < bb.Count
	}
}
