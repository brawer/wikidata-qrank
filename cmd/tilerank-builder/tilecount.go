// SPDX-License-Identifier: MIT

package main

import (
	"encoding/binary"
	"github.com/lanrat/extsort"
)

type TileCount struct {
	Key   TileKey
	Count uint64
}

func (c TileCount) ToBytes() []byte {
	zoom, x, y := c.Key.ZoomXY()
	buf := make([]byte, binary.MaxVarintLen32*2+binary.MaxVarintLen64+1)
	pos := binary.PutUvarint(buf, uint64(x))
	pos += binary.PutUvarint(buf[pos:], uint64(y))
	pos += binary.PutUvarint(buf[pos:], c.Count)
	buf[pos] = zoom
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
	key := MakeTileKey(zoom, uint32(x), uint32(y))
	return TileCount{Key: key, Count: count}
}

func TileCountLess(a, b extsort.SortType) bool {
	aa := a.(TileCount)
	bb := b.(TileCount)
	if aa.Key != bb.Key {
		return aa.Key < bb.Key
	} else {
		return aa.Count < bb.Count
	}
}
