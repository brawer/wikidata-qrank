// SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
	"strconv"

	"github.com/lanrat/extsort"
)

// TileLatitude returns the latitude of a web mercator tile’s northern edge,
// in radians. For degrees, multiply by 180/π.
func TileLatitude(zoom uint8, y uint32) float64 {
	yf := 1.0 - 2.0*float64(y)/float64(uint32(1)<<zoom)
	return math.Atan(math.Sinh(math.Pi * yf))
}

// TileArea returns the area of a web mercator tile in km².
func TileArea(zoom uint8, y uint32) float64 {
	earthSurface := 510065623.0 // in km²
	latFraction := (TileLatitude(zoom, y) - TileLatitude(zoom, y+1)) / math.Pi
	return earthSurface * latFraction / float64(uint32(1)<<zoom)
}

// TileKey encodes a zoom/x/y tile into an uin64. Containing tiles get
// sorted before all their content; when sorting a set of tile keys,
// the resulting order is that of a depth-first pre-order tree traversal.
type TileKey uint64

// WorldTile is the tile for the entire planet, to the extent it is visible
// in the Web Mercator projection.
const WorldTile = TileKey(0)

// NoTile is a tile that does not actually exist. This is useful
// as an out-of-range value when iterating over a range of tiles.
const NoTile = TileKey(^uint64(0x1f)) // zoom 0, sorts after all valid tiles

// MakeTileKey returns a TileKey given the zoom/x/y tile coordinates.
func MakeTileKey(zoom uint8, x, y uint32) TileKey {
	val := uint64(zoom)
	shift := uint8(64 - 2*zoom)
	for bit := uint8(0); bit < zoom; bit++ {
		xm := uint64((x>>bit)&1) << shift
		ym := uint64((y>>bit)&1) << (shift + 1)
		val |= xm | ym
		shift += 2
	}
	return TileKey(val)
}

// Contains returns true if this tile strictly contains `other`.
func (t TileKey) Contains(other TileKey) bool {
	zoom := t.Zoom()
	otherZoom := other.Zoom()
	if otherZoom > zoom {
		return t == other.ToZoom(zoom)
	} else {
		return false
	}
}

// Zoom returns the zoom level of a TileKey.
func (t TileKey) Zoom() uint8 {
	return uint8(t) & 0x1f
}

// ZoomXY returns the zoom, x and y coordinates for a TileKey.
func (t TileKey) ZoomXY() (zoom uint8, x, y uint32) {
	val := uint64(t)
	zoom = uint8(val) & 0x1f
	shift := uint8(64 - 2*zoom)
	for bit := uint8(0); bit < zoom; bit++ {
		x |= (uint32(val>>shift) & 1) << bit
		y |= (uint32(val>>(shift+1)) & 1) << bit
		shift += 2
	}
	return zoom, x, y
}

func (t TileKey) ToZoom(z uint8) TileKey {
	val := uint64(t)
	shift := uint8(64 - 2*z)
	return TileKey(((val >> shift) << shift) | uint64(z))
}

// String formats the tile coordinates into a string.
func (t TileKey) String() string {
	if t == NoTile {
		return "NoTile"
	}

	zoom, x, y := t.ZoomXY()
	return fmt.Sprintf("%d/%d/%d", zoom, x, y)
}

// Next returns the next TileKey in pre-order depth-first traversal order,
// or NoTile after we’ve reached the very last tile.
func (t TileKey) Next(maxZoom uint8) TileKey {
	zoom := uint8(t) & 0x1f

	// Descend into tree: x/y/0 → x/y/1 → ... → x/y/maxZoom, for any x and y.
	if zoom < maxZoom {
		return TileKey(uint64(t) & ^uint64(0x1f) | uint64(zoom+1))
	}

	shift := uint8(64 - 2*maxZoom)
	val := uint64(t) >> shift

	// Terminate after last tile.
	if bits.OnesCount64(val) == int(2*maxZoom) { // 2/3/3 → NoTile
		return NoTile
	}

	val = val + 1
	newZoom := maxZoom - uint8(bits.TrailingZeros64(val)/2)
	return TileKey(val<<shift | uint64(newZoom))
}

// TileCount counts the number of impressions for a tile.
type TileCount struct {
	Key   TileKey
	Count uint64
}

func ParseTileCount(s string) TileCount {
	match := tileLogRegexp.FindStringSubmatch(s)
	if match == nil || len(match) != 5 {
		return TileCount{NoTile, 0}
	}
	zoom, _ := strconv.Atoi(match[1])
	if zoom < 0 || zoom > 24 {
		return TileCount{NoTile, 0}
	}
	x, _ := strconv.ParseUint(match[2], 10, 32)
	y, _ := strconv.ParseUint(match[3], 10, 32)
	if x >= 1<<zoom || y >= 1<<zoom {
		return TileCount{NoTile, 0}
	}
	count, _ := strconv.ParseUint(match[4], 10, 64)
	key := MakeTileKey(uint8(zoom), uint32(x), uint32(y))
	return TileCount{Key: key, Count: count}
}

// ToBytes serializes a TileCount into a byte array.
func (c TileCount) ToBytes() []byte {
	zoom, x, y := c.Key.ZoomXY()
	var buf [binary.MaxVarintLen32*2 + binary.MaxVarintLen64 + 1]byte
	pos := binary.PutUvarint(buf[:], uint64(x))
	pos += binary.PutUvarint(buf[pos:], uint64(y))
	pos += binary.PutUvarint(buf[pos:], c.Count)
	buf[pos] = zoom
	pos += 1
	return buf[0:pos]
}

// TileCountFromBytes de-serializes a TileCount from a byte array.
// The result is returned as an extsort.SortType because that is
// needed by the library for external sorting.
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

// TileCountLess returns true if TileCount a should be sorted before b.
// The arguments are passed as extsort.SortType because that is
// needed by the library for external sorting.
func TileCountLess(a, b extsort.SortType) bool {
	aa := a.(TileCount)
	bb := b.(TileCount)
	if aa.Key != bb.Key {
		return aa.Key < bb.Key
	} else {
		return aa.Count < bb.Count
	}
}
