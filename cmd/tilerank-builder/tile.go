// SPDX-License-Identifier: MIT

package main

import (
	"math"
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
