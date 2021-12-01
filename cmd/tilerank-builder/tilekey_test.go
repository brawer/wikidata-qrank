// SPDX-License-Identifier: MIT

package main

import (
	"math/rand"
	"testing"
)

var tk TileKey

func BenchmarkMakeTileKey(b *testing.B) {
	zoom := make([]uint8, 64)
	x := make([]uint32, 64)
	y := make([]uint32, 64)
	for i, key := range makeTestTileKeys(64) {
		zoom[i], x[i], y[i] = key.ZoomXY()
	}
	for n := 0; n < b.N; n++ {
		tk = MakeTileKey(zoom[n%64], x[n%64], y[n%64])
	}
}

var unused uint32

func BenchmarkTileKeyZoomXY(b *testing.B) {
	keys := makeTestTileKeys(64)
	for n := 0; n < b.N; n++ {
		zoom, x, y := keys[n%64].ZoomXY()
		unused |= uint32(zoom) + x + y
	}
}

func TestMakeTileKey(t *testing.T) {
	for n := 0; n < 5000; n++ {
		zoom := uint8(rand.Intn(24))
		x := uint32(rand.Intn(1 << zoom))
		y := uint32(rand.Intn(1 << zoom))
		key := MakeTileKey(zoom, x, y)
		gotZoom, gotX, gotY := key.ZoomXY()
		if gotZoom != zoom || gotX != x || gotY != y {
			t.Errorf("expected %d/%d/%d, got %d/%d/%d", zoom, x, y, gotZoom, gotX, gotY)
		}
	}
}

func makeTestTileKeys(n int) []TileKey {
	keys := make([]TileKey, n)
	for i := 0; i < n; i++ {
		zoom := uint8(rand.Intn(24))
		x := uint32(rand.Intn(1 << zoom))
		y := uint32(rand.Intn(1 << zoom))
		keys[i] = MakeTileKey(zoom, x, y)
	}
	return keys
}
