// SPDX-License-Identifier: MIT

package main

import (
	"math/rand"
	"testing"
)

func TestTileCountRoundTrip(t *testing.T) {
	for _, key := range makeTestTileKeys(1000) {
		tc := TileCount{Key: key, Count: rand.Uint64()}
		got := TileCountFromBytes(tc.ToBytes()).(TileCount)
		if got.Key != tc.Key || got.Count != tc.Count {
			t.Errorf("not round-trippable: %v, got %v", tc, got)
		}
	}
}

func TestTileCountLess(t *testing.T) {
	type TC struct {
		x, y  uint32
		count uint64
		zoom  uint8
	}
	for _, tc := range []struct {
		a, b     TC
		expected bool
	}{
		{TC{2, 5, 8, 9}, TC{3, 5, 8, 9}, true},  // a.X < b.X
		{TC{2, 5, 8, 9}, TC{1, 5, 8, 9}, false}, // a.X > b.X
		{TC{2, 5, 8, 9}, TC{2, 6, 8, 9}, true},  // a.Y < b.Y
		{TC{2, 5, 8, 9}, TC{2, 4, 8, 9}, false}, // a.Y > b.Y

		{TC{2, 5, 8, 9}, TC{2, 5, 7, 9}, false}, // a.Count>b.Count
		{TC{2, 5, 8, 9}, TC{2, 5, 9, 9}, true},  // a.Count<b.Count
		{TC{2, 5, 8, 9}, TC{2, 5, 8, 9}, false}, // all equal

		{TC{0, 0, 0, 0}, TC{0, 0, 0, 0}, false},
		{TC{0, 0, 0, 0}, TC{0, 0, 0, 1}, true},
		{TC{0, 0, 0, 0}, TC{0, 1, 0, 1}, true},
		{TC{0, 0, 0, 0}, TC{1, 0, 0, 1}, true},
		{TC{0, 0, 0, 0}, TC{1, 1, 0, 1}, true},
		{TC{0, 0, 0, 1}, TC{0, 1, 0, 1}, true},
		{TC{0, 1, 0, 1}, TC{0, 1, 0, 1}, false},
		{TC{1, 0, 0, 1}, TC{0, 1, 0, 1}, true},
		{TC{1, 1, 0, 1}, TC{0, 1, 0, 1}, false},
		{TC{0, 0, 0, 2}, TC{0, 1, 0, 1}, true},

		{TC{17187, 11494, 104, 15}, TC{17187, 11495, 79, 15}, true},
		{TC{17187, 11495, 79, 15}, TC{17187, 11494, 104, 15}, false},
	} {
		a := TileCount{MakeTileKey(tc.a.zoom, tc.a.x, tc.a.y), tc.a.count}
		b := TileCount{MakeTileKey(tc.b.zoom, tc.b.x, tc.b.y), tc.b.count}
		got := TileCountLess(a, b)
		if tc.expected != got {
			t.Errorf("expected TileCountLess(%v, %v) = %v, got %v",
				tc.a, tc.b, tc.expected, got)
		}
	}
}

func TestTileCountLessContainment(t *testing.T) {
	bigX := uint32(17161)
	bigY := uint32(11476)
	big := TileCount{Key: MakeTileKey(15, bigX, bigY), Count: 42}
	for y := bigY << 3; y < (bigY+1)<<3; y++ {
		for x := bigX << 3; x < (bigX+1)<<3; x++ {
			small := TileCount{Key: MakeTileKey(18, x, y), Count: 7}
			if !TileCountLess(big, small) {
				t.Errorf("expected TileCountLess(%v, %v) = true because the former geographically contains the latter",
					big, small)
			}
			if TileCountLess(small, big) {
				t.Errorf("expected TileCountLess(%v, %v) = false because the former is geographically contained within the latter",
					small, big)
			}
		}
	}

	smallOutside := TileCount{MakeTileKey(18, bigX<<3-1, bigY<<3), 42}
	if TileCountLess(big, smallOutside) {
		t.Errorf("expected TileCountLess(%v, %v) = false, got true",
			big, smallOutside)
	}
}
