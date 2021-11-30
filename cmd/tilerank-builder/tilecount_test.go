// SPDX-License-Identifier: MIT

package main

import (
	"math/rand"
	"testing"
)

func TestTileCountRoundTrip(t *testing.T) {
	var tc TileCount
	for i := 0; i < 10000; i++ {
		tc.X = rand.Uint32()
		tc.Y = rand.Uint32()
		tc.Count = rand.Uint64()
		tc.Zoom = uint8(rand.Uint32() % 31)

		got := TileCountFromBytes(tc.ToBytes()).(TileCount)
		if got.X != tc.X || got.Y != tc.Y || got.Count != tc.Count || got.Zoom != tc.Zoom {
			t.Errorf("not round-trippable: %v, got %v", tc, got)
		}
	}
}

func TestTileCountLess(t *testing.T) {
	for _, tc := range []struct {
		a, b     TileCount
		expected bool
	}{
		{TileCount{2, 5, 8, 9}, TileCount{3, 5, 8, 9}, true},  // a.X < b.X
		{TileCount{2, 5, 8, 9}, TileCount{1, 5, 8, 9}, false}, // a.X > b.X
		{TileCount{2, 5, 8, 9}, TileCount{2, 6, 8, 9}, true},  // a.Y < b.Y
		{TileCount{2, 5, 8, 9}, TileCount{2, 4, 8, 9}, false}, // a.Y > b.Y
		{TileCount{2, 5, 8, 9}, TileCount{2, 5, 7, 9}, false}, // a.Count>b.Count
		{TileCount{2, 5, 8, 9}, TileCount{2, 5, 9, 9}, true},  // a.Count<b.Count
		{TileCount{2, 5, 8, 9}, TileCount{2, 5, 8, 9}, false}, // all equal

		{TileCount{0, 0, 0, 0}, TileCount{0, 0, 0, 0}, false},
		{TileCount{0, 0, 0, 0}, TileCount{0, 0, 0, 1}, true},
		{TileCount{0, 0, 0, 0}, TileCount{0, 1, 0, 1}, true},
		{TileCount{0, 0, 0, 0}, TileCount{1, 0, 0, 1}, true},
		{TileCount{0, 0, 0, 0}, TileCount{1, 1, 0, 1}, true},
		{TileCount{0, 0, 0, 1}, TileCount{0, 1, 0, 1}, true},
		{TileCount{0, 1, 0, 1}, TileCount{0, 1, 0, 1}, false},
		{TileCount{1, 0, 0, 1}, TileCount{0, 1, 0, 1}, true},
		{TileCount{1, 1, 0, 1}, TileCount{0, 1, 0, 1}, false},
		{TileCount{0, 0, 0, 2}, TileCount{0, 1, 0, 1}, true},

		{TileCount{17187, 11494, 104, 15}, TileCount{17187, 11495, 79, 15}, true},
		{TileCount{17187, 11495, 79, 15}, TileCount{17187, 11494, 104, 15}, false},
	} {
		got := TileCountLess(tc.a, tc.b)
		if tc.expected != got {
			t.Errorf("expected TileCountLess(%v, %v) = %v, got %v",
				tc.a, tc.b, tc.expected, got)
		}
	}
}

func TestTileCountLessContainment(t *testing.T) {
	big := TileCount{Zoom: 15, X: 17161, Y: 11476, Count: 42}
	last := big
	for y := big.Y << 3; y < (big.Y+1)<<3; y++ {
		for x := big.X << 3; x < (big.X+1)<<3; x++ {
			small := TileCount{Zoom: 18, X: x, Y: y, Count: 7}
			if !TileCountLess(big, small) {
				t.Errorf("expected TileCountLess(%v, %v) = true because the former geographically contains the latter",
					big, small)
			}
			if TileCountLess(small, big) {
				t.Errorf("expected TileCountLess(%v, %v) = false because the former is geographically contained within the latter",
					small, big)
			}
			if !TileCountLess(last, small) {
				t.Errorf("expected TileCountLess(%v, %v) = true", last, small)
			}
			last = small
		}
	}

	smallOutside := TileCount{Zoom: 18, X: (big.X << 3) - 1, Y: big.Y << 3}
	if TileCountLess(big, smallOutside) {
		t.Errorf("expected TileCountLess(%v, %v) = false, got true",
			big, smallOutside)
	}
}
