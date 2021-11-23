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

		got := TileCountFromBytes(tc.ToBytes()).(TileCount)
		if got.X != tc.X || got.Y != tc.Y || got.Count != tc.Count {
			t.Errorf("not round-trippable: %v, got %v", tc, got)
		}
	}
}

func TestTileCountLess(t *testing.T) {
	for _, tc := range []struct {
		a, b     TileCount
		expected bool
	}{
		{TileCount{2, 5, 8}, TileCount{3, 5, 8}, true},  // a.X < b.X
		{TileCount{2, 5, 8}, TileCount{1, 5, 8}, false}, // a.X > b.X
		{TileCount{2, 5, 8}, TileCount{2, 6, 8}, true},  // a.Y < b.Y
		{TileCount{2, 5, 8}, TileCount{2, 4, 8}, false}, // a.Y > b.Y
		{TileCount{2, 5, 8}, TileCount{2, 5, 7}, false}, // a.Count>b.Count
		{TileCount{2, 5, 8}, TileCount{2, 5, 9}, true},  // a.Count<b.Count
		{TileCount{2, 5, 8}, TileCount{2, 5, 8}, false}, // all equal
	} {
		got := TileCountLess(tc.a, tc.b)
		if tc.expected != got {
			t.Errorf("expected TileCountLess(%v, %v) = %v, got %v",
				tc.a, tc.b, tc.expected, got)
		}
	}
}
