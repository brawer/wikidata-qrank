// SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"testing"
)

func TestFindSharedTiles(t *testing.T) {
	shared := findSharedTiles([]uint32{12, 72, 88, 72, 32, 18})
	if len(shared) != 1 {
		t.Fatalf("want len(shared) == 1, got %d", len(shared))
	}

	samples := shared[72].SampleTiles
	found := false
	for _, tile := range samples {
		if tile == 1 || tile == 3 {
			found = true
		} else {
			t.Errorf("unexpected tile %d; samples=%v", tile, samples)
		}
		if !found {
			t.Errorf("expected 1 and/or 3; samples=%v", samples)
		}
	}
}
