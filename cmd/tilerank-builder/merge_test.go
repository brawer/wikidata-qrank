// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"sort"
	"strings"
	"testing"

	"golang.org/x/sync/errgroup"
)

func TestMergeTileCounts(t *testing.T) {
	// Helper for sorting a []TileCount array.
	sortCounts := func(counts []TileCount) {
		sort.Slice(counts, func(i, j int) bool {
			return TileCountLess(counts[i], counts[j])
		})
	}

	want := make([]TileCount, 0, 10000) // Expected output.

	// Prepare the input for running the merge function under test.
	// We pass 100 input readers, each with 0..99 random TileCounts
	// in already sorted order. For the sake of debugging,
	// TileCount.Count indicates which reader supplied the value.
	readers := make([]io.Reader, 0, 100)
	for i := 0; i < 100; i++ {
		var buf strings.Builder
		counts := make([]TileCount, 0, 100)
		for _, tileKey := range makeTestTileKeys(rand.Intn(100)) {
			counts = append(counts, TileCount{Key: tileKey, Count: uint64(i)})
		}
		sortCounts(counts) // Input to mergeTileCounts() is in sorted order.
		for _, c := range counts {
			want = append(want, c)
			fmt.Fprintf(&buf, "%s %d\n", c.Key, c.Count)
		}
		readers = append(readers, strings.NewReader(buf.String()))
	}
	sortCounts(want)

	got, err := readMerged(readers)
	if err != nil {
		t.Fatal(err)
	}

	if len(got) != len(want) {
		t.Fatalf("got %d TileCounts, want %d", len(got), len(want))
	}

	for i := 0; i < len(got); i++ {
		if got[i] != want[i] {
			t.Fatalf("got TileCount[%d]=%v, want %v", i, got[i], want[i])
		}
	}
}

// Helper for testing mergeTileCounts().
func readMerged(readers []io.Reader) ([]TileCount, error) {
	result := make([]TileCount, 0, 10000)
	// To test channel overflow, pass a channel that buffers just one item.
	ch := make(chan TileCount, 1)
	g, ctx := errgroup.WithContext(context.Background())
	g.Go(func() error {
		return mergeTileCounts(readers, ch, ctx)
	})
	g.Go(func() error {
		for t := range ch {
			result = append(result, t)
		}
		return nil
	})
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return result, nil
}
