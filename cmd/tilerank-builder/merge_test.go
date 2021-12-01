// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"

	"golang.org/x/sync/errgroup"
)

func TestMergeTileCounts(t *testing.T) {
	// TODO: Should pass multiple readers, once k-way merging is implemented.
	r1 := strings.NewReader("0/0/0 42\n15/456/789 3\n")
	readers := []io.Reader{r1}
	expected := "0/0/0 42◆15/456/789 3"
	if got, err := readMerged(readers); err != nil {
		t.Error(err)
	} else if expected != got {
		t.Errorf("expected %v, got %v", expected, got)
	}
}

// Helper for testing mergeTileCounts().
func readMerged(readers []io.Reader) (string, error) {
	var result strings.Builder
	// To test channel overflow, pass a channel that buffers just one item.
	ch := make(chan TileCount, 1)
	g, ctx := errgroup.WithContext(context.Background())
	g.Go(func() error {
		return mergeTileCounts(readers, ch, ctx)
	})
	g.Go(func() error {
		first := true
		for tile := range ch {
			if !first {
				result.WriteRune('◆')
			}
			first = false
			result.WriteString(fmt.Sprintf("%d/%d/%d %d", tile.Zoom, tile.X, tile.Y, tile.Count))
		}
		return nil
	})
	if err := g.Wait(); err != nil {
		return "", err
	}
	return result.String(), nil
}
