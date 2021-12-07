// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"fmt"
	"io"

	"golang.org/x/sync/errgroup"
)

// Paint a GeoTIFF file.
func paint(cachedir string, zoom int, tilecounts []io.Reader, ctx context.Context) error {
	ch := make(chan TileCount, 100000)
	g, subCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return mergeTileCounts(tilecounts, ch, subCtx)
	})
	g.Go(func() error {
		tile := WorldTile
		counts := make([]uint64, len(tilecounts))
		numCounts := 0 // number of counts for the same tile
		for {
			select {
			case <-subCtx.Done():
				return subCtx.Err()
			case c, more := <-ch:
				if c.Key != tile {
					if numCounts > 0 {
						// fmt.Println("TODO: Paint", tile, counts[:numCounts], more)
					}
					numCounts = 0
					tile = c.Key
				}

				if c.Count > 0 {
					if numCounts >= len(counts) {
						return fmt.Errorf("tile %s appears more than %d times in input", tile.String(), len(counts))
					}
					counts[numCounts] = c.Count
					numCounts = numCounts + 1
				}

				if !more {
					if numCounts > 0 {
						// fmt.Println("TODO: Paint", tile, counts[:numCounts], more)
					}
					return nil
				}
			}
		}
	})
	if err := g.Wait(); err != nil {
		return err
	}
	return nil
}
