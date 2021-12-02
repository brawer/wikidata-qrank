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
		for tile := range ch {
			select {
			case <-subCtx.Done():
				return subCtx.Err()
			default:
			}
			zoom, x, y := tile.Key.ZoomXY()
			if zoom <= 4 {
				fmt.Printf("%d/%d/%d %d\n", zoom, x, y, tile.Count)
			}
		}
		return nil
	})
	if err := g.Wait(); err != nil {
		return err
	}
	return nil
}
