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
			fmt.Println(tile)
		}
		return nil
	})
	if err := g.Wait(); err != nil {
		return err
	}
	return nil
}
