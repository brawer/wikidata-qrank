// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"fmt"
	"io"

	"golang.org/x/sync/errgroup"
)

type Painter struct {
	numWeeks int
	zoom     uint8
	last     TileKey
}

func (p *Painter) Paint(tile TileKey, counts []uint64) error {
	p.last = tile
	return nil
}

func (p *Painter) Close() error {
	// For the part of the world we haven't covered yet, paint empty rasters.
	zoom := p.zoom - 8
	for t := p.last.Next(zoom); t != NoTile; t = t.Next(zoom) {
		// TODO: Emit empty raster.
	}
	return nil
}

func NewPainter(numWeeks int, zoom uint8) *Painter {
	return &Painter{numWeeks: numWeeks, zoom: zoom}
}

// Paint produces a GeoTIFF file from a set of weekly tile view counts.
// Tile views at zoom level `zoom` become one pixel in the output GeoTIFF.
func paint(cachedir string, zoom uint8, tilecounts []io.Reader, ctx context.Context) error {
	// One goroutine is decompressing, parsing and merging the weekly counts;
	// another is painting the image from data that gets sent over a channel.
	ch := make(chan TileCount, 100000)
	painter := NewPainter(len(tilecounts), zoom)
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
						if err := painter.Paint(tile, counts[:numCounts]); err != nil {
							return err
						}
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
						if err := painter.Paint(tile, counts[:numCounts]); err != nil {
							return err
						}
					}
					return nil
				}
			}
		}
	})
	if err := g.Wait(); err != nil {
		return err
	}
	if err := painter.Close(); err != nil {
		return err
	}
	return nil
}
