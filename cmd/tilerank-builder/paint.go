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
	raster   *Raster
}

func (p *Painter) Paint(tile TileKey, counts []uint64) error {
	if _, err := p.setupRaster(tile); err != nil {
		return err
	}
	p.last = tile
	return nil
}

func (p *Painter) setupRaster(tile TileKey) (*Raster, error) {
	rasterTile := tile
	if tile.Zoom() >= p.zoom-8 {
		rasterTile = tile.ToZoom(p.zoom - 8)
	}

	// If the current raster is for rasterTile, we’re already set up.
	if p.raster != nil && rasterTile == p.raster.tile {
		return p.raster, nil
	}

	// Since we’re receiving tiles in pre-order depth-first traversal order,
	// we’re completely done with any parent Rasters that do not contain
	// the new rasterTile. Those can be compressed and stored into the
	// output TIFF file.
	for p.raster != nil && !p.raster.tile.Contains(rasterTile) {
		// TODO: Compress p.raster and store it into TIFF file.
		// fmt.Printf("TODO: Compress and store %s\n", p.raster.tile)
		p.raster = p.raster.parent
	}

	if p.raster == nil {
		p.raster = NewRaster(WorldTile, nil)
		if rasterTile == WorldTile {
			return p.raster, nil
		}
	}

	for t := p.last.Next(p.zoom - 8); t < rasterTile; t = t.Next(p.zoom - 8) {
		if t.Contains(rasterTile) {
			p.raster = NewRaster(t, p.raster)
		} else {
			// fmt.Printf("TODO: Store empty raster %s into output TIFF\n", t)
		}
	}

	p.raster = NewRaster(rasterTile, p.raster)
	//fmt.Printf("final rasterTile=%s tile=%s\n", rasterTile, tile)
	return p.raster, nil
}

func (p *Painter) Close() error {
	// For the part of the world we haven't covered yet, paint empty rasters.
	zoom := p.zoom - 8
	for t := p.last.Next(zoom); t != NoTile; t = t.Next(zoom) {
		// fmt.Printf("TODO: Store empty raster %s into output TIFF\n", t)
	}
	return nil
}

func NewPainter(numWeeks int, zoom uint8) *Painter {
	return &Painter{numWeeks: numWeeks, zoom: zoom}
}

type Raster struct {
	tile   TileKey
	area   float64
	parent *Raster
}

func NewRaster(tile TileKey, parent *Raster) *Raster {
	zoom, _, y := tile.ZoomXY()

	// Check that NewRaster() is called for the right parent. This check
	// should never fail, no matter what the input data is. If it does fail,
	// something must be wrong with our logic to construct parent rasters.
	if parent != nil {
		if zoom != parent.tile.Zoom()+1 {
			panic(fmt.Sprintf("NewRaster(%s) with parent.tile=%s", tile, parent.tile))
		}
	} else if zoom != 0 {
		panic(fmt.Sprintf("NewRaster(%s) with parent=<nil>", tile))
	}

	return &Raster{tile: tile, area: TileArea(zoom, y), parent: parent}
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
