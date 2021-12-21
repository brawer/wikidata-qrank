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
	writer   *RasterWriter
}

func (p *Painter) Paint(tile TileKey, counts []uint64) error {
	raster, err := p.setupRaster(tile)
	if err != nil {
		return err
	}

	// Compute the average weekly views per km² for this tile.
	// TODO: Since the counts are already in sorted order, we could
	// easily ignore the top and bottom percentiles. This might
	// help to smoothen out short-term peaks. Figure out if this
	// is worth doing, and what percentile thresholds to use.
	// Don't forget we also have (p.numWeeks - len(counts)) weeks
	// that had zero views for this tile. For the current averaging,
	// this is accounted for because we divide by p.numWeeks; please
	// make sure to consider this when changing the aggregation logic.
	sum := uint64(0)
	for _, c := range counts {
		sum += c
	}
	zoom, _, y := tile.ZoomXY()
	viewsPerKm2 := float32(sum) / (float32(p.numWeeks) * float32(TileArea(zoom, y)))

	if tile == raster.tile {
		raster.viewsPerKm2 = viewsPerKm2
		if raster.parent != nil {
			raster.viewsPerKm2 += raster.parent.viewsPerKm2
		}
	}

	raster.Paint(tile, viewsPerKm2)

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
		if err := p.emitRaster(); err != nil {
			return nil, err
		}
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
			err := p.writer.WriteUniform(t, uint32(p.raster.viewsPerKm2+0.5))
			if err != nil {
				return nil, err
			}
		}
	}

	p.raster = NewRaster(rasterTile, p.raster)
	//fmt.Printf("final rasterTile=%s tile=%s\n", rasterTile, tile)
	return p.raster, nil
}

func (p *Painter) Close() error {
	// For the part of the world we haven't covered yet, emit uniform rasters.
	zoom := p.zoom - 8
	for t := p.last.Next(zoom); t != NoTile; t = t.Next(zoom) {
		for p.raster != nil && !p.raster.tile.Contains(t) {
			if err := p.emitRaster(); err != nil {
				return err
			}
		}
		if err := p.writer.WriteUniform(t, uint32(p.raster.viewsPerKm2+0.5)); err != nil {
			return err
		}
	}

	for p.raster != nil {
		if err := p.emitRaster(); err != nil {
			return err
		}
	}

	return p.writer.Close()
}

// Function emitRaster is called when the Painter has finished painting
// pixels into the current Raster. The raster gets removed from the tree,
// compressed, and stored into a temporary file.
// TODO: Subsample pixels to parent raster on behalf of GeoTIFF overview.
func (p *Painter) emitRaster() error {
	raster := p.raster
	if raster.parent != nil {
		raster.parent.PaintChild(raster)
	}
	p.raster = raster.parent
	raster.parent = nil
	return p.writer.Write(raster)
}

func NewPainter(path string, numWeeks int, zoom uint8) (*Painter, error) {
	writer, err := NewRasterWriter(path, zoom-8)
	if err != nil {
		return nil, err
	}
	return &Painter{
		numWeeks: numWeeks,
		zoom:     zoom,
		writer:   writer,
	}, nil
}

// Paint produces a GeoTIFF file from a set of weekly tile view counts.
// Tile views at zoom level `zoom` become one pixel in the output GeoTIFF.
func paint(path string, zoom uint8, tilecounts []io.Reader, ctx context.Context) error {
	// One goroutine is decompressing, parsing and merging the weekly counts;
	// another is painting the image from data that gets sent over a channel.
	ch := make(chan TileCount, 100000)
	painter, err := NewPainter(path, len(tilecounts), zoom)
	if err != nil {
		return err
	}
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
