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

	// When painting for zoom=17, 191925 of 349525 output rasters (about 55%)
	// have the same color for  all pixels. That number is large enough to
	// be worth optimizing for, and small enough to keep their tile keys and
	// color in memory. However, about 30% of all output rasters (or 54% of
	// the uniformly colored ones) have less then 0.5 views per km², so we
	// don’t keep those.
	uniformRasters map[TileKey]float32
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
			p.emitUniformRaster(t, p.raster.viewsPerKm2)
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
		p.emitUniformRaster(t, p.raster.viewsPerKm2)
	}

	for p.raster != nil {
		if err := p.emitRaster(); err != nil {
			return err
		}
	}

	return nil
}

// Function emitRaster is called when the Painter has finished painting
// pixels into the current Raster. The raster gets removed from the tree,
// compressed, and stored into a temporary file.
// TODO: Subsample pixels to parent raster on behalf of GeoTIFF overview.
func (p *Painter) emitRaster() error {
	raster := p.raster
	p.raster = raster.parent
	raster.parent = nil

	// TODO: Compress p.raster and store it into TIFF file.
	// fmt.Printf("TODO: Emit %s\n", raster.tile)
	return nil
}

// Function emitUniformRaster is called to produce a raster whose pixels
// all have the same color. In a typical output, about 55% of the rasters
// are uniformly coloreds, so we treat them specially.
func (p *Painter) emitUniformRaster(tile TileKey, viewsPerKm2 float32) {
	// About 30% of all output rasters (or 54% of the uniformly colored ones)
	// have less then 0.5 views per km², so we don’t keep those in memory.
	if viewsPerKm2 >= 0.5 {
		p.uniformRasters[tile] = viewsPerKm2
	}
}

func NewPainter(numWeeks int, zoom uint8) *Painter {
	return &Painter{
		numWeeks:       numWeeks,
		zoom:           zoom,
		uniformRasters: make(map[TileKey]float32, 10000),
	}
}

type Raster struct {
	tile        TileKey
	parent      *Raster
	viewsPerKm2 float32
	pixels      [256 * 256]float32
}

func (r *Raster) Paint(tile TileKey, viewsPerKm2 float32) {
	rZoom, rX, rY := r.tile.ZoomXY()

	// If the to-be-painted tile is smaller than 1 pixel, we scale it
	// to one pixel and reduce the number of views accordingly.
	// We only do this at deep zoom levels, where the area per pixel
	// is nearly uniform despite the distortion of the web mercator
	// projection.
	if zoom := tile.Zoom(); zoom > rZoom+8 {
		viewsPerKm2 /= float32(int32(1 << (2 * (zoom - (rZoom + 8)))))
		tile = tile.ToZoom(rZoom + 8)
	}

	zoom, x, y := tile.ZoomXY()
	deltaZoom := zoom - rZoom
	left := (x - rX<<deltaZoom) << (8 - deltaZoom)
	top := (y - rY<<deltaZoom) << (8 - deltaZoom)
	width := uint32(1 << (8 - deltaZoom))
	// Because our tiles are squares, the height is the same as the width.
	for y := top; y < top+width; y++ {
		for x := left; x < left+width; x++ {
			r.pixels[y<<8+x] += viewsPerKm2
		}
	}
}

func NewRaster(tile TileKey, parent *Raster) *Raster {
	zoom := tile.Zoom()

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

	return &Raster{tile: tile, parent: parent}
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
