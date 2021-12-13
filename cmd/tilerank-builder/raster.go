// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"runtime"

	"github.com/lanrat/extsort"
)

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

type RasterWriter struct {
	tiffTilesToSort  chan<- extsort.SortType
	tiffTiles        <-chan extsort.SortType
	tiffTilesSortErr <-chan error
}

func NewRasterWriter() *RasterWriter {
	inChan := make(chan extsort.SortType, 10000)
	config := extsort.DefaultConfig()
	config.NumWorkers = runtime.NumCPU()
	sorter, outChan, errChan := extsort.New(inChan, tiffTileFromBytes, tiffTileLess, config)
	go sorter.Sort(context.Background())
	return &RasterWriter{
		tiffTilesToSort:  inChan,
		tiffTiles:        outChan,
		tiffTilesSortErr: errChan,
	}
}

func (w *RasterWriter) Write(r *Raster) {
}

// WriteUniform produces a raster whose pixels all have the same color.
// In a typical output, about 55% of all rasters are uniformly coloreds,
// so we treat them specially as an optimization.
func (w *RasterWriter) WriteUniform(tile TileKey, color uint32) error {
	var t tiffTile
	zoom, x, y := tile.ZoomXY()
	t.zoom = zoom
	t.x = x
	t.y = y
	t.uniformColor = color
	w.tiffTilesToSort <- t
	return nil
}

func (w *RasterWriter) Close() error {
	close(w.tiffTilesToSort)
	for _ = range w.tiffTiles {
		// fmt.Printf("TODO: Store %v\n", t)
	}
	if err := <-w.tiffTilesSortErr; err != nil {
		return err
	}
	return nil
}

// tiffTile represents a raster tile that will be written into
// a Cloud-Optimized GeoTIFF file. The file format requires
// a specific arrangement of the data, which is different from
// the order in which we’re painting our raster tiles.
type tiffTile struct {
	zoom         uint8
	x, y         uint32
	uniformColor uint32
	byteCount    uint32
	offset       uint64
}

// ToBytes serializes a tiffTile into a byte array.
func (c tiffTile) ToBytes() []byte {
	var buf [1 + 4*binary.MaxVarintLen32 + binary.MaxVarintLen64]byte
	buf[0] = c.zoom
	pos := 1
	pos += binary.PutUvarint(buf[pos:], uint64(c.x))
	pos += binary.PutUvarint(buf[pos:], uint64(c.y))
	pos += binary.PutUvarint(buf[pos:], uint64(c.uniformColor))
	pos += binary.PutUvarint(buf[pos:], uint64(c.byteCount))
	pos += binary.PutUvarint(buf[pos:], c.offset)
	return buf[0:pos]
}

// Function tiffTileFromBytes de-serializes a tiffTile from a byte slice.
// The result is returned as an extsort.SortType because that is
// needed by the library for external sorting.
func tiffTileFromBytes(b []byte) extsort.SortType {
	zoom, pos := b[0], 1
	x, len := binary.Uvarint(b[1:])
	pos += len
	y, len := binary.Uvarint(b[pos:])
	pos += len
	uniformColor, len := binary.Uvarint(b[pos:])
	pos += len
	byteCount, len := binary.Uvarint(b[pos:])
	pos += len
	offset, len := binary.Uvarint(b[pos:])
	pos += len
	return tiffTile{
		zoom:         zoom,
		x:            uint32(x),
		y:            uint32(y),
		uniformColor: uint32(uniformColor),
		byteCount:    uint32(byteCount),
		offset:       offset,
	}
}

// tiffTileLess returns true if the TIFF tag for raster a should come
// before b in the Cloud-Optimized GeoTIFF file format.
func tiffTileLess(a, b extsort.SortType) bool {
	aa := a.(tiffTile)
	bb := b.(tiffTile)
	if aa.zoom != bb.zoom {
		return aa.zoom > bb.zoom
	} else if aa.y != bb.y {
		return aa.y < bb.y
	} else {
		return aa.x < bb.x
	}
}
