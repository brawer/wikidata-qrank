// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"compress/flate"
	"context"
	"encoding/binary"
	"fmt"
	"os"
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
	uniform          map[uint32]tiffTile
	tempFile         *os.File
	dataSize         uint64
}

func NewRasterWriter() (*RasterWriter, error) {
	tempFile, err := os.CreateTemp("", "*.tmp")
	if err != nil {
		return nil, err
	}
	inChan := make(chan extsort.SortType, 10000)
	config := extsort.DefaultConfig()
	config.NumWorkers = runtime.NumCPU()
	sorter, outChan, errChan := extsort.New(inChan, tiffTileFromBytes, tiffTileLess, config)
	go sorter.Sort(context.Background())
	return &RasterWriter{
		tempFile:         tempFile,
		tiffTilesToSort:  inChan,
		tiffTiles:        outChan,
		tiffTilesSortErr: errChan,
		uniform:          make(map[uint32]tiffTile, 16),
	}, nil
}

func (w *RasterWriter) Write(r *Raster) error {
	// About 124K rasters are not strictly uniform, but they have only
	// marginal differences in color. For those, we can save the effort
	// of compression.
	uniform := true
	color := uint32(r.pixels[0] + 0.5)
	for i := 1; i < len(r.pixels); i++ {
		if uint32(r.pixels[i]+0.5) != color {
			uniform = false
			break
		}
	}
	if uniform {
		return w.WriteUniform(r.tile, color)
	}

	offset, byteCount, err := w.compress(r.tile, r.pixels[:])
	if err != nil {
		return err
	}

	var t tiffTile
	t.zoom, t.x, t.y = r.tile.ZoomXY()
	t.offset, t.byteCount = offset, byteCount
	w.tiffTilesToSort <- t
	return nil
}

// WriteUniform produces a raster whose pixels all have the same color.
// In a typical output, about 55% of all rasters are uniformly colored,
// so we treat them specially as an optimization.
func (w *RasterWriter) WriteUniform(tile TileKey, color uint32) error {
	var t tiffTile
	zoom, x, y := tile.ZoomXY()
	t.zoom = zoom
	t.x = x
	t.y = y
	if same, exists := w.uniform[color]; exists {
		t.offset = same.offset
		t.byteCount = same.byteCount
		w.tiffTilesToSort <- t
		return nil
	}
	var pixels [256 * 256]float32
	for i := 0; i < 256*256; i++ {
		pixels[i] = float32(color)
	}
	offset, len, err := w.compress(tile, pixels[:])
	if err != nil {
		return err
	}
	t.offset, t.byteCount = offset, len
	w.uniform[color] = t
	w.tiffTilesToSort <- t
	return nil
}

func (w *RasterWriter) compress(tile TileKey, pixels []float32) (offset uint64, size uint32, err error) {
	var buf bytes.Buffer
	buf.Grow(len(pixels) * 4)
	if err := binary.Write(&buf, binary.LittleEndian, pixels); err != nil {
		return 0, 0, err
	}

	var compressed bytes.Buffer
	writer, err := flate.NewWriter(&compressed, flate.BestCompression)
	if err != nil {
		return 0, 0, err
	}

	if _, err := writer.Write(buf.Bytes()); err != nil {
		return 0, 0, err
	}

	if err := writer.Close(); err != nil {
		return 0, 0, err
	}

	len, err := w.tempFile.Write(compressed.Bytes())
	if err != nil {
		return 0, 0, err
	}

	offset = w.dataSize
	w.dataSize += uint64(len)
	return offset, uint32(len), nil
}

func (w *RasterWriter) Close() error {
	close(w.tiffTilesToSort)
	for _ = range w.tiffTiles {
		// fmt.Printf("TODO: Store %v\n", t)
	}
	if err := <-w.tiffTilesSortErr; err != nil {
		return err
	}

	// Delete the temporary file for compressed data of TIFF tiles.
	tempFileName := w.tempFile.Name()
	if err := w.tempFile.Close(); err != nil {
		return err
	}
	if err := os.Remove(tempFileName); err != nil {
		return err
	}

	fmt.Printf("Total data size: %d\n", w.dataSize)
	return nil
}

// tiffTile represents a raster tile that will be written into
// a Cloud-Optimized GeoTIFF file. The file format requires
// a specific arrangement of the data, which is different from
// the order in which we’re painting our raster tiles.
type tiffTile struct {
	zoom      uint8
	x, y      uint32
	byteCount uint32
	offset    uint64
}

// ToBytes serializes a tiffTile into a byte array.
func (c tiffTile) ToBytes() []byte {
	var buf [1 + 3*binary.MaxVarintLen32 + binary.MaxVarintLen64]byte
	buf[0] = c.zoom
	pos := 1
	pos += binary.PutUvarint(buf[pos:], uint64(c.x))
	pos += binary.PutUvarint(buf[pos:], uint64(c.y))
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
	byteCount, len := binary.Uvarint(b[pos:])
	pos += len
	offset, len := binary.Uvarint(b[pos:])
	pos += len
	return tiffTile{
		zoom:      zoom,
		x:         uint32(x),
		y:         uint32(y),
		byteCount: uint32(byteCount),
		offset:    offset,
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
