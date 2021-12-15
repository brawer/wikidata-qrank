// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
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
	path         string
	tempFile     *os.File
	tempFileSize uint64
	dataSize     uint64
	zoom         uint8

	// For each zoom level, tileOffsets is the position of the TileOffset
	// relative to the start of the temporary file. In the final output,
	// we need to group together the tiles from the same zoom level.
	tileOffsets    [][]uint32
	tileByteCounts [][]uint32
	uniformTiles   []map[uint32]int
}

func NewRasterWriter(path string, zoom uint8) (*RasterWriter, error) {
	tempFile, err := os.CreateTemp("", "*.tmp")
	if err != nil {
		return nil, err
	}

	r := &RasterWriter{
		path:           path,
		tempFile:       tempFile,
		zoom:           zoom,
		tileOffsets:    make([][]uint32, zoom+1),
		tileByteCounts: make([][]uint32, zoom+1),
		uniformTiles:   make([]map[uint32]int, zoom+1),
	}
	for z := uint8(0); z <= zoom; z++ {
		r.tileOffsets[z] = make([]uint32, 1<<(2*z))
		r.tileByteCounts[z] = make([]uint32, 1<<(2*z))
		r.uniformTiles[z] = make(map[uint32]int, 16)
	}
	return r, nil
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

	offset, size, err := w.compress(r.tile, r.pixels[:])
	if err != nil {
		return err
	}

	zoom, x, y := r.tile.ZoomXY()
	tileIndex := (1<<zoom)*y + x
	w.tileOffsets[zoom][tileIndex] = uint32(offset)
	w.tileByteCounts[zoom][tileIndex] = size

	return nil
}

// WriteUniform produces a raster whose pixels all have the same color.
// In a typical output, about 55% of all rasters are uniformly colored,
// so we treat them specially as an optimization.
func (w *RasterWriter) WriteUniform(tile TileKey, color uint32) error {
	zoom, x, y := tile.ZoomXY()
	tileIndex := (1<<zoom)*y + x
	if same, exists := w.uniformTiles[zoom][color]; exists {
		w.tileOffsets[zoom][tileIndex] = w.tileOffsets[zoom][same]
		w.tileByteCounts[zoom][tileIndex] = w.tileByteCounts[zoom][same]
		return nil
	}
	var pixels [256 * 256]float32
	for i := 0; i < len(pixels); i++ {
		pixels[i] = float32(color)
	}
	offset, size, err := w.compress(tile, pixels[:])
	if err != nil {
		return err
	}
	w.tileOffsets[zoom][tileIndex] = uint32(offset)
	w.tileByteCounts[zoom][tileIndex] = size
	w.uniformTiles[zoom][color] = int(tileIndex)
	return nil
}

func (w *RasterWriter) compress(tile TileKey, pixels []float32) (offset uint64, size uint32, err error) {
	var buf bytes.Buffer
	buf.Grow(len(pixels) * 4)
	if err := binary.Write(&buf, binary.LittleEndian, pixels); err != nil {
		return 0, 0, err
	}

	//var compressed bytes.Buffer
	//writer, err := zlib.NewWriterLevel(&compressed, zlib.BestCompression)
	//if err != nil {
	//	return 0, 0, err
	//}
	//
	//if _, err := writer.Write(testImage[:]); err != nil {
	//	return 0, 0, err
	//}
	//
	//if err := writer.Close(); err != nil {
	//	return 0, 0, err
	//}

	n := buf.Len()
	if _, err := buf.WriteTo(w.tempFile); err != nil {
		return 0, 0, err
	}

	//n, err := compressed.WriteTo(w.tempFile)
	//if err != nil {
	//	return 0, 0, err
	//}

	offset = w.tempFileSize
	size = uint32(n)
	w.tempFileSize += uint64(size)

	return offset, size, nil
}

func (w *RasterWriter) Close() error {
	out, err := os.Create(w.path + ".tmp")
	if err != nil {
		return err
	}
	if err := w.writeTiff(out); err != nil {
		return err
	}
	if err := out.Sync(); err != nil {
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	if err := os.Rename(w.path+".tmp", w.path); err != nil {
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

	return nil
}

func (w *RasterWriter) writeTiff(out *os.File) error {
	// Magic header for little-endian TIFF files, followed by offset
	// to first Image File Directory in the file.
	magic := []byte{'I', 'I', 42, 0, 8, 0, 0, 0}
	if _, err := out.Write(magic); err != nil {
		return err
	}
	fileSize := uint32(len(magic))

	const (
		imageWidth       = 256
		imageHeight      = 257
		bitsPerSample    = 258
		compression      = 259
		photometric      = 262
		imageDescription = 270
		samplesPerPixel  = 277
		planarConfig     = 284
		software         = 305
		tileWidth        = 322
		tileLength       = 323
		tileOffsets      = 324
		tileByteCounts   = 325
		sampleFormat     = 339

		asciiFormat = 2
		shortFormat = 3
		longFormat  = 4
	)

	// TODO: To emit overviews, make this a loop over multiple zooms,
	// and patch the IFD offsets so they form a linked list in the TIFF file.
	// In a cloud-optimized GeoTIFF file, the IFDs have to be sorted
	// from deepest to coarsest zoom level.
	numTiles := uint32(1 << (w.zoom * 2))
	ifd := []struct {
		tag uint16
		val uint32
	}{
		{imageWidth, 1 << (w.zoom + 8)},
		{imageHeight, 1 << (w.zoom + 8)},
		{bitsPerSample, 32},
		{compression, 1}, // 1 = no compression; 8 = zlib/flate
		{photometric, 1}, // 1 = BlackIsZero
		{imageDescription, 0},
		{samplesPerPixel, 1},
		{planarConfig, 1},
		{software, 0},
		{tileWidth, 256},
		{tileLength, 256},
		{tileOffsets, 0},
		{tileByteCounts, 0},
		{sampleFormat, 3}, // 3 = IEEE floating point, TIFF spec page 80
	}

	// Position of extra data that does not fit inline in Image File Directory,
	// relative to start of TIFF file.
	tileOffsetsPos := fileSize + uint32(len(ifd)*12+6)
	tileByteCountsPos := tileOffsetsPos + uint32(numTiles*4)
	extraPos := tileByteCountsPos + uint32(numTiles*4)

	var buf, extraBuf bytes.Buffer
	if err := binary.Write(&buf, binary.LittleEndian, uint16(len(ifd))); err != nil {
		return err
	}

	lastTag := uint16(0)
	for _, e := range ifd {
		// Sanity check that our tags appear in the Image File Directory
		// in increasing order, as required by the TIFF specification.
		if e.tag <= lastTag {
			panic("TIFF tags must be in increasing order")
		}
		lastTag = e.tag

		if err := binary.Write(&buf, binary.LittleEndian, e.tag); err != nil {
			return err
		}
		var typ uint16
		var count, value uint32
		switch e.tag {
		case imageDescription:
			s := []byte("OpenStreetMap view density, in weekly user views per km2\u0000")
			typ, count, value = asciiFormat, uint32(len(s)), extraPos+uint32(extraBuf.Len())
			extraBuf.Write(s)

		case software:
			s := []byte("TileRank\u0000")
			typ, count, value = asciiFormat, uint32(len(s)), extraPos+uint32(extraBuf.Len())
			extraBuf.Write(s)

		case tileOffsets:
			typ, count, value = longFormat, numTiles, tileOffsetsPos

		case tileByteCounts:
			typ, count, value = longFormat, numTiles, tileByteCountsPos

		default:
			typ, count, value = longFormat, uint32(1), e.val
			if e.val <= 0xffff {
				typ = shortFormat
			}
		}
		if err := binary.Write(&buf, binary.LittleEndian, typ); err != nil {
			return err
		}
		if err := binary.Write(&buf, binary.LittleEndian, count); err != nil {
			return err
		}
		if err := binary.Write(&buf, binary.LittleEndian, value); err != nil {
			return err
		}
	}

	nextIFD := uint32(0)
	if err := binary.Write(&buf, binary.LittleEndian, nextIFD); err != nil {
		return err
	}

	if _, err := out.Write(buf.Bytes()); err != nil {
		return err
	}
	fileSize += uint32(buf.Len())

	// Reserve space for tileOffsets. We will overwrite tileOffsets
	// later, when writing out the actual data, since only then we’ll
	// know the actual offsets in the file.
	if fileSize != tileOffsetsPos {
		panic("fileSize != tileOffsetsPos")
	}
	numRows := 1 << w.zoom
	emptyRow := make([]byte, numRows*4)
	for y := 0; y < numRows; y++ {
		if _, err := out.Write(emptyRow); err != nil {
			return err
		}
	}
	fileSize += uint32(numTiles * 4)

	// Write tileByteCounts. Unlike tileOffsets, we already know the
	// final byte counts because the tile size is the same as in the
	// temporary file.
	if fileSize != tileByteCountsPos {
		panic("fileSize != tileByteCountsPos")
	}

	if err := binary.Write(out, binary.LittleEndian, w.tileByteCounts[w.zoom]); err != nil {
		return err
	}
	fileSize += uint32(len(w.tileByteCounts[w.zoom]) * 4)

	// Add padding to extraBuf so its length is a multiple of four.
	if pad := (4 - extraBuf.Len()%4) % 4; pad > 0 {
		if _, err := extraBuf.Write([]byte{0, 0, 0, 0}[:pad]); err != nil {
			return err
		}
	}
	if fileSize != extraPos {
		panic("fileSize != extraPos")
	}

	fileSize += uint32(extraBuf.Len())
	if _, err := extraBuf.WriteTo(out); err != nil {
		return err
	}

	uniform := make(map[uint32]bool, len(w.uniformTiles[w.zoom]))
	uniformPos := make(map[uint32]uint32, len(w.uniformTiles[w.zoom]))
	for _, t := range w.uniformTiles[w.zoom] {
		uniform[w.tileOffsets[w.zoom][t]] = true
	}

	finalTileOffsets := make([]uint32, numTiles)
	for tile := uint32(0); tile < numTiles; tile++ {
		tileOffset := w.tileOffsets[w.zoom][tile]
		if unipos, exists := uniformPos[tileOffset]; !exists {
			tileData := make([]byte, w.tileByteCounts[w.zoom][tile])
			if _, err := w.tempFile.ReadAt(tileData, int64(tileOffset)); err != nil {
				return err
			}
			finalTileOffsets[tile] = fileSize
			if uniform[tileOffset] {
				uniformPos[tileOffset] = fileSize
			}
			if _, err := out.Write(tileData); err != nil {
				return err
			}
			fileSize += uint32(len(tileData))
		} else {
			finalTileOffsets[tile] = unipos
		}
	}

	if _, err := out.Seek(int64(tileOffsetsPos), 0); err != nil {
		return err
	}
	if err := binary.Write(out, binary.LittleEndian, finalTileOffsets); err != nil {
		return err
	}

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
