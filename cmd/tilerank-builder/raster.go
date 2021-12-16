// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"fmt"
	"io"
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

	// For each zoom level, tileOffsetsPos is the position of the pointer
	// to the tileOffsets array within the Image File Directory,
	// relative to the start of the final output TIFF file.
	tileOffsetsPos []uint32
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
		tileOffsetsPos: make([]uint32, zoom+1),
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
	var compressed bytes.Buffer
	writer, err := zlib.NewWriterLevel(&compressed, zlib.BestCompression)
	if err != nil {
		return 0, 0, err
	}

	if err := binary.Write(writer, binary.LittleEndian, pixels); err != nil {
		return 0, 0, err
	}

	if err := writer.Close(); err != nil {
		return 0, 0, err
	}

	n, err := compressed.WriteTo(w.tempFile)
	if err != nil {
		return 0, 0, err
	}

	offset = w.tempFileSize
	w.tempFileSize += uint64(n)
	return offset, uint32(n), nil
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

		geoKeyDirectory = 34735
		geoAsciiParams  = 34737

		asciiFormat = 2
		shortFormat = 3
		longFormat  = 4
	)

	// The following was done by analyzing the hex dump of this command:
	// $ gdal_translate -a_srs EPSG:3857 image.tif geotiff.tif
	geoAscii := "WGS 84 / Pseudo-Mercator|WGS 84|\u0000"
	geoKeys := []uint16{
		1, 1, 0, // Version: 1.1.0
		7,             // NumberOfKeys: 7
		1024, 0, 1, 1, // GTModelType: 2D projected
		1025, 0, 1, 1, // GTRasterTyp: PixelIsArea
		1026, geoAsciiParams, 25, 0, // GTCitation: "WGS 84 / Pseudo-Mercator"
		2049, geoAsciiParams, 7, 25, // GeodeticCitation: "WGS 84"
		2054, 0, 1, 9102, // GeogAngularUnits: degree [EPSG unit 9102]
		3072, 0, 1, 3857, // ProjectedCRS: Web Mercator [epsg.io/3857]
		3076, 0, 1, 9001, // ProjLinearUnits: meter [EPSG unit 9001]
	}

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
		{compression, 8}, // 1 = no compression; 8 = zlib/flate
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
		{geoKeyDirectory, 0},
		{geoAsciiParams, 0},
	}

	// Position of extra data that does not fit inline in Image File Directory,
	// relative to start of TIFF file.
	w.tileOffsetsPos[w.zoom] = fileSize + uint32(len(ifd)*12+6)
	tileByteCountsPos := w.tileOffsetsPos[w.zoom] + uint32(numTiles*4)
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

		case geoKeyDirectory:
			typ, count, value = shortFormat, uint32(len(geoKeys)), extraPos+uint32(extraBuf.Len())
			binary.Write(&extraBuf, binary.LittleEndian, geoKeys)

		case geoAsciiParams:
			s := []byte(geoAscii)
			typ, count, value = asciiFormat, uint32(len(s)), extraPos+uint32(extraBuf.Len())
			extraBuf.Write(s)

		case tileOffsets:
			typ, count, value = longFormat, numTiles, w.tileOffsetsPos[w.zoom]

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
	if fileSize != w.tileOffsetsPos[w.zoom] {
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

	if err := w.writeTiles(w.zoom, out); err != nil {
		return err
	}

	return nil
}

// writeTiles writes the tile data for a zoom level to the output TIFF file.
// For each written tile, its offset within the TIFF file is stored into
// the TileOffsets array in the zoom level’s Image File Directory.
func (w *RasterWriter) writeTiles(zoom uint8, f *os.File) error {
	fileSize, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	numTiles := uint32(1 << (zoom * 2))

	// w.uniformTiles[zoom] maps a pixel color to the index,
	// in TileOffsets and TileByteCounts, of a compressed tile
	// that has this same color uniformly across all its pixels.
	// Sharing tile data for uniform tiles saves a lot of space,
	// so of course we want to do this tile data sharing also in
	// our final output, not just in the temporary file.
	//
	// uniform[t] is true if the data at offset t in the temp file
	// is for a uniform raster whose data is shared by multiple tiles.
	// This array gets populated before entering the loop.
	//
	// uniformPos[t] indicates the position of the shared uniform
	// tile data (whose data starts at offset t in the temporary file)
	// in the final output TIFF file. This array gets populated
	// when actually writing the output to the output TIFF.
	uniform := make(map[uint32]bool, len(w.uniformTiles[zoom]))
	uniformPos := make(map[uint32]uint32, len(w.uniformTiles[zoom]))
	for _, t := range w.uniformTiles[zoom] {
		uniform[w.tileOffsets[zoom][t]] = true
	}

	finalTileOffsets := make([]uint32, numTiles)
	for tile := uint32(0); tile < numTiles; tile++ {
		tileOffset := w.tileOffsets[zoom][tile]
		if unipos, exists := uniformPos[tileOffset]; !exists {
			// TODO: Write tile data leaders and trailers, like GDAL.
			// https://gdal.org/drivers/raster/cog.html#tile-data-leader-and-trailer
			tileData := make([]byte, w.tileByteCounts[zoom][tile])
			if _, err := w.tempFile.ReadAt(tileData, int64(tileOffset)); err != nil {
				return err
			}
			finalTileOffsets[tile] = uint32(fileSize)
			if uniform[tileOffset] {
				uniformPos[tileOffset] = uint32(fileSize)
			}
			if _, err := f.Write(tileData); err != nil {
				return err
			}
			fileSize += int64(len(tileData))
		} else {
			finalTileOffsets[tile] = unipos
		}
	}

	if _, err := f.Seek(int64(w.tileOffsetsPos[zoom]), 0); err != nil {
		return err
	}

	if err := binary.Write(f, binary.LittleEndian, finalTileOffsets); err != nil {
		return err
	}

	return nil
}
