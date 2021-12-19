// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
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
	tileOffsetsPos    []int64
	tileByteCountsPos []int64
}

func NewRasterWriter(path string, zoom uint8) (*RasterWriter, error) {
	tempFile, err := os.CreateTemp("", "*.tmp")
	if err != nil {
		return nil, err
	}

	r := &RasterWriter{
		path:              path,
		tempFile:          tempFile,
		zoom:              zoom,
		tileOffsets:       make([][]uint32, zoom+1),
		tileByteCounts:    make([][]uint32, zoom+1),
		uniformTiles:      make([]map[uint32]int, zoom+1),
		tileOffsetsPos:    make([]int64, zoom+1),
		tileByteCountsPos: make([]int64, zoom+1),
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
	// Magic header for a little-endian TIFF file that is smaller than 4GiB.
	// Our output is “only” a few hundred megabytes, so we do not need to
	// care about BigTIFF.
	magic := []byte{'I', 'I', 42, 0}
	if _, err := out.Write(magic); err != nil {
		return err
	}

	// Structural Metadata for GDAL and compatible readers.
	// https://gdal.org/drivers/raster/cog.html#header-ghost-area
	//
	// Note that we do **not** use BLOCK_ORDER=ROW_MAJOR because
	// that would mean we couldn’t share tile data among tiles.
	// In our output image, we have very many tiles with uniform
	// color across all pixels, and sharing this data between
	// all tiles using them saves a lot of space.
	smd := `LAYOUT=IFDS_BEFORE_DATA
BLOCK_LEADER=SIZE_AS_UINT4
BLOCK_TRAILER=LAST_4_BYTES_REPEATED
KNOWN_INCOMPATIBLE_EDITION=NO 
`
	if !strings.Contains(smd, "=NO \n") {
		panic("missing space after NO") // as per GDAL documentation
	}

	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("GDAL_STRUCTURAL_METADATA_SIZE=%06d bytes\n", len(smd)))
	buf.WriteString(smd)
	if err := addPadding(&buf); err != nil {
		return err
	}

	// Offset to first Image File Directory in file.
	firstIFDPos := uint32(len(magic) + 4 + buf.Len())
	if err := binary.Write(out, binary.LittleEndian, firstIFDPos); err != nil {
		return err
	}

	if _, err := out.Write(buf.Bytes()); err != nil {
		return err
	}

	// TODO: Also write overview zoom levels, not just deepest zoom.
	if _, err := w.writeIFD(w.zoom, out); err != nil {
		return err
	}

	for zoom := uint8(0); zoom <= w.zoom; zoom++ {
		if err := w.writeTiles(zoom, out); err != nil {
			return err
		}
	}

	// TileByteCounts at the end, as per Cloud-Optimized GeoTIFF discussion:
	// https://github.com/cogeotiff/cog-spec/issues/5#issuecomment-996511137
	for zoom := uint8(0); zoom <= w.zoom; zoom++ {
		if err := w.writeTileByteCounts(zoom, out); err != nil {
			return err
		}
	}

	return nil
}

func (w *RasterWriter) writeIFD(zoom uint8, f *os.File) (int64, error) {
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

	fileSize, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}

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

	numTiles := uint32(1 << (zoom * 2))
	type ifdEntry struct {
		tag uint16
		val uint32
	}
	ifd := []ifdEntry{
		{imageWidth, 1 << (zoom + 8)},
		{imageHeight, 1 << (zoom + 8)},
		{bitsPerSample, 32},
		{compression, 8}, // 1 = no compression; 8 = zlib/flate
		{photometric, 0}, // 0 = WhiteIsZero
		{samplesPerPixel, 1},
		{planarConfig, 1},
		{tileWidth, 256},
		{tileLength, 256},
		{tileOffsets, 0},
		{tileByteCounts, 0},
		{sampleFormat, 3}, // 3 = IEEE floating point, TIFF spec page 80
	}

	// Some TIFF tags are only used on the main (highest resolution) image.
	if zoom == w.zoom {
		ifd = append(ifd, ifdEntry{imageDescription, 0})
		ifd = append(ifd, ifdEntry{software, 0})
		ifd = append(ifd, ifdEntry{geoKeyDirectory, 0})
		ifd = append(ifd, ifdEntry{geoAsciiParams, 0})
	}

	sort.Slice(ifd, func(i, j int) bool { return ifd[i].tag < ifd[j].tag })

	// Position of extra data that does not fit inline in Image File Directory,
	// relative to start of TIFF file.
	extraPos := fileSize + int64(2+len(ifd)*12+4)

	var buf, extraBuf bytes.Buffer
	if err := binary.Write(&buf, binary.LittleEndian, uint16(len(ifd))); err != nil {
		return 0, err
	}

	lastTag := uint16(0)
	for i, e := range ifd {
		// Compute the position of the currently written IFD entry,
		// relative to the start of the output TIFF file.
		ifdEntryPos := fileSize + int64(2+i*12) // 2 bytes for number of entries

		// Sanity check that our tags appear in the Image File Directory
		// in increasing order, as required by the TIFF specification.
		if e.tag <= lastTag {
			panic("TIFF tags must be in increasing order")
		}
		lastTag = e.tag

		if err := binary.Write(&buf, binary.LittleEndian, e.tag); err != nil {
			return 0, err
		}
		var typ uint16
		var count, value uint32
		switch e.tag {
		case imageDescription:
			s := []byte("OpenStreetMap view density, in weekly user views per km2\u0000")
			typ, count, value = asciiFormat, uint32(len(s)), uint32(extraPos)+uint32(extraBuf.Len())
			if _, err := extraBuf.Write(s); err != nil {
				return 0, err
			}

		case software:
			s := []byte("TileRank\u0000")
			typ, count, value = asciiFormat, uint32(len(s)), uint32(extraPos)+uint32(extraBuf.Len())
			extraBuf.Write(s)

		case geoKeyDirectory:
			typ, count, value = shortFormat, uint32(len(geoKeys)), uint32(extraPos)+uint32(extraBuf.Len())
			if err := binary.Write(&extraBuf, binary.LittleEndian, geoKeys); err != nil {
				return 0, err
			}

		case geoAsciiParams:
			s := []byte(geoAscii)
			typ, count, value = asciiFormat, uint32(len(s)), uint32(extraPos)+uint32(extraBuf.Len())
			if _, err := extraBuf.Write(s); err != nil {
				return 0, err
			}

		case tileOffsets:
			typ, count, value = longFormat, numTiles, 0xdeadbeef
			w.tileOffsetsPos[zoom] = ifdEntryPos + 8

		case tileByteCounts:
			typ, count, value = longFormat, numTiles, 0xdeadbeef
			w.tileByteCountsPos[zoom] = ifdEntryPos + 8

		default:
			typ, count, value = longFormat, uint32(1), e.val
			if e.val <= 0xffff {
				typ = shortFormat
			}
		}
		if err := binary.Write(&buf, binary.LittleEndian, typ); err != nil {
			return 0, err
		}
		if err := binary.Write(&buf, binary.LittleEndian, count); err != nil {
			return 0, err
		}
		if err := binary.Write(&buf, binary.LittleEndian, value); err != nil {
			return 0, err
		}
	}

	nextIFD := uint32(0)
	nextIFDPos := int64(fileSize) + int64(buf.Len())
	if err := binary.Write(&buf, binary.LittleEndian, nextIFD); err != nil {
		return 0, err
	}

	if _, err := f.Write(buf.Bytes()); err != nil {
		return 0, err
	}
	fileSize += int64(buf.Len())

	if err := addPadding(&extraBuf); err != nil {
		return 0, err
	}
	if fileSize != extraPos {
		panic("fileSize != extraPos")
	}

	fileSize += int64(extraBuf.Len())
	if _, err := extraBuf.WriteTo(f); err != nil {
		return 0, err
	}

	return nextIFDPos, nil
}

// writeTiles writes the tile data for a zoom level to the output TIFF file.
// For each written tile, its offset within the TIFF file is stored into
// the TileOffsets array in the zoom level’s Image File Directory.
func (w *RasterWriter) writeTiles(zoom uint8, f *os.File) error {
	// Only write byte counts for a zoom level if we have previously written
	// an Image File Directory.
	if w.tileByteCountsPos[zoom] == 0 {
		return nil
	}

	fileSize, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	numTiles := uint32(1 << (zoom * 2))

	// Reserve space for tileOffsets. We will overwrite tileOffsets below,
	// once we know the actual offset of each tile.
	tileOffsetsPos := fileSize
	numRows := 1 << zoom
	emptyRow := make([]byte, numRows*4)
	for y := 0; y < numRows; y++ {
		if _, err := f.Write(emptyRow); err != nil {
			return err
		}
	}
	fileSize += int64(numTiles * 4)

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
		tileOffset := w.tileOffsets[zoom][tile] // offset in temp file
		if unipos, exists := uniformPos[tileOffset]; !exists {
			// Copy tile data into the final TIFF file. We also write
			// a “tile data leader” and “trailer”, like GDAL does.
			// https://gdal.org/drivers/raster/cog.html#tile-data-leader-and-trailer
			tileSize := w.tileByteCounts[zoom][tile]
			data := make([]byte, tileSize+8)
			payload := data[4 : 4+tileSize]
			if _, err := w.tempFile.ReadAt(payload, int64(tileOffset)); err != nil {
				return err
			}

			// The “tile data leader” for tile t is four bytes containing
			// TileByteCount[t] in little-endian encoding, stored in the
			// TIFF file at position TileOffsets[i] - 4.  With this,
			// clients do not need to access the TileByteCounts array
			// for reading a tile. This can speed up reading TIFF files
			// over a network. (The GDAL documentation does not make it
			// clear whether the leader is always in little-encoding,
			// or whether it’s matching the encoding of the TIFF file.
			// Since we always write our output in little-encoding,
			// even when we’re running on a big-endian machine, it does
			// not matter for us).
			var leader bytes.Buffer
			if err := binary.Write(&leader, binary.LittleEndian, uint32(tileSize)); err != nil {
				return err
			}
			copy(data[0:4], leader.Bytes())

			// The “tile data trailer” is four bytes that repeat the last
			// four bytes of the compressed tile data. In the TIFF file,
			// it gets stored immediately after the tile data. Clients use
			// this trailer to detect file changes by software that does
			// not support the “tile data leader and trailer” convention.
			copy(data[len(data)-4:], payload[len(payload)-4:])

			finalTileOffset := uint32(fileSize) + 4
			finalTileOffsets[tile] = finalTileOffset
			if uniform[tileOffset] {
				uniformPos[tileOffset] = finalTileOffset
			}
			if _, err := f.Write(data); err != nil {
				return err
			}
			fileSize += int64(len(data))
		} else {
			finalTileOffsets[tile] = unipos
		}
	}

	if len(finalTileOffsets) == 1 {
		if _, err := f.Seek(w.tileOffsetsPos[zoom], io.SeekStart); err != nil {
			return err
		}
		if err := binary.Write(f, binary.LittleEndian, finalTileOffsets[0]); err != nil {
			return err
		}

		return nil
	}

	if _, err := f.Seek(tileOffsetsPos, io.SeekStart); err != nil {
		return err
	}

	if err := binary.Write(f, binary.LittleEndian, finalTileOffsets); err != nil {
		return err
	}

	// Patch up the Image File Directory so its TileOffsets entry points
	// to the freshly written TileOffsets array.
	if err := patchOffset(f, w.tileOffsetsPos[zoom], tileOffsetsPos); err != nil {
		return err
	}

	return nil
}

// writeTileByteCounts stores the TileByteCounts array into the output TIFF.
func (w *RasterWriter) writeTileByteCounts(zoom uint8, f io.WriteSeeker) error {
	pos, sizes := w.tileByteCountsPos[zoom], w.tileByteCounts[zoom]

	// Only write byte counts for a zoom level if we have previously written
	// an Image File Directory.
	if pos == 0 {
		return nil
	}

	// If the TileByteCounts array has just one single entry, it fits into
	// the Image File Directory and _has_ to be inlined (as per TIFF spec).
	if len(sizes) == 1 {
		if _, err := f.Seek(pos, io.SeekStart); err != nil {
			return err
		}
		if err := binary.Write(f, binary.LittleEndian, sizes); err != nil {
			return err
		}
		return nil
	}

	arrayPos, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	if err := binary.Write(f, binary.LittleEndian, sizes); err != nil {
		return err
	}

	if err := patchOffset(f, pos, arrayPos); err != nil {
		return err
	}

	return nil
}

// addPadding writes zero bytes to buf to make its length a multiple of two.
func addPadding(buf *bytes.Buffer) error {
	if buf.Len()&1 != 0 {
		if err := buf.WriteByte(0); err != nil {
			return err
		}
	}
	return nil
}

func patchOffset(f io.WriteSeeker, pos int64, value int64) error {
	if value < 0 || value > 0xffffffff {
		// If this triggers, there probably is a bug in the code that has
		// calculatied the passed value. If we really had to deal with
		// file offsets above 2^32, we could implement BigTIFF, but this
		// seems rather unlikely because our data size is not that large.
		panic("offset value out of range")
	}

	if _, err := f.Seek(pos, io.SeekStart); err != nil {
		return err
	}

	if err := binary.Write(f, binary.LittleEndian, uint32(value)); err != nil {
		return err
	}

	return nil
}
