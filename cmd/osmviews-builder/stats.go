// SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"sort"

	"github.com/fogleman/gg"
)

func BuildStats(tiffPath, statsPath, plotPath string) error {
	f, err := os.Open(tiffPath)
	if err != nil {
		return err
	}
	defer f.Close()

	t, err := NewTiffReader(f)
	if err != nil {
		return err
	}

	hist, err := buildHistogram(t)
	if err != nil {
		return err
	}

	stats, err := calcStats(hist)
	if err != nil {
		return err
	}

	if err := stats.Plot(plotPath); err != nil {
		return err
	}

	j, err := json.Marshal(stats)
	if err != nil {
		return err
	}

	tmpStatsPath := statsPath + ".tmp"
	statsFile, err := os.Create(tmpStatsPath)
	if err != nil {
		return err
	}
	defer statsFile.Close()

	if _, err := statsFile.Write(j); err != nil {
		return err
	}
	if err := statsFile.Sync(); err != nil {
		return err
	}
	if err := statsFile.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpStatsPath, statsPath); err != nil {
		return err
	}

	return nil
}

type Sample []interface{} // [[Lat, Lng], Rank, Value]

type Stats struct {
	Median  int
	Samples []Sample
}

type TiffReader struct {
	r                                              io.ReadSeeker
	order                                          binary.ByteOrder
	imageWidth, imageHeight, tileWidth, tileHeight uint32
	tileOffsets, tileByteCounts                    []uint32
}

func NewTiffReader(r io.ReadSeeker) (*TiffReader, error) {
	tr := &TiffReader{r: r}
	if err := tr.readFirstIFD(); err != nil {
		return nil, err
	}
	return tr, nil
}

func (t *TiffReader) readFirstIFD() error {
	var header [4]byte
	if _, err := t.r.Read(header[:]); err != nil {
		return err
	}

	// We only need to decode our own files, which are never big-endian.
	if bytes.Equal(header[:], []byte{'I', 'I', 42, 0}) {
		t.order = binary.LittleEndian
	} else if bytes.Equal(header[:], []byte{'M', 'M', 0, 42}) {
		t.order = binary.BigEndian
	} else {
		return fmt.Errorf("unsupported format")
	}

	var ifdOffset uint32
	if err := binary.Read(t.r, t.order, &ifdOffset); err != nil {
		return err
	}
	if _, err := t.r.Seek(int64(ifdOffset), os.SEEK_SET); err != nil {
		return err
	}

	var numDirEntries uint16
	if err := binary.Read(t.r, t.order, &numDirEntries); err != nil {
		return err
	}

	var ifd bytes.Buffer
	if _, err := io.CopyN(&ifd, t.r, int64(numDirEntries)*12); err != nil {
		return err
	}

	for i := uint16(0); i < numDirEntries; i++ {
		var tag, typ uint16
		var count, value uint32
		if err := binary.Read(&ifd, t.order, &tag); err != nil {
			return err
		}
		if err := binary.Read(&ifd, t.order, &typ); err != nil {
			return err
		}
		if err := binary.Read(&ifd, t.order, &count); err != nil {
			return err
		}
		switch typ {
		case 3: // SHORT
			var sval1, sval2 uint16
			if err := binary.Read(&ifd, t.order, &sval1); err != nil {
				return err
			}
			binary.Read(&ifd, t.order, &sval2)
			value = uint32(sval1)

		default: // LONG
			if err := binary.Read(&ifd, t.order, &value); err != nil {
				return err
			}
		}

		switch tag {
		case 256: // ImageWidth
			t.imageWidth = value

		case 257: // ImageLength
			t.imageHeight = value

		case 322: // TileWidth
			t.tileWidth = value

		case 323: // TileLength
			t.tileHeight = value

		case 324: // TileOffsets
			if a, err := t.readIntArray(typ, count, value); err == nil {
				t.tileOffsets = a
			} else {
				return err
			}

		case 325: // TileByteCounts
			if a, err := t.readIntArray(typ, count, value); err == nil {
				t.tileByteCounts = a
			} else {
				return err
			}
		}
	}

	return nil
}

func (t *TiffReader) readIntArray(typ uint16, count, value uint32) ([]uint32, error) {
	if typ != 4 {
		return nil, fmt.Errorf("got type=%d, want 4", typ)
	}

	if _, err := t.r.Seek(int64(value), os.SEEK_SET); err != nil {
		return nil, err
	}

	result := make([]uint32, count)
	if err := binary.Read(t.r, t.order, &result); err != nil {
		return nil, err
	}

	return result, nil
}

func (t *TiffReader) readTile(index TileIndex, data []float32) error {
	if _, err := t.r.Seek(int64(t.tileOffsets[index]), os.SEEK_SET); err != nil {
		return err
	}

	n := int64(t.tileByteCounts[index])
	var buf bytes.Buffer
	if _, err := io.CopyN(&buf, t.r, n); err != nil {
		return err
	}

	reader, err := zlib.NewReader(&buf)
	if err != nil {
		return err
	}

	if err := binary.Read(reader, t.order, &data); err != nil {
		return err
	}

	return nil
}

type TileIndex int

// SharedTile keeps information about a tile is used more than once.
// In our GeoTIFF, 93.1% of all tile offsets point to a shared tile.
// Usually these are patches of oceans or deserts into which no map user
// ever zooms deeply.
type SharedTile struct {
	UseCount    int         // Total number of tiles sharing this data.
	SampleTiles []TileIndex // A random sample of tiles that share this data.
}

type SharedTiles map[uint32]*SharedTile

func findSharedTiles(tileOffsets []uint32) SharedTiles {
	shared := make(SharedTiles, 20)     // 16 for GeoTIFF of 2022-01-24
	uses := make(map[uint32]int, 80000) // 72138 for TIFF of 2022-01-24
	for _, off := range tileOffsets {
		uses[off] += 1
	}

	for off, n := range uses {
		if n > 1 {
			r := SharedTile{UseCount: n, SampleTiles: make([]TileIndex, 2000)}
			for i := 0; i < len(r.SampleTiles); i++ {
				r.SampleTiles[i] = -1
			}
			shared[off] = &r
		}
	}

	stride := 1 << (math.Ilogb(float64(len(tileOffsets))) / 2)
	for _, y := range rand.Perm(stride) {
		for x := 0; x < stride; x++ {
			tile := TileIndex(y*stride + x)
			off := tileOffsets[tile]
			if r, ok := shared[off]; ok {
				key := int(tile) % len(r.SampleTiles)
				if r.SampleTiles[key] < 0 || rand.Intn(50) == 0 {
					r.SampleTiles[key] = tile
				}
			}
		}
	}

	// If any slots are left unused, remove them.
	for _, st := range shared {
		j := 0
		for i := 0; i < len(st.SampleTiles); i++ {
			if st.SampleTiles[i] >= 0 {
				st.SampleTiles[j] = st.SampleTiles[i]
				j++
			}
		}
		st.SampleTiles = st.SampleTiles[0:j]
	}

	return shared
}

func (s SharedTiles) Plot(dc *gg.Context, tileOffsets []uint32) {
	dc.SetRGB(1, 1, 1)
	dc.Clear()
	dc.SetRGB(0.8, 0.8, 1)
	stride := 1 << (math.Ilogb(float64(len(tileOffsets))) / 2)
	for ti, off := range tileOffsets {
		if s[off] != nil {
			dc.SetPixel(ti%stride, ti/stride)
		}
	}

	dc.SetRGB(0.6, 0.6, 1)
	for _, t := range s {
		for _, tile := range t.SampleTiles {
			tileX, tileY := int(tile)%stride, int(tile)/stride
			dc.DrawCircle(float64(tileX), float64(tileY), 1.5)
			dc.Fill()
		}
	}
}

type histogram struct {
	imageWidth, imageHeight int
	tileWidth, tileHeight   int
	stride, zoom            int
	tileWidthBits           int
	buckets                 map[uint64]Bucket
}

type BucketSample struct{ value, lat, lng float32 }

type Bucket struct {
	Count  int64
	Sample BucketSample
}

func newHistogram(imageWidth, imageHeight, tileWidth, tileHeight int) *histogram {
	h := &histogram{imageWidth: imageWidth, imageHeight: imageHeight, tileWidth: tileWidth, tileHeight: tileHeight}
	h.stride = (imageWidth + tileWidth - 1) / tileWidth
	h.zoom = math.Ilogb(float64(imageWidth))
	h.tileWidthBits = math.Ilogb(float64(tileWidth))
	h.buckets = make(map[uint64]Bucket, 250000) // 210037 for 2022-01-24 data
	return h
}

func (h *histogram) Add(data []float32, uses int, samples []TileIndex) {
	numSamples, numSamplesTaken := 2, 0
	for y := 0; y < h.tileHeight; y++ {
		pos := y * h.tileWidth
		for x := 0; x < h.tileWidth; x++ {
			val := data[pos]
			pos++
			key := uint64(val + 0.5)
			if b, ok := h.buckets[key]; ok && numSamplesTaken >= numSamples {
				// Frequent code path, taken 4.72 billion times.
				b.Count += int64(uses)
				h.buckets[key] = b
			} else {
				// Infrequent code path, taken 354 thousand times.
				count := h.buckets[key].Count + int64(uses)
				h.buckets[key] = h.makeBucket(val, count, samples[0], x, y)
				numSamplesTaken += 1
			}
		}
	}
}

func (h *histogram) makeBucket(val float32, count int64, tile TileIndex, x, y int) Bucket {
	tileX := int(tile) % h.stride
	pixelX := uint32(tileX<<h.tileWidthBits + x)
	lng := float32(pixelX)/float32(h.imageWidth)*360.0 - 180.0

	tileY := int(tile) / h.stride
	pixelY := uint32(tileY<<h.tileWidthBits + y)
	lat := float32(TileLatitude(uint8(h.zoom), pixelY) * (180 / math.Pi))

	return Bucket{count, BucketSample{val, lat, lng}}
}

func (h *histogram) Plot(dc *gg.Context, buckets []Bucket) {
	ctr := make(map[uint64]int)
	dc.SetRGB(1, 0, 0)
	z := uint8(h.zoom - h.tileWidthBits)
	var total int64
	for _, b := range buckets {
		x, y := TileFromLatLng(float64(b.Sample.lat), float64(b.Sample.lng), z)
		dc.DrawCircle(float64(x), float64(y), 3.0)
		dc.Fill()
		ctr[uint64(y)*1024+uint64(x)] += 1
		total += b.Count
	}
	fmt.Println("**** Number of unique lat/lng samples:", len(ctr))
}

func buildHistogram(t *TiffReader) ([]Bucket, error) {
	sharedTiles := findSharedTiles(t.tileOffsets)
	stride := 1 << (math.Ilogb(float64(len(t.tileOffsets))) / 2)
	hist := newHistogram(int(t.imageWidth), int(t.imageHeight), int(t.tileWidth), int(t.tileHeight))

	data := make([]float32, t.tileWidth*t.tileHeight)
	nn := 0
	for _, y := range rand.Perm(stride) {
		for _, x := range rand.Perm(stride) {
			ti := TileIndex(y*stride + x)
			off := t.tileOffsets[ti]
			if _, isShared := sharedTiles[off]; isShared {
				continue
			}
			// if nn > 8 { break }
			if err := t.readTile(ti, data); err != nil {
				return nil, err
			}
			hist.Add(data, 1, []TileIndex{ti})
			nn++
		}
	}

	buckets := make([]Bucket, 0, len(hist.buckets)+2000*len(sharedTiles))
	for _, h := range hist.buckets {
		buckets = append(buckets, h)
	}

	for _, st := range sharedTiles {
		if err := t.readTile(st.SampleTiles[0], data); err != nil {
			return nil, err
		}
		tileUses := int64(st.UseCount) * int64(len(data))
		for i, tile := range st.SampleTiles {
			count := tileUses / int64(len(st.SampleTiles))
			if i == 0 {
				count += tileUses % int64(len(st.SampleTiles))
			}
			buckets = append(buckets, hist.makeBucket(data[0], count, tile, 0, 0))
		}
	}

	sort.Slice(buckets, func(i, j int) bool {
		return buckets[i].Sample.value > buckets[j].Sample.value
	})

	return buckets, nil
}

func calcStats(hist []Bucket) (*Stats, error) {
	var maxVal float32
	var totalCount int64
	for _, h := range hist {
		if h.Sample.value > maxVal {
			maxVal = h.Sample.value
		}
		totalCount += h.Count
	}

	stats := &Stats{Samples: make([]Sample, 0, 1000)}
	rank := int64(1)
	scaleX := 1000.0 / math.Log10(float64(totalCount))
	scaleY := 1000.0 / math.Log10(float64(maxVal))
	var lastX, lastY float64
	for i, b := range hist {
		x := math.Max(math.Log10(float64(rank))*scaleX, 0)
		y := math.Max(math.Log10(float64(b.Sample.value))*scaleY, 0)
		distance := (x-lastX)*(x-lastX) + (y-lastY)*(y-lastY)
		isLast := i == len(hist)-1
		if isLast {
			rank = totalCount
		}
		if distance >= 16.0 || isLast {
			stats.Samples = append(stats.Samples, Sample{[]float32{b.Sample.lat, b.Sample.lng}, rank, b.Sample.value})
			lastX, lastY = x, y
			if stats.Median == 0 && rank >= totalCount/2 {
				stats.Median = len(stats.Samples) - 1
			}
		}
		rank += b.Count
	}

	return stats, nil
}

func (s *Stats) Plot(path string) error {
	firstValue := float64(s.Samples[0][2].(float32))
	lastRank := float64(s.Samples[len(s.Samples)-1][1].(int64))
	scaleX := 1000.0 / math.Log10(lastRank)
	scaleY := 1000.0 / math.Log10(firstValue)

	dc := gg.NewContext(1010, 1010)
	dc.SetRGB(1, 1, 1)
	dc.Clear()

	dc.SetRGB(0, 0.4, 1)
	dc.MoveTo(5, 5)
	for _, p := range s.Samples {
		x := math.Max(math.Log10(float64(p[1].(int64)))*scaleX, 0)
		y := math.Max(math.Log10(float64(p[2].(float32)))*scaleY, 0)
		dc.LineTo(x+5.0, 1000.0-y+5.0)
	}
	dc.Stroke()

	for _, p := range s.Samples {
		x := math.Max(math.Log10(float64(p[1].(int64)))*scaleX, 0)
		y := math.Max(math.Log10(float64(p[2].(float32)))*scaleY, 0)
		dc.DrawCircle(x+5.0, 1000.0-y+5.0, 4.0)
		dc.Fill()
	}

	dc.SetRGB(1, 0.4, 0.4)
	for _, p := range s.Samples {
		lat, lng := p[0].([]float32)[0], p[0].([]float32)[1]
		x, y := TileFromLatLng(float64(lat), float64(lng), 9)
		dc.DrawCircle(float64(x)+5.0, float64(y)+5+1000-512, 3.0)
		dc.Fill()
	}

	if err := dc.SavePNG(path); err != nil {
		return err
	}

	return nil
}
