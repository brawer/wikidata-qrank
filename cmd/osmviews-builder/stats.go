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

func buildStats(tiffPath, statsPath string) error {
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
		return nil
	}

	stats, err := calcStats(hist)
	if err != nil {
		return nil
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

type bucketSample struct{ value, lat, lng float32 }

type bucket struct {
	count  int64
	sample bucketSample
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
	buckets                 map[uint64]bucket2
	extraSamples            map[uint64][]bucketSample
}

type bucket2 struct {
	Count  int64
	Sample bucketSample
}

func newHistogram(imageWidth, imageHeight, tileWidth, tileHeight int) *histogram {
	h := &histogram{imageWidth: imageWidth, imageHeight: imageHeight, tileWidth: tileWidth, tileHeight: tileHeight}
	h.stride = (imageWidth + tileWidth - 1) / tileWidth
	h.zoom = math.Ilogb(float64(imageWidth))
	h.tileWidthBits = math.Ilogb(float64(tileWidth))
	h.buckets = make(map[uint64]bucket2, 250000) // 210037 for 2022-01-24 data
	h.extraSamples = make(map[uint64][]bucketSample, 50)
	for i := uint64(0); i < 10; i++ {
		h.extraSamples[i] = make([]bucketSample, 0, 1000)
	}
	return h
}

func (h *histogram) Add(data []float32, uses int, samples []TileIndex) {
	numSamples, numSamplesTaken := 2, 0
	for y := 0; y < h.tileHeight; y++ {
		pos := y * h.tileWidth
		for x := 0; x < h.tileWidth; x++ {
			val := data[pos]
			pos++
			key := uint64(val + 0.5) //+ 100
			if b, ok := h.buckets[key]; ok && numSamplesTaken >= numSamples {
				// Frequent code path, taken 4.7 billion times (without numSamples check); TODO times now.
				b.Count += int64(uses)
			} else {
				// Infrequent code path, taken 210 thousand times.
				// Not worth optimizing.
				tileX := int(samples[0]) % h.stride
				pixelX := uint32(tileX<<h.tileWidthBits + x)
				lng := float32(pixelX)/float32(h.imageWidth)*360.0 - 180.0

				tileY := int(samples[0]) / h.stride
				pixelY := uint32(tileY<<h.tileWidthBits + y)
				lat := float32(TileLatitude(uint8(h.zoom), pixelY) * (180 / math.Pi))

				count := h.buckets[key].Count + int64(uses)
				h.buckets[key] = bucket2{count, bucketSample{val, lat, lng}}
				numSamplesTaken += 1
			}
		}
	}
}

func (h *histogram) Plot(dc *gg.Context) {
	ctr := make(map[uint64]int)
	dc.SetRGB(1, 0, 0)
	z := uint8(h.zoom - h.tileWidthBits)
	for _, b := range h.buckets {
		x, y := TileFromLatLng(float64(b.Sample.lat), float64(b.Sample.lng), z)
		dc.DrawCircle(float64(x)+0.5, float64(y)+0.5, 4.0)
		dc.Fill()
		ctr[uint64(y)*1024+uint64(x)] += 1
	}
	fmt.Println("**** Number of unique lat/lng samples:", len(ctr))
}

func buildHistogram(t *TiffReader) ([]bucket2, error) {
	sharedTiles := findSharedTiles(t.tileOffsets)
	stride := 1 << (math.Ilogb(float64(len(t.tileOffsets))) / 2)
	h3 := newHistogram(int(t.imageWidth), int(t.imageHeight), int(t.tileWidth), int(t.tileHeight))

	data := make([]float32, t.tileWidth*t.tileHeight)
	nn := 0
	for _, y := range rand.Perm(stride) {
		for _, x := range rand.Perm(stride) {
			ti := TileIndex(y*stride + x)
			off := t.tileOffsets[ti]
			if _, isShared := sharedTiles[off]; isShared {
				continue
			}
			//if nn > 8 { break }
			if err := t.readTile(ti, data); err != nil {
				return nil, err
			}
			h3.Add(data, 1, []TileIndex{ti})
			nn++
		}
	}

	for _, st := range sharedTiles {
		if err := t.readTile(st.SampleTiles[0], data); err != nil {
			return nil, err
		}
		h3.Add(data, st.UseCount, st.SampleTiles)
	}

	buckets := make([]bucket2, 0, len(h3.buckets))
	for _, h := range h3.buckets {
		buckets = append(buckets, h)
	}
	sort.Slice(buckets, func(i, j int) bool {
		return buckets[i].Sample.value >= buckets[j].Sample.value
	})

	dc1 := gg.NewContext(h3.stride, h3.stride)
	sharedTiles.Plot(dc1, t.tileOffsets)
	h3.Plot(dc1)
	fmt.Println("len(h3.buckets):", len(h3.buckets))
	if err := dc1.SavePNG("debug.png"); err != nil {
		return nil, err
	}
	return buckets, nil

	/*
		tileUses := make(map[uint32]int, 80000) // 72138 for TIFF of Jan 24, 2022
		tile := make(map[uint32]int, 80000)
		for i, off := range t.tileOffsets {
			tile[off] = i
			tileUses[off] += 1
		}

		stride = int((t.imageWidth + t.tileWidth - 1) / t.tileWidth)
		hist := make(map[int64]bucket, 250000) // 210037 for TIFF of Jan 24, 2022
		//hist2 := newHistogram()
		var n int

		sharedTiles.Plot(dc1, t.tileOffsets)
		if err := dc1.SavePNG("debug.png"); err != nil {
			return nil, err
		}

		zoom := uint8(math.Ilogb(float64(t.imageWidth)))
		for off, ti := range tile {
			continue
			if err := t.readTile(TileIndex(ti), data); err != nil {
				return nil, err
			}
			uses := int64(tileUses[off])
			tileX, tileY := ti%stride, ti/stride
			for px, value := range data {
				key := int64(value + 0.5) //+ 100
				// Collect more samples for small values.
				if value < 100 {
					//key = int64(value*100 + 0.5)
				}
				if h, present := hist[key]; present {
					// frequent code path
					h.count += uses
					hist[key] = h
				} else {
					// infrequent code path, taken ~200K times
					x := uint32(tileX)*t.tileWidth + uint32(px)%t.tileWidth
					y := uint32(tileY)*t.tileHeight + uint32(px)/t.tileWidth
					lng := float32(x)/float32(t.imageWidth)*360.0 - 180.0
					lat := float32(TileLatitude(zoom+8, y) * (180 / math.Pi))
					//lng, lat := float32(x), float32(y)
					hist[key] = bucket{uses, bucketSample{value, lat, lng}}
				}
			}
			n += 1
			//if n > 5000 { break }
		}

		buckets := make([]bucket, 0, len(hist))
		for _, h := range hist {
			buckets = append(buckets, h)
		}
		//fmt.Printf("**** ZEBRA len(hist)=%d\n", len(hist))
		sort.Slice(buckets, func(i, j int) bool {
			return buckets[i].sample.value >= buckets[j].sample.value
		})
		return buckets, nil
	*/
}

func calcStats(hist []bucket2) (*Stats, error) {
	var maxVal float32
	var totalCount int64
	for _, h := range hist {
		if h.Sample.value > maxVal {
			maxVal = h.Sample.value
		}
		totalCount += h.Count
	}

	var x, y, lastX, lastY float64
	stats := &Stats{}
	stats.Samples = make([]Sample, 0, 1000)

	rank := int64(1)
	scaleX := 1000.0 / math.Log10(float64(totalCount))
	scaleY := 1000.0 / math.Log10(float64(maxVal))

	dc := gg.NewContext(1020, 1020)
	dc.SetRGB(1, 1, 1)
	dc.Clear()
	dc.SetRGB(0, 0.4, 1)

	type Point struct{ X, Y float64 }
	graph := make([]Point, 0, 1000)
	for i, b := range hist {
		x = math.Log10(float64(rank)) * scaleX
		if x < 0 {
			x = 0
		} else if x > 1000 {
			x = 1000
		}
		y = math.Log10(float64(b.Sample.value)) * scaleY
		if y < 0 {
			y = 0
		} else if y > 1000 {
			y = 1000
		}
		dist := (x-lastX)*(x-lastX) + (y-lastY)*(y-lastY)
		if dist >= 16.0 || i == len(hist)-1 {
			graph = append(graph, Point{x, y})
			stats.Samples = append(stats.Samples, Sample{[]float32{b.Sample.lat, b.Sample.lng}, rank, b.Sample.value})
			lastX, lastY = x, y
			if stats.Median == 0 && rank >= totalCount/2 {
				stats.Median = len(stats.Samples) - 1
			}
		}
		rank += b.Count
	}

	dc.MoveTo(10, 10)
	for _, p := range graph {
		dc.LineTo(p.X+10.0, 1000.0-p.Y+10.0)
	}
	dc.Stroke()

	for _, p := range graph {
		dc.DrawCircle(p.X+10.0, 1000.0-p.Y+10.0, 4.0)
		dc.Fill()
	}

	if err := dc.SavePNG("osmviews-distribution.png"); err != nil {
		return nil, err
	}

	return stats, nil
}
