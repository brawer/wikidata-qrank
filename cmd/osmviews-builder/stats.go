package main

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"time"

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

func (t *TiffReader) readTile(index int, data []float32) error {
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

func buildHistogram(t *TiffReader) ([]bucket, error) {
	tileUses := make(map[uint32]int, 2000) // 1647 for TIFF of Jan 24, 2022
	tile := make(map[uint32]int, 2000)
	for i, off := range t.tileOffsets {
		tile[off] = i
		tileUses[off] += 1
	}

	fmt.Println("*** GIRAFFE", time.Now().Format(time.RFC3339), len(tile), len(tileUses), len(t.tileOffsets))
	data := make([]float32, t.tileWidth*t.tileHeight)
	tileStride := int((t.imageWidth + t.tileWidth - 1) / t.tileWidth)
	zoom := uint8(math.Ilogb(float64(t.imageWidth)))
	hist := make(map[int64]bucket, 250000) // 210037 for TIFF of Jan 24, 2022
	var n int

	for i, ti := range tile {
		if err := t.readTile(ti, data); err != nil {
			return nil, err
		}
		uses := int64(tileUses[i])
		tileX, tileY := ti%tileStride, ti/tileStride
		for px, value := range data {
			key := int64(value+0.5) + 100
			// Collect more samples for small values.
			if value < 100 {
				key = int64(value*100 + 0.5)
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
		if n > 0 && n%5000 == 0 {
			fmt.Println("*** GIRAFFE-1", time.Now().Format(time.RFC3339), n)
		}
		//if n > 5000 { break }
	}

	buckets := make([]bucket, 0, len(hist))
	for _, h := range hist {
		buckets = append(buckets, h)
	}
	fmt.Printf("**** ZEBRA len(hist)=%d\n", len(hist))
	sort.Slice(buckets, func(i, j int) bool {
		return buckets[i].sample.value >= buckets[j].sample.value
	})
	return buckets, nil
}

func calcStats(hist []bucket) (*Stats, error) {
	var maxVal float32
	var totalCount int64
	for _, h := range hist {
		if h.sample.value > maxVal {
			maxVal = h.sample.value
		}
		totalCount += h.count
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
		y = math.Log10(float64(b.sample.value)) * scaleY
		if y < 0 {
			y = 0
		} else if y > 1000 {
			y = 1000
		}
		dist := (x-lastX)*(x-lastX) + (y-lastY)*(y-lastY)
		if dist >= 16.0 || i == len(hist)-1 {
			graph = append(graph, Point{x, y})
			stats.Samples = append(stats.Samples, Sample{[]float32{b.sample.lat, b.sample.lng}, rank, b.sample.value})
			lastX, lastY = x, y
			if stats.Median == 0 && rank >= totalCount/2 {
				stats.Median = len(stats.Samples) - 1
			}
		}
		rank += b.count
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
