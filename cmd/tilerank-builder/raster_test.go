// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"testing"

	"github.com/orcaman/writerseeker"
)

func TestRaster_Paint(t *testing.T) {
	r := NewRaster(MakeTileKey(1, 1, 1), NewRaster(WorldTile, nil))
	r.Paint(MakeTileKey(2, 3, 3), 23)
	r.Paint(MakeTileKey(3, 6, 7), 42)
	wantPixels(t, r.pixels, [4][4]float32{
		{0, 0, 0, 0},
		{0, 0, 0, 0},
		{0, 0, 23, 23},
		{0, 0, 65, 23},
	})
}

func TestRaster_Paint_SubPixel(t *testing.T) {
	tile := MakeTileKey(1, 0, 0)
	r := NewRaster(tile, NewRaster(WorldTile, nil))
	r.Paint(MakeTileKey(10, 256, 256), 100) // covers 1/4th of a pixel
	wantPixels(t, r.pixels, [4][4]float32{
		{0, 0, 0, 0},
		{0, 0, 0, 0},
		{0, 0, 25, 0},
		{0, 0, 0, 0},
	})
}

func TestRaster_PaintChild(t *testing.T) {
	r := NewRaster(MakeTileKey(1, 1, 1), NewRaster(WorldTile, nil))
	r.pixels[1] = 123456
	r.pixels[256] = 789123
	r.parent.PaintChild(r)
	wantPixels(t, r.parent.pixels, [4][4]float32{
		{0, 0, 0, 0},
		{0, 0, 0, 0},
		{0, 0, 789123, 0},
		{0, 0, 0, 0},
	})
}

func wantPixels(t *testing.T, got [256 * 256]float32, want [4][4]float32) {
	px := []int{0, 64, 128, 192}
	for j, vals := range want {
		for i, wantPix := range vals {
			x, y := px[i], px[j]
			gotPix := got[y<<8+x]
			if math.Abs(float64(gotPix-wantPix)) > 1e-4 {
				t.Errorf("pixel (%d, %d): got %f, want %f", x, y, gotPix, wantPix)
			}
		}
	}
}

func TestRasterWriter_writeIFDList(t *testing.T) {
	f := &writerseeker.WriterSeeker{}
	f.Write([]byte{
		// Byte 0..3: File magic for Little-Endian TIFF less than 4GiB.
		'I', 'I', 42, 0,

		// Byte 4..7: Offset of first Image File Directory (IFD).
		0xff, 0xff, 0xff, 0xff, // to be overwritten

		// Byte 8..11: Ghost are; actually used for GDAL structural metadata.
		9, 8, 7, 6,

		// Byte 12..41: First IFD with 2 entries.
		2, 0, // numEntries
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, // entry #0
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, // entry #1
		0xde, 0xad, 0xbe, 0xef, // Bytes 38..41: nextOffset, overwritten

		// Byte 42..59: Second IFD with 1 entries.
		1, 0, // numEntries
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, // entry #0
		0xde, 0xad, 0xbe, 0xef, // Bytes 56..59: nextOffset, overwritten
	})
	r := &RasterWriter{
		zoom:       2,
		ifdPos:     []int64{42, 0, 12},
		nextIFDPos: []int64{56, 0, 38},
	}
	if err := r.writeIFDList(f); err != nil {
		t.Fatal(err)
	}

	b, _ := io.ReadAll(f.Reader())
	got := make([]uint32, 0, 2)
	p := int64(4)
	for {
		var ifd uint32
		binary.Read(bytes.NewReader(b[p:p+4]), binary.LittleEndian, &ifd)
		if ifd == 0 {
			break
		}
		got = append(got, ifd)
		p = int64(ifd)
		var numEntries uint16
		binary.Read(bytes.NewReader(b[p:p+2]), binary.LittleEndian, &numEntries)
		p += int64(2 + numEntries*12)
	}

	want := []uint32{12, 42}
	if fmt.Sprintf("%v", got) != fmt.Sprintf("%v", want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestRasterWriter_writeTileByteCounts_singleTile(t *testing.T) {
	f := &writerseeker.WriterSeeker{}
	f.Write([]byte{0, 1, 2, 3, 4, 5, 6, 7})
	r := &RasterWriter{
		tileByteCounts:    [][]uint32{{0xfffefdfc}},
		tileByteCountsPos: []int64{2},
	}
	if err := r.writeTileByteCounts(0, f); err != nil {
		t.Fatal(err)
	}

	b, err := io.ReadAll(f.Reader())
	if err != nil {
		t.Fatal(err)
	}

	want := "[0 1 252 253 254 255 6 7]"
	if got := fmt.Sprintf("%v", b); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestRasterWriter_writeTileByteCounts_multiTile(t *testing.T) {
	f := &writerseeker.WriterSeeker{}
	f.Write([]byte{0, 1, 2, 3, 4, 5, 6, 7})
	r := &RasterWriter{
		tileByteCounts:    [][]uint32{{0xfffefdfc, 0x28272625}},
		tileByteCountsPos: []int64{2},
	}
	if err := r.writeTileByteCounts(0, f); err != nil {
		t.Fatal(err)
	}

	b, err := io.ReadAll(f.Reader())
	if err != nil {
		t.Fatal(err)
	}

	want := "[0 1 8 0 0 0 6 7 252 253 254 255 37 38 39 40]"
	if got := fmt.Sprintf("%v", b); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestRasterWriter_writeTileByteCounts_padding(t *testing.T) {
	for i := 0; i < 4; i++ {
		f := &writerseeker.WriterSeeker{}
		f.Write([]byte{'I', 'I', 42, 0, 4, 5, 6, 7})
		f.Write([]byte{8, 9, 10, 11}[:i]) // inject bytes to force alignment
		r := &RasterWriter{
			tileByteCounts:    [][]uint32{{0xcafe, 0xbeef}},
			tileByteCountsPos: []int64{4},
		}
		if err := r.writeTileByteCounts(0, f); err != nil {
			t.Fatal(err)
		}

		b, err := io.ReadAll(f.Reader())
		if err != nil {
			t.Fatal(err)
		}
		if len(b)%4 != 0 {
			t.Errorf("after writeTileByteCounts(), total size should be divisible by 4, but %d is not", len(b))
		}

		var offset uint32
		binary.Read(bytes.NewReader(b[4:8]), binary.LittleEndian, &offset)

		wantOffset, wantLen := uint32(8), 16
		if i > 0 {
			wantOffset = uint32(12)
			wantLen = 20
		}

		if offset != wantOffset {
			t.Errorf("got offset %d, want %d", offset, wantOffset)
		}

		if len(b) != wantLen {
			t.Errorf("got len(%v)=%d, want %d", b, len(b), wantLen)
		}
	}
}

func TestRasterWriter_patchOffset(t *testing.T) {
	f := &writerseeker.WriterSeeker{}
	if _, err := f.Write([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}); err != nil {
		t.Fatal(err)
	}
	if err := patchOffset(f, 3, 0xbeefcafe); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}

	got, err := io.ReadAll(f.Reader())
	if err != nil {
		t.Fatal(err)
	}

	want := []byte{0, 1, 2, 0xfe, 0xca, 0xef, 0xbe, 7, 8, 9}
	if string(got) != string(want) {
		t.Errorf("got %v, want %v", got, want)
	}
}
