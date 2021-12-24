// SPDX-License-Identifier: MIT

package main

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
)

func ExampleTileKey_String() {
	fmt.Println(MakeTileKey(7, 42, 23).String(), NoTile.String())
	// Output: 7/42/23 NoTile
}

func ExampleTileKey_Contains() {
	switzerland, zurich := MakeTileKey(6, 33, 22), MakeTileKey(13, 4290, 2868)
	fmt.Println(switzerland.Contains(zurich))
	fmt.Println(zurich.Contains(switzerland))
	fmt.Println(zurich.Contains(zurich))
	fmt.Println(WorldTile.Contains(zurich))
	// Output:
	// true
	// false
	// false
	// true
}

func ExampleTileKey_Zoom() {
	fmt.Println(MakeTileKey(7, 42, 23).Zoom())
	// Output: 7
}

func ExampleTileKey_Next() {
	for tile := WorldTile; tile != NoTile; tile = tile.Next(2) {
		fmt.Println(tile)
	}
	// Output:
	// 0/0/0
	// 1/0/0
	// 2/0/0
	// 2/1/0
	// 2/0/1
	// 2/1/1
	// 1/1/0
	// 2/2/0
	// 2/3/0
	// 2/2/1
	// 2/3/1
	// 1/0/1
	// 2/0/2
	// 2/1/2
	// 2/0/3
	// 2/1/3
	// 1/1/1
	// 2/2/2
	// 2/3/2
	// 2/2/3
	// 2/3/3
}

func TestTileKey_Next_CurZoomDeeperThanMaxZoom(t *testing.T) {
	tile := MakeTileKey(17, 68640, 45888)
	got := tile.Next(12)
	want := MakeTileKey(12, 2144, 1435)
	if got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestTileKey_Latitude(t *testing.T) {
	for _, tc := range []struct {
		zoom     uint8
		y        uint32
		expected float64
	}{
		{0, 0, 85.05112877980659},
		{0, 1, -85.05112877980659},

		{1, 0, 85.05112877980659},
		{1, 1, 0.0},
		{1, 2, -85.05112877980659},

		{2, 0, 85.05112877980659},
		{2, 1, 66.51326044311185},
		{2, 2, 0.0},
		{2, 3, -66.51326044311185},
		{2, 4, -85.05112877980659},

		// https://tile.openstreetmap.org/3/3/$y.png
		{3, 0, 85.05112877980659},
		{3, 1, 79.17133464081945},
		{3, 2, 66.51326044311185},
		{3, 3, 40.979898069620134},
		{3, 4, 0.0},
		{3, 5, -40.979898069620134},
		{3, 6, -66.51326044311185},
		{3, 7, -79.17133464081945},
		{3, 8, -85.05112877980659},

		// Rapperswil, https://tile.openstreetmap.org/17/68746/45980.png
		{17, 45980, 47.22702939886731},
	} {
		got := TileLatitude(tc.zoom, tc.y) * 180 / math.Pi
		if math.Abs(got-tc.expected) > 1e-13 {
			t.Errorf("expected TileLatitude(%d, %d) = %g, got %g",
				tc.zoom, tc.y, tc.expected, got)
		}
	}
}

func TestTileKey_Area(t *testing.T) {
	for _, tc := range []struct {
		name     string
		zoom     uint8
		x, y     uint32
		expected float64 // in km2
	}{
		{"visible in web mercator", 0, 0, 0, 482018410.976947546},

		{"max north", 1, 1, 0, 120504602.744236887},
		{"Longyearbyen", 1, 1, 0, 120504602.744236887},
		{"London", 1, 0, 0, 120504602.744236887},
		{"Zurich", 1, 1, 0, 120504602.744236887},
		{"Null Island", 1, 1, 1, 120504602.744236887},
		{"Nairobi", 1, 1, 1, 120504602.744236887},
		{"Ushuaia", 1, 0, 1, 120504602.744236887},
		{"McMurdo", 1, 1, 1, 120504602.744236887},
		{"max south", 1, 1, 1, 120504602.744236887},

		{"max north", 2, 2, 0, 13132679.669789132},
		{"Longyearbyen", 2, 2, 0, 13132679.669789132},
		{"London", 2, 1, 1, 47119621.702329315},
		{"Zurich", 2, 2, 1, 47119621.702329315},
		{"Null Island", 2, 2, 2, 47119621.702329315},
		{"Nairobi", 2, 2, 2, 47119621.702329315},
		{"Ushuaia", 2, 1, 2, 47119621.702329315},
		{"McMurdo", 2, 3, 3, 13132679.669789132},
		{"max south", 2, 2, 3, 13132679.669789132},

		{"max north", 3, 4, 0, 2082695.042093211},
		{"Longyearbyen", 3, 4, 1, 4483644.792801355},
		{"London", 3, 3, 2, 9044229.434944317},
		{"Zurich", 3, 4, 2, 9044229.434944317},
		{"Null Island", 3, 4, 4, 14515581.416220339},
		{"Nairobi", 3, 4, 4, 14515581.416220339},
		{"Ushuaia", 3, 2, 5, 9044229.434944317},
		{"McMurdo", 3, 7, 6, 4483644.792801355},
		{"max south", 3, 4, 7, 2082695.042093211},

		{"max north", 4, 8, 0, 420599.369138243},
		{"Longyearbyen", 4, 8, 2, 912413.768734086},
		{"London", 4, 7, 5, 2620581.675768902},
		{"Zurich", 4, 8, 5, 2620581.675768902},
		{"Null Island", 4, 8, 8, 3886247.635600871},
		{"Nairobi", 4, 9, 8, 3886247.635600871},
		{"Ushuaia", 4, 4, 10, 2620581.675768902},
		{"McMurdo", 4, 15, 13, 912413.768734086},
		{"max south", 4, 8, 15, 420599.369138243},

		{"max north", 5, 16, 0, 94917.304263808},
		{"Longyearbyen", 5, 17, 4, 206374.519280502},
		{"London", 5, 15, 10, 606949.201369069},
		{"Zurich", 5, 16, 11, 703341.636515382},
		{"Null Island", 5, 16, 16, 989881.686781106},
		{"Nairobi", 5, 19, 16, 989881.686781106},
		{"Ushuaia", 5, 9, 21, 606949.201369069},
		{"McMurdo", 5, 30, 27, 206374.519280502},
		{"max south", 5, 16, 31, 94917.304263808},

		{"max north", 6, 32, 0, 22570.745042465},
		{"Longyearbyen", 6, 34, 8, 49117.967031093},
		{"London", 6, 31, 21, 157636.885855947},
		{"Zurich", 6, 33, 22, 169732.469604271},
		{"Null Island", 6, 32, 32, 248656.363333909},
		{"Nairobi", 6, 38, 32, 248656.363333909},
		{"Ushuaia", 6, 19, 43, 145837.714828588},
		{"McMurdo", 6, 61, 54, 54069.292609158},
		{"max south", 6, 32, 63, 22570.745042465},

		{"max north", 7, 64, 0, 5504.791483570},
		{"Longyearbyen", 7, 69, 17, 12574.897104782},
		{"London", 7, 63, 42, 38660.607312305},
		{"Zurich", 7, 67, 44, 41671.234980972},
		{"Null Island", 7, 64, 64, 62238.880174075},
		{"Nairobi", 7, 77, 64, 62238.880174075},
		{"Ushuaia", 7, 39, 87, 35734.237866878},
		{"McMurdo", 7, 123, 109, 13193.536055588},
		{"max south", 7, 64, 127, 5504.791483570},

		{"max north", 8, 128, 0, 1359.376360159},
		{"Longyearbyen", 8, 139, 35, 3181.506689224},
		{"London", 8, 127, 85, 9758.120023951},
		{"Zurich", 8, 134, 89, 10512.792562642},
		{"Null Island", 8, 128, 128, 15564.404929515},
		{"Nairobi", 8, 154, 128, 15564.404929515},
		{"Ushuaia", 8, 79, 174, 9023.332093710},
		{"McMurdo", 8, 246, 219, 3258.827905010},
		{"max south", 8, 128, 255, 1359.376360159},

		{"max north", 9, 256, 0, 337.766825753},
		{"Longyearbyen", 9, 278, 70, 790.599387807},
		{"London", 9, 255, 170, 2427.867920060},
		{"Zurich", 9, 268, 179, 2640.090770781},
		{"Null Island", 9, 256, 256, 3891.394203086},
		{"Nairobi", 9, 308, 257, 3890.808261671},
		{"Ushuaia", 9, 158, 349, 2244.554483606},
		{"McMurdo", 9, 493, 438, 819.595114172},
		{"max south", 9, 256, 511, 337.766825753},

		{"max north", 10, 512, 0, 84.183621022},
		{"Longyearbyen", 10, 556, 141, 198.243581328},
		{"London", 10, 511, 340, 605.511694822},
		{"Zurich", 10, 536, 358, 658.535053188},
		{"Null Island", 10, 512, 512, 972.866864026},
		{"Nairobi", 10, 616, 515, 972.647139442},
		{"Ushuaia", 10, 317, 699, 559.732298176},
		{"McMurdo", 10, 986, 877, 204.284259056},
		{"max south", 10, 512, 1023, 84.183621022},

		{"max north", 11, 1024, 0, 21.013742359},
		{"Longyearbyen", 11, 1113, 283, 49.635325608},
		{"London", 11, 1023, 680, 151.196171817},
		{"Zurich", 11, 1072, 717, 164.819655753},
		{"Null Island", 11, 1024, 1024, 243.217860625},
		{"Nairobi", 11, 1233, 1031, 243.153775545},
		{"Ushuaia", 11, 635, 1398, 140.108643546},
		{"McMurdo", 11, 1972, 1754, 51.147660147},
		{"max south", 11, 1024, 2047, 21.013742359},

		{"max north", 12, 2048, 0, 5.249421323},
		{"Longyearbyen", 12, 2226, 567, 12.418148558},
		{"London", 12, 2046, 1361, 37.821751893},
		{"Zurich", 12, 2145, 1434, 41.181673356},
		{"Null Island", 12, 2048, 2048, 60.804536696},
		{"Nairobi", 12, 2466, 2062, 60.789516505},
		{"Ushuaia", 12, 1270, 2796, 35.049120909},
		{"McMurdo", 12, 3944, 3508, 12.796503161},
		{"max south", 12, 2048, 4095, 5.249421323},

		{"max north", 13, 4096, 0, 1.311853928},
		{"Longyearbyen", 13, 4452, 1134, 3.103371658},
		{"London", 13, 4093, 2723, 9.458277221},
		{"Zurich", 13, 4290, 2868, 10.292513514},
		{"Null Island", 13, 4096, 4096, 15.201138645},
		{"Nairobi", 13, 4933, 4125, 15.197249514},
		{"Ushuaia", 13, 2541, 5593, 8.759534359},
		{"McMurdo", 13, 7888, 7016, 3.200325163},
		{"max south", 13, 4096, 8191, 1.311853928},

		{"max north", 14, 8192, 0, 0.327900830},
		{"Longyearbyen", 14, 8904, 2268, 0.775697282},
		{"London", 14, 8186, 5446, 2.364214360},
		{"Zurich", 14, 8580, 5736, 2.572765291},
		{"Null Island", 14, 8192, 8192, 3.800284941},
		{"Nairobi", 14, 9866, 8250, 3.799328859},
		{"Ushuaia", 14, 5082, 11186, 2.190226769},
		{"McMurdo", 14, 15776, 14032, 0.800231266},
		{"max south", 14, 8192, 16383, 0.327900830},

		{"max north", 15, 16384, 0, 0.081967378},
		{"Longyearbyen", 15, 17808, 4536, 0.193906120},
		{"London", 15, 16372, 10892, 0.591009224},
		{"Zurich", 15, 17160, 11472, 0.643145938},
		{"Null Island", 15, 16384, 16384, 0.950071253},
		{"Nairobi", 15, 19732, 16500, 0.949834257},
		{"Ushuaia", 15, 10164, 22372, 0.547599593},
		{"McMurdo", 15, 31552, 28064, 0.200076567},
		{"max south", 15, 16384, 32767, 0.081967378},

		{"max north", 16, 32768, 0, 0.020490866},
		{"Longyearbyen", 16, 35616, 9072, 0.048474255},
		{"London", 16, 32744, 21784, 0.147746761},
		{"Zurich", 16, 34320, 22944, 0.160780811},
		{"Null Island", 16, 32768, 32768, 0.237517814},
		{"Nairobi", 16, 39464, 33000, 0.237458819},
		{"Ushuaia", 16, 20328, 44744, 0.136905261},
		{"McMurdo", 16, 63104, 56128, 0.050021486},
		{"max south", 16, 32768, 65535, 0.020490866},

		{"max north", 17, 65536, 0, 0.005122594},
		{"Longyearbyen", 17, 71232, 18144, 0.012118279},
		{"London", 17, 65488, 43568, 0.036935997},
		{"Zurich", 17, 68640, 45888, 0.040194494},
		{"Null Island", 17, 65536, 65536, 0.059379454},
		{"Nairobi", 17, 78928, 66000, 0.059364736},
		{"Ushuaia", 17, 40656, 89488, 0.034226986},
		{"McMurdo", 17, 126208, 112256, 0.012505664},
		{"max south", 17, 65536, 131071, 0.005122594},

		{"max north", 18, 131072, 0, 0.001280633},
		{"Longyearbyen", 18, 142464, 36288, 0.003029534},
		{"London", 18, 130976, 87136, 0.009233913},
		{"Zurich", 18, 137280, 91776, 0.010048535},
		{"Null Island", 18, 131072, 131072, 0.014844863},
		{"Nairobi", 18, 157856, 132000, 0.014841188},
		{"Ushuaia", 18, 81312, 178976, 0.008556830},
		{"McMurdo", 18, 252416, 224512, 0.003126453},
		{"max south", 18, 131072, 262143, 0.001280633},

		{"max north", 19, 262144, 0, 0.000320156},
		{"Longyearbyen", 19, 284928, 72576, 0.000757379},
		{"London", 19, 261952, 174272, 0.002308467},
		{"Zurich", 19, 274560, 183552, 0.002512123},
		{"Null Island", 19, 262144, 262144, 0.003711216},
		{"Nairobi", 19, 315712, 264000, 0.003710298},
		{"Ushuaia", 19, 162624, 357952, 0.002139218},
		{"McMurdo", 19, 504832, 449024, 0.000781618},
		{"max south", 19, 262144, 524287, 0.000320156},

		{"max north", 20, 524288, 0, 0.000080039},
		{"Longyearbyen", 20, 569856, 145152, 0.000189344},
		{"London", 20, 523904, 348544, 0.000577115},
		{"Zurich", 20, 549120, 367104, 0.000628029},
		{"Null Island", 20, 524288, 524288, 0.000927804},
		{"Nairobi", 20, 631424, 528000, 0.000927574},
		{"Ushuaia", 20, 325248, 715904, 0.000534806},
		{"McMurdo", 20, 1009664, 898048, 0.000195405},
		{"max south", 20, 524288, 1048575, 0.000080039},
	} {
		got := TileArea(tc.zoom, tc.y)
		if math.Abs(got-tc.expected) > 0.0001 {
			t.Errorf("expected TileArea(%d, %d) = %g km2, got %g km2",
				tc.zoom, tc.y, tc.expected, got)
		}
	}
}

func TestTileKey_ToZoom(t *testing.T) {
	type TK struct {
		z    uint8
		x, y uint32
	}
	for _, tc := range []struct{ a, b TK }{
		{TK{0, 0, 0}, TK{0, 0, 0}},
		{TK{0, 0, 0}, TK{2, 0, 0}},
		{TK{2, 0, 0}, TK{0, 0, 0}},
		{TK{3, 4, 5}, TK{3, 4, 5}},
		{TK{7, 1, 1}, TK{9, 4, 4}},
		{TK{18, 130976, 87136}, TK{12, 2046, 1361}},
	} {
		tile := MakeTileKey(tc.a.z, tc.a.x, tc.a.y)
		want := MakeTileKey(tc.b.z, tc.b.x, tc.b.y)
		if got := tile.ToZoom(tc.b.z); got != want {
			t.Errorf("got %v.ToZoom(%d)=%v, want %v", tile, tc.b.z, got, want)
		}
	}
}

var tk TileKey

func BenchmarkMakeTileKey(b *testing.B) {
	zoom := make([]uint8, 64)
	x := make([]uint32, 64)
	y := make([]uint32, 64)
	for i, key := range makeTestTileKeys(64) {
		zoom[i], x[i], y[i] = key.ZoomXY()
	}
	for n := 0; n < b.N; n++ {
		tk = MakeTileKey(zoom[n%64], x[n%64], y[n%64])
	}
}

var unused uint32

func BenchmarkTileKey_ZoomXY(b *testing.B) {
	keys := makeTestTileKeys(64)
	for n := 0; n < b.N; n++ {
		zoom, x, y := keys[n%64].ZoomXY()
		unused |= uint32(zoom) + x + y
	}
}

func TestMakeTileKey(t *testing.T) {
	for n := 0; n < 5000; n++ {
		zoom := uint8(rand.Intn(24))
		x := uint32(rand.Intn(1 << zoom))
		y := uint32(rand.Intn(1 << zoom))
		key := MakeTileKey(zoom, x, y)
		gotZoom, gotX, gotY := key.ZoomXY()
		if gotZoom != zoom || gotX != x || gotY != y {
			t.Errorf("expected %d/%d/%d, got %d/%d/%d", zoom, x, y, gotZoom, gotX, gotY)
		}
	}
}

func makeTestTileKeys(n int) []TileKey {
	keys := make([]TileKey, n)
	for i := 0; i < n; i++ {
		zoom := uint8(rand.Intn(24))
		x := uint32(rand.Intn(1 << zoom))
		y := uint32(rand.Intn(1 << zoom))
		keys[i] = MakeTileKey(zoom, x, y)
	}
	return keys
}

func ExampleParseTileCount() {
	fmt.Println(ParseTileCount("7/42/23 98765"))
	fmt.Println(ParseTileCount("1/207/400 10"), ParseTileCount("junk"))
	// Output: {7/42/23 98765}
	// {NoTile 0} {NoTile 0}
}

func TestTileCountRoundTrip(t *testing.T) {
	for _, key := range makeTestTileKeys(1000) {
		tc := TileCount{Key: key, Count: rand.Uint64()}
		got := TileCountFromBytes(tc.ToBytes()).(TileCount)
		if got.Key != tc.Key || got.Count != tc.Count {
			t.Errorf("not round-trippable: %v, got %v", tc, got)
		}
	}
}

func TestTileCountLess(t *testing.T) {
	type TC struct {
		x, y  uint32
		count uint64
		zoom  uint8
	}
	for _, tc := range []struct {
		a, b     TC
		expected bool
	}{
		{TC{2, 5, 8, 9}, TC{3, 5, 8, 9}, true},  // a.X < b.X
		{TC{2, 5, 8, 9}, TC{1, 5, 8, 9}, false}, // a.X > b.X
		{TC{2, 5, 8, 9}, TC{2, 6, 8, 9}, true},  // a.Y < b.Y
		{TC{2, 5, 8, 9}, TC{2, 4, 8, 9}, false}, // a.Y > b.Y

		{TC{2, 5, 8, 9}, TC{2, 5, 7, 9}, false}, // a.Count>b.Count
		{TC{2, 5, 8, 9}, TC{2, 5, 9, 9}, true},  // a.Count<b.Count
		{TC{2, 5, 8, 9}, TC{2, 5, 8, 9}, false}, // all equal

		{TC{0, 0, 0, 0}, TC{0, 0, 0, 0}, false},
		{TC{0, 0, 0, 0}, TC{0, 0, 0, 1}, true},
		{TC{0, 0, 0, 0}, TC{0, 1, 0, 1}, true},
		{TC{0, 0, 0, 0}, TC{1, 0, 0, 1}, true},
		{TC{0, 0, 0, 0}, TC{1, 1, 0, 1}, true},
		{TC{0, 0, 0, 1}, TC{0, 1, 0, 1}, true},
		{TC{0, 1, 0, 1}, TC{0, 1, 0, 1}, false},
		{TC{1, 0, 0, 1}, TC{0, 1, 0, 1}, true},
		{TC{1, 1, 0, 1}, TC{0, 1, 0, 1}, false},
		{TC{0, 0, 0, 2}, TC{0, 1, 0, 1}, true},

		{TC{17187, 11494, 104, 15}, TC{17187, 11495, 79, 15}, true},
		{TC{17187, 11495, 79, 15}, TC{17187, 11494, 104, 15}, false},
	} {
		a := TileCount{MakeTileKey(tc.a.zoom, tc.a.x, tc.a.y), tc.a.count}
		b := TileCount{MakeTileKey(tc.b.zoom, tc.b.x, tc.b.y), tc.b.count}
		got := TileCountLess(a, b)
		if tc.expected != got {
			t.Errorf("expected TileCountLess(%v, %v) = %v, got %v",
				tc.a, tc.b, tc.expected, got)
		}
	}
}

func TestTileCountLessContainment(t *testing.T) {
	bigX := uint32(17161)
	bigY := uint32(11476)
	big := TileCount{Key: MakeTileKey(15, bigX, bigY), Count: 42}
	for y := bigY << 3; y < (bigY+1)<<3; y++ {
		for x := bigX << 3; x < (bigX+1)<<3; x++ {
			small := TileCount{Key: MakeTileKey(18, x, y), Count: 7}
			if !TileCountLess(big, small) {
				t.Errorf("expected TileCountLess(%v, %v) = true because the former geographically contains the latter",
					big, small)
			}
			if TileCountLess(small, big) {
				t.Errorf("expected TileCountLess(%v, %v) = false because the former is geographically contained within the latter",
					small, big)
			}
		}
	}

	smallOutside := TileCount{MakeTileKey(18, bigX<<3-1, bigY<<3), 42}
	if TileCountLess(big, smallOutside) {
		t.Errorf("expected TileCountLess(%v, %v) = false, got true",
			big, smallOutside)
	}
}
