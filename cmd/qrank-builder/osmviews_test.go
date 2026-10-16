// SPDX-FileCopyrightText: 2026 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"reflect"
	"slices"
	"testing"
)

// TestOSMViewsProvenance describes the GeoTIFF in testdata/osmviews,
// which FakeWikiSite serves as if it were the current upstream version.
var testOSMViewsProvenance = &OSMViewsProvenance{
	Version:   "2021-11-28",
	URL:       "https://osmviews.brawer.ch/data/osmviews-20211128.tiff",
	PURL:      "pkg:generic/osmviews@2021-11-28?checksum=sha256:959f2273e0e6eb11cf4d4233617906f2e72690bc2328b4180b5d17c55eb51569&download_url=https://osmviews.brawer.ch/data/osmviews-20211128.tiff",
	SHA256:    "959f2273e0e6eb11cf4d4233617906f2e72690bc2328b4180b5d17c55eb51569",
	SHA512:    "269ff52890972bf3c45b9b52a3fd39121ba3c978553c5f02769a54bedd9e8b8778b843651c16cc68128d4bf3d65cf8eaa552cb3e6e1b00a6960b2adb37bb0744",
	BOMURL:    "https://osmviews.brawer.ch/data/osmviews-20211128.cdx.json",
	BOMSHA256: "963d721b013cf4f938ae571cf56f041e70ace453f7b3c3b93325a7d9088093d2",
}

func TestFetchOSMViews(t *testing.T) {
	logger = log.New(&bytes.Buffer{}, "", log.Lshortfile)
	ctx := context.Background()
	s3 := NewFakeS3()
	site := &FakeWikiSite{}
	client := &http.Client{Transport: site}

	// With an empty cache, the GeoTIFF gets downloaded and cached.
	path, prov, err := fetchOSMViews(ctx, client, s3)
	if err != nil {
		t.Fatal(err)
	}
	checkOSMViewsFile(t, path)
	if !reflect.DeepEqual(prov, testOSMViewsProvenance) {
		t.Errorf("got %+v, want %+v", prov, testOSMViewsProvenance)
	}
	for _, p := range []string{"osmviews/osmviews-20211128.tiff", "osmviews/osmviews-20211128.cdx.json"} {
		if _, ok := s3.data[p]; !ok {
			t.Errorf("fetchOSMViews() should cache %s", p)
		}
	}
	wantPuts := []string{"osmviews/osmviews-20211128.tiff", "osmviews/osmviews-20211128.cdx.json"}
	if !slices.Equal(s3.puts, wantPuts) {
		t.Errorf("got puts %v, want %v", s3.puts, wantPuts)
	}

	// While the upstream version is unchanged, the GeoTIFF
	// comes from the cache.
	site.Requests = nil
	path, prov, err = fetchOSMViews(ctx, client, s3)
	if err != nil {
		t.Fatal(err)
	}
	checkOSMViewsFile(t, path)
	if !reflect.DeepEqual(prov, testOSMViewsProvenance) {
		t.Errorf("got %+v, want %+v", prov, testOSMViewsProvenance)
	}
	if want := []string{osmviewsDatapackageURL}; !slices.Equal(site.Requests, want) {
		t.Errorf("got requests %v, want %v", site.Requests, want)
	}

	// If upstream is down, the GeoTIFF comes from the cache.
	site.Broken = true
	path, prov, err = fetchOSMViews(ctx, client, s3)
	if err != nil {
		t.Fatal(err)
	}
	checkOSMViewsFile(t, path)
	if !reflect.DeepEqual(prov, testOSMViewsProvenance) {
		t.Errorf("got %+v, want %+v", prov, testOSMViewsProvenance)
	}
}

func TestFetchOSMViews_BrokenWithoutCache(t *testing.T) {
	logger = log.New(&bytes.Buffer{}, "", log.Lshortfile)
	client := &http.Client{Transport: &FakeWikiSite{Broken: true}}
	if _, _, err := fetchOSMViews(context.Background(), client, NewFakeS3()); err == nil {
		t.Error("want error, got nil")
	}
}

// A corrupt copy in the cache must not be used.
func TestFetchOSMViews_CorruptCache(t *testing.T) {
	logger = log.New(&bytes.Buffer{}, "", log.Lshortfile)
	ctx := context.Background()
	s3 := NewFakeS3()
	client := &http.Client{Transport: &FakeWikiSite{}}
	path, _, err := fetchOSMViews(ctx, client, s3)
	if err != nil {
		t.Fatal(err)
	}
	os.Remove(path)
	s3.data["osmviews/osmviews-20211128.tiff"] = []byte("corrupt")
	client = &http.Client{Transport: &FakeWikiSite{Broken: true}}
	if _, _, err := fetchOSMViews(ctx, client, s3); err == nil {
		t.Error("want error, got nil")
	}
}

func checkOSMViewsFile(t *testing.T, path string) {
	t.Helper()
	defer os.Remove(path)
	if err := checkFileSHA256(path, "sha256:"+testOSMViewsProvenance.SHA256, 64872); err != nil {
		t.Error(err)
	}
}

func TestPruneOSMViewsCache(t *testing.T) {
	ctx := context.Background()
	s3 := NewFakeS3()
	for _, ymd := range []string{"20260927", "20261004", "20261011"} {
		s3.data["osmviews/osmviews-"+ymd+".tiff"] = []byte("tiff")
		s3.data["osmviews/osmviews-"+ymd+".cdx.json"] = []byte("bom")
	}
	s3.data["osmviews/osmviews-20261018.tiff"] = []byte("incomplete, no BOM")
	if err := pruneOSMViewsCache(ctx, 2, s3); err != nil {
		t.Fatal(err)
	}
	var got []string
	for path := range s3.data {
		got = append(got, path)
	}
	slices.Sort(got)
	want := []string{
		"osmviews/osmviews-20261004.cdx.json",
		"osmviews/osmviews-20261004.tiff",
		"osmviews/osmviews-20261011.cdx.json",
		"osmviews/osmviews-20261011.tiff",
		"osmviews/osmviews-20261018.tiff",
	}
	if !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestScoreItemCoordinates(t *testing.T) {
	logger = log.New(&bytes.Buffer{}, "", log.Lshortfile)
	ctx := context.Background()
	s3 := NewFakeS3()
	coordinates := "item_coordinates/wikidatawiki-20240401-item_coordinates.zst"
	s3.WriteLines([]string{
		"662541,46.51,8.31",
		"72,47.37444444,8.54111111",
		"72,47.28,8.4",
		"5649951,89.9,0",
	}, coordinates)

	path, err := scoreItemCoordinates(ctx, "testdata/osmviews/osmviews-20211128.tiff", coordinates, s3)
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(path)
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	scores := newOSMViewsScores(f)

	for _, tc := range []struct {
		item  int64
		score float32
		ok    bool
	}{
		{1, 0, false},
		{72, 1, true},
		{100, 0, false},
		{662541, 0.30179042, true},
		{5649951, 0, true},
		{5649952, 0, false},
	} {
		score, ok, err := scores.Lookup(tc.item)
		if err != nil {
			t.Fatal(err)
		}
		if score != tc.score || ok != tc.ok {
			t.Errorf("Q%d: got (%v, %v), want (%v, %v)", tc.item, score, ok, tc.score, tc.ok)
		}
	}
}

func TestOSMViewsScores(t *testing.T) {
	var buf bytes.Buffer
	for _, s := range []itemScore{{3, 0.5}, {7, 0}, {9, 1}} {
		var rec [12]byte
		binary.LittleEndian.PutUint64(rec[0:8], uint64(s.item))
		binary.LittleEndian.PutUint32(rec[8:12], math.Float32bits(s.score))
		buf.Write(rec[:])
	}
	scores := newOSMViewsScores(&buf)
	var got []float32
	for item := int64(1); item <= 10; item++ {
		score, ok, err := scores.Lookup(item)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			score = -1
		}
		got = append(got, score)
	}
	want := []float32{-1, -1, 0.5, -1, -1, -1, 0, -1, 1, -1}
	if !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestOSMViewsScores_Truncated(t *testing.T) {
	scores := newOSMViewsScores(bytes.NewReader([]byte{1, 2, 3}))
	if _, _, err := scores.Lookup(1); err != io.ErrUnexpectedEOF {
		t.Errorf("got %v, want io.ErrUnexpectedEOF", err)
	}
}

func TestItemScoreToBytes(t *testing.T) {
	a := itemScore{item: 662541, score: 0.25}
	if got := itemScoreFromBytes(a.ToBytes()).(itemScore); got != a {
		t.Errorf("got %v, want %v", got, a)
	}
}

func TestParseOSMViewsBOM_Incomplete(t *testing.T) {
	bom := []byte(`{"metadata": {"component": {"purl": "pkg:generic/osmviews@2021-11-28"}}}`)
	if _, err := parseOSMViewsBOM(bom, "2021-11-28", "", ""); err == nil {
		t.Error("want error for BOM without SHA-256, got nil")
	}
}
