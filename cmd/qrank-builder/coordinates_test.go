// SPDX-FileCopyrightText: 2026 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"bufio"
	"bytes"
	"context"
	"log"
	"math"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestCoordinatesTileKey(t *testing.T) {
	for _, tc := range []struct {
		lat, lon float64
		want     string
	}{
		{0, 0, "0524800"}, // tile 512/512
		{85.05, -180, "0000000"},
		{-85.05, 179.999, "1048575"},
		{0, 180, "0524288"}, // wraps around to -180
		{85.06, 0, "9999999"},
		{-89.9, 0, "9999999"},
		{math.NaN(), 0, "9999999"},
	} {
		if got := coordinatesTileKey(tc.lat, tc.lon); got != tc.want {
			t.Errorf("coordinatesTileKey(%v, %v): got %q, want %q", tc.lat, tc.lon, got, tc.want)
		}
	}

	// Neighbouring coordinates fall into the same tile.
	zurich := coordinatesTileKey(47.37444444, 8.54111111)
	if got := coordinatesTileKey(47.37, 8.54); got != zurich {
		t.Errorf("got %q, want %q", got, zurich)
	}
}

func TestJoinCoordinates(t *testing.T) {
	pages := bufio.NewScanner(strings.NewReader(strings.Join([]string{
		"1,Q107661323,3470",
		"10,Q10,1",
		"200,Q72,,550,85,186",
		"5411171,Q5649951,,1,,20",
		"623646,Q662541,,32,9,15",
	}, "\n")))
	coords := make(chan string, 10)
	for _, c := range []string{
		"1,1.5,2.5",
		"100,1,1", // no item
		"200,47.37444444,8.54111111",
		"200,47.28,8.4",
		"623646,46.51,8.31",
		"9,1,1", // no item, after the last page
	} {
		coords <- c
	}
	close(coords)
	out := make(chan string, 10)
	if err := joinCoordinates(context.Background(), pages, coords, out); err != nil {
		t.Fatal(err)
	}
	close(out)
	var got []string
	for line := range out {
		_, rest, _ := strings.Cut(line, ",")
		got = append(got, rest)
	}
	want := []string{
		"107661323,1.5,2.5",
		"72,47.37444444,8.54111111",
		"72,47.28,8.4",
		"662541,46.51,8.31",
	}
	if !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestBuildItemCoordinates(t *testing.T) {
	logger = log.New(&bytes.Buffer{}, "", log.Lshortfile)
	ctx := context.Background()
	s3 := NewFakeS3()
	dumps := filepath.Join("testdata", "dumps")
	s3.WriteLines([]string{
		"1,Q107661323,3470",
		"19441465,Q5296,372",
		"200,Q72,,550,85,186",
		"5411171,Q5649951,,1,,20",
		"623646,Q662541,,32,9,15",
	}, "page_signals/wikidatawiki-20240401-page_signals.zst")
	dumped, _ := time.Parse(time.DateOnly, "2024-04-01")
	site := &WikiSite{Key: "wikidatawiki", Domain: "www.wikidata.org", LastDumped: dumped}
	sites := &WikiSites{
		Sites:   map[string]*WikiSite{"wikidatawiki": site},
		Domains: map[string]*WikiSite{"www.wikidata.org": site},
	}
	s3.data["item_coordinates/wikidatawiki-20240101-item_coordinates.zst"] = []byte("old")

	if err := buildItemCoordinates(ctx, dumps, sites, s3); err != nil {
		t.Fatal(err)
	}
	path := "item_coordinates/wikidatawiki-20240401-item_coordinates.zst"
	got, err := s3.ReadLines(path)
	if err != nil {
		t.Fatal(err)
	}

	// Moon coordinates, pages without items and NULL coordinates
	// are left out. Sorted by tile, with the polar coordinate last.
	want := []string{
		"72,47.28,8.4",
		"72,47.37444444,8.54111111",
		"662541,46.51,8.31",
		"5649951,89.9,0",
	}
	if !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}

	stored, err := storedItemCoordinates(ctx, sites, s3)
	if err != nil {
		t.Fatal(err)
	}
	if stored != path {
		t.Errorf("got %q, want %q", stored, path)
	}

	// A second run finds the file in storage.
	s3.data[path] = []byte("sentinel")
	if err := buildItemCoordinates(ctx, dumps, sites, s3); err != nil {
		t.Fatal(err)
	}
	if string(s3.data[path]) != "sentinel" {
		t.Error("buildItemCoordinates() should not re-build stored coordinates")
	}
}

func TestStoredItemCoordinates_None(t *testing.T) {
	dumped, _ := time.Parse(time.DateOnly, "2024-04-01")
	site := &WikiSite{Key: "wikidatawiki", Domain: "www.wikidata.org", LastDumped: dumped}
	sites := &WikiSites{Sites: map[string]*WikiSite{"wikidatawiki": site}}
	s3 := NewFakeS3()
	s3.data["item_coordinates/wikidatawiki-20240101-item_coordinates.zst"] = []byte("old")
	got, err := storedItemCoordinates(context.Background(), sites, s3)
	if err != nil || got != "" {
		t.Errorf(`got (%q, %v), want ("", nil)`, got, err)
	}
}
