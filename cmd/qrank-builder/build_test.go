// SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"log"
	"path/filepath"
	"slices"
	"testing"
)

// TestBuild is a large integration test that runs the entire pipeline.
func TestBuild(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	logger = log.New(&bytes.Buffer{}, "", log.Lshortfile)
	dumps := filepath.Join("testdata", "dumps")
	s3 := NewFakeS3()
	if err := Build(dumps /*numWeeks*/, 1, s3); err != nil {
		t.Fatal(err)
	}

	got, err := s3.ReadLines("public/item_signals-20240501.csv.zst")
	if err != nil {
		t.Fatal(err)
	}

	want := []string{
		"item,pageviews_52w,wikitext_bytes,claims,identifiers,sitelinks",
		"Q72,0,3142,550,85,186",
		"Q5296,0,2872,0,0,0",
		"Q662541,3,4973,32,9,15",
		"Q5649951,0,0,1,0,20",
		"Q107661323,0,3470,0,0,0",
	}

	if !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}
