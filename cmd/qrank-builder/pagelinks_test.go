// SPDX-FileCopyrightText: 2024 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"context"
	"log"
	"net/http"
	"path/filepath"
	"slices"
	"testing"
)

// Test for pagelinks schema with pl_title and pl_namespace columns.
func TestBuildLinksFromTitle(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	logger = log.New(&bytes.Buffer{}, "", log.Lshortfile)
	ctx := context.Background()
	client := &http.Client{Transport: &FakeWikiSite{}}
	dumps := filepath.Join("testdata", "dumps")
	sites, err := ReadWikiSites(client, dumps)
	if err != nil {
		t.Fatal(err)
	}

	site := sites.Sites["rmwiki"]
	s3 := NewFakeS3()

	err = s3.WriteLines([]string{
		"1,Q5296,2500",
		"3824,Q662541,4973",
		"799,Q72,3142",
	}, site.S3Path("page_signals"))
	if err != nil {
		t.Fatal(err)
	}

	err = s3.WriteLines([]string{
		"Chantun_Turitg\tQ11943",
		"Flum\tQ4022",
		"Lai_da_Turitg\tQ14407",
		"Turitg\tQ72",
		"Wikipedia:Bainvegni\tQ17596642",
	}, site.S3Path("titles"))
	if err != nil {
		t.Fatal(err)
	}

	err = s3.WriteLines([]string{"Zürich\tQ72"}, site.S3Path("redirects"))
	if err != nil {
		t.Fatal(err)
	}

	if err := buildLinks(site, ctx, dumps, s3); err != nil {
		t.Fatal(err)
	}

	got, err := s3.ReadLines(site.S3Path("links"))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"Q72,Q4022",
		"Q72,Q11943",
		"Q72,Q14407",
		"Q5296,Q17596642",
		"Q662541,Q72",
	}
	if !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

// Test for pagelinks schema with pl_target_id but no pl_title or pl_namespace.
// https://github.com/brawer/wikidata-qrank/issues/43
func TestBuildLinksFromLinkTarget(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	logger = log.New(&bytes.Buffer{}, "", log.Lshortfile)
	ctx := context.Background()
	client := &http.Client{Transport: &FakeWikiSite{}}

	dumps := filepath.Join("testdata", "dumps")
	sites, err := ReadWikiSites(client, dumps)
	if err != nil {
		t.Fatal(err)
	}

	site := sites.Sites["itwikibooks"]
	s3 := NewFakeS3()

	err = s3.WriteLines([]string{
		"54321,Q54321,23",
		"54322,Q54322,42",
		"123456,Q8681970,5678",
	}, site.S3Path("page_signals"))
	if err != nil {
		t.Fatal(err)
	}

	err = s3.WriteLines([]string{
		"Allegra,_allegra\tQ54321",
		"Categoria:Testi_in_romancio\tQ8681970",
	}, site.S3Path("titles"))
	if err != nil {
		t.Fatal(err)
	}

	err = s3.WriteLines([]string{
		"In_chaschiel_ed_ina_nursa\tQ54322",
	}, site.S3Path("redirects"))
	if err != nil {
		t.Fatal(err)
	}

	if err := buildLinks(site, ctx, dumps, s3); err != nil {
		t.Fatal(err)
	}

	got, err := s3.ReadLines(site.S3Path("links"))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"Q8681970,Q54321",
		"Q8681970,Q54322",
	}
	if !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}
