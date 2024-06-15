// SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net/http"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"testing"
)

// TestBuild is a large integration test that runs the entire pipeline.
func TestBuild(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	logger = log.New(&bytes.Buffer{}, "", log.Lshortfile)
	dumps := filepath.Join("testdata", "dumps")
	client := &http.Client{Transport: &FakeWikiSite{}}
	s3 := NewFakeS3()
	if err := Build(client, dumps /*numWeeks*/, 1, s3); err != nil {
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
		"Q54321,0,23,0,0,0",
		"Q54322,0,24,0,0,0",
		"Q662541,3,4973,32,9,15",
		"Q4847311,0,0,0,0,0",
		"Q5649951,0,0,1,0,20",
		"Q8681970,0,5678,0,0,0",
		"Q107661323,0,3470,0,0,0",
	}

	if !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestBuildSiteFiles(t *testing.T) {
	logger = log.New(&bytes.Buffer{}, "", log.Lshortfile)
	ctx := context.Background()
	s3 := NewFakeS3()
	s3.data["foobar/itwikibooks-20240301-foobar.zst"] = []byte("old-2024")
	s3.data["foobar/loginwiki-20030203-foobar.zst"] = []byte("old-2003")
	s3.data["foobar/rmwiki-20010203-foobar.zst"] = []byte("old-2001")
	s3.data["foobar/rmwiki-20020203-foobar.zst"] = []byte("old-2002")
	s3.data["foobar/rmwiki-20030203-foobar.zst"] = []byte("old-2003")

	dumps := filepath.Join("testdata", "dumps")
	sites, err := ReadWikiSites(nil, dumps)
	if err != nil {
		t.Fatal(err)
	}

	buildFunc := func(site *WikiSite, ctx context.Context, dumps string, s3 S3) error {
		ymd := site.LastDumped.Format("20060102")
		path := fmt.Sprintf("foobar/%s-%s-foobar.zst", site.Key, ymd)
		s3.(*FakeS3).data[path] = []byte("fresh-" + ymd[:4])
		return nil
	}

	if err := buildSiteFiles(ctx, "foobar", buildFunc, dumps, sites, s3); err != nil {
		t.Fatal(err)
	}

	got := make([]string, 0, len(s3.data))
	for path, value := range s3.data {
		got = append(got, fmt.Sprintf("%s = %s", path, string(value)))
	}
	sort.Strings(got)

	want := []string{
		"foobar/itwikibooks-20240301-foobar.zst = old-2024",
		"foobar/loginwiki-20030203-foobar.zst = old-2003",
		"foobar/loginwiki-20240501-foobar.zst = fresh-2024",
		"foobar/rmwiki-20020203-foobar.zst = old-2002",
		"foobar/rmwiki-20030203-foobar.zst = old-2003",
		"foobar/rmwiki-20240301-foobar.zst = fresh-2024",
		"foobar/rmwikibooks-20240301-foobar.zst = fresh-2024",
		"foobar/wikidatawiki-20240401-foobar.zst = fresh-2024",
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %q, want %q", got, want)
	}
}
