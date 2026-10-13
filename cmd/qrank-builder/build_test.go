// SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"maps"
	"net/http"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"testing"
	"time"
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
	if err := Build(client, dumps /*numWeeks*/, 2, s3); err != nil {
		t.Fatal(err)
	}

	for _, week := range []string{"2023-W11", "2023-W12"} {
		path := "pageviews/pageviews-" + week + ".zst"
		if _, found := s3.data[path]; !found {
			t.Errorf("Build() should store %s", path)
		}
	}

	// Every input was written by this run, so there is a single producer.
	date, _ := time.Parse(time.DateOnly, "2024-05-01")
	prov, err := readItemSignalsProvenance(context.Background(), date, s3)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(prov.Software, []string{SoftwareVersion}) {
		t.Errorf("got provenance software %v, want [%s]", prov.Software, SoftwareVersion)
	}
	if prov.Dumps.Newest != "2024-05-01" || prov.Pageviews.Weeks != 2 {
		t.Errorf("got provenance %+v", prov)
	}

	got, err := s3.ReadLines("public/item_signals-20240501.csv.zst")
	if err != nil {
		t.Fatal(err)
	}

	want := []string{
		"item,pageviews_52w,wikitext_bytes,claims,identifiers,sitelinks",
		"Q72,3,3142,550,85,186",
		"Q5296,4,2872,0,0,0",
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

	// Sorted by decreasing QRank; ties by increasing numeric entity ID,
	// so Q72 comes before Q662541. Items without pageviews are omitted.
	qrank, err := gunzipString(s3.data["public/qrank-20240501.csv.gz"])
	if err != nil {
		t.Fatal(err)
	}
	if want := "Entity,QRank\nQ5296,4\nQ72,3\nQ662541,3\n"; qrank != want {
		t.Errorf("got %q, want %q", qrank, want)
	}

	// Median is the index of the median item in Samples.
	stats := string(s3.data["public/qrank-stats-20240501.json"])
	wantStats := `{"Median":1,"Samples":[["Q5296",1,4],["Q72",2,3],["Q662541",3,3]]}`
	if stats != wantStats {
		t.Errorf("got stats %s, want %s", stats, wantStats)
	}

	// A second run on the same inputs should find everything fresh
	// and leave the stored files exactly as they were.
	before := make(map[string][]byte, len(s3.data))
	for path, content := range s3.data {
		before[path] = content
	}
	if err := Build(client, dumps /*numWeeks*/, 2, s3); err != nil {
		t.Fatal(err)
	}
	if !maps.EqualFunc(before, s3.data, bytes.Equal) {
		for path := range s3.data {
			if _, found := before[path]; !found {
				t.Errorf("second Build() added %s", path)
			} else if !bytes.Equal(before[path], s3.data[path]) {
				t.Errorf("second Build() changed %s", path)
			}
		}
		for path := range before {
			if _, found := s3.data[path]; !found {
				t.Errorf("second Build() removed %s", path)
			}
		}
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
		fake := s3.(*FakeS3)
		fake.mutex.Lock()
		defer fake.mutex.Unlock()
		fake.data[path] = []byte("fresh-" + ymd[:4])
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
