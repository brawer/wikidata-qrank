// SPDX-FileCopyrightText: 2024 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"context"
	"log"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/lanrat/extsort"
)

func TestItemSignalsAdd(t *testing.T) {
	s := ItemSignals{72, 1, 2, 3, 4, 5}
	s.Add(ItemSignals{72, 2, 2, 2, 2, 2})
	want := ItemSignals{72, 3, 4, 5, 6, 7}
	if !reflect.DeepEqual(s, want) {
		t.Errorf("got %v, want %v", s, want)
	}
}

func TestItemSignalsClear(t *testing.T) {
	s := ItemSignals{1, 2, 3, 4, 5, 6}
	s.Clear()
	want := ItemSignals{}
	if !reflect.DeepEqual(s, want) {
		t.Errorf("got %v, want %v", s, want)
	}
}

func TestItemSignalsToBytes(t *testing.T) {
	// Serialize and then de-serialize an ItemSignals struct.
	a := ItemSignals{1, 2, 3, 4, 5, 6}
	got := ItemSignalsFromBytes(a.ToBytes()).(ItemSignals)
	if !reflect.DeepEqual(got, a) {
		t.Errorf("got %v, want %v", got, a)
	}
}

func TestItemSignalsLess(t *testing.T) {
	for _, tc := range []struct {
		a    string
		b    string
		want bool
	}{
		{"123456", "123456", false},
		{"923456", "123456", false},
		{"123456", "923456", true},

		{"------", "------", false},
		{"7-----", "------", false},
		{"-7----", "------", false},
		{"--7---", "------", false},
		{"---7--", "------", false},
		{"----7-", "------", false},
		{"-----7", "------", false},
		{"------", "7-----", true},
		{"------", "-7----", true},
		{"------", "--7---", true},
		{"------", "---7--", true},
		{"------", "----7-", true},
		{"------", "-----7", true},
	} {
		a := ItemSignals{
			item:          int64(tc.a[0]),
			pageviews:     int64(tc.a[1]),
			wikitextBytes: int64(tc.a[2]),
			claims:        int64(tc.a[3]),
			identifiers:   int64(tc.a[4]),
			sitelinks:     int64(tc.a[5]),
		}
		b := ItemSignals{
			item:          int64(tc.b[0]),
			pageviews:     int64(tc.b[1]),
			wikitextBytes: int64(tc.b[2]),
			claims:        int64(tc.b[3]),
			identifiers:   int64(tc.b[4]),
			sitelinks:     int64(tc.b[5]),
		}
		got := ItemSignalsLess(a, b)
		if got != tc.want {
			t.Errorf("got %v, want %v, for ItemSignalsLess(%#v, %#v)", got, tc.want, a, b)
		}
	}
}

func TestBuildItemSignals(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	logger = log.New(&bytes.Buffer{}, "", log.Lshortfile)
	ctx := context.Background()
	s3 := NewFakeS3()

	// Store fake pageviews in fake S3 storage.
	pageviewsW07 := []string{
		"rm.wikipedia,1,314159267", // Q5296
		"rm.wikipedia,3824,3",      // Q662541
		"rm.wikipedia,799,1111",    // Q72
		"www.wikidata,200,28",      // Q72
	}
	pageviewsW08 := []string{
		"rm.wikipedia,3824,2",    // Q662541
		"rm.wikipedia,799,4444",  // Q72
		"rm.wikipedia,9999,9999", // no wikidata item
		"www.wikidata,200,2",     // Q72
	}
	pageviews := []string{
		"pageviews/pageviews-2011-W07.zst",
		"pageviews/pageviews-2011-W08.zst",
	}
	s3.WriteLines(pageviewsW07, pageviews[0])
	s3.WriteLines(pageviewsW08, pageviews[1])

	// Store fake page_signals in fake S3 storage.
	rmwiki := []string{
		"1,Q5296,2500",
		"3824,Q662541,4973",
		"799,Q72,3142",
	}
	wdwiki := []string{
		"1,Q107661323,3470",
		"19441465,Q5296,372",
		"200,Q72,,550,85,186",
		"5411171,Q5649951,,1,,20",
		"623646,Q662541,,32,9,15",
	}
	s3.WriteLines(rmwiki, "page_signals/rmwiki-20111209-page_signals.zst")
	s3.WriteLines(wdwiki, "page_signals/wikidatawiki-20110403-page_signals.zst")
	rmDumped, _ := time.Parse(time.DateOnly, "2011-12-09")
	wdDumped, _ := time.Parse(time.DateOnly, "2011-04-03")
	rmwikiSite := &WikiSite{Key: "rmwiki", Domain: "rm.wikipedia.org", LastDumped: rmDumped}
	wikidatawikiSite := &WikiSite{Key: "wikidatawiki", Domain: "www.wikidata.org", LastDumped: wdDumped}
	sites := &WikiSites{
		Sites:   map[string]*WikiSite{"rmwiki": rmwikiSite, "wikidatawiki": wikidatawikiSite},
		Domains: map[string]*WikiSite{"rm.wikipedia.org": rmwikiSite, "www.wikidata.org": wikidatawikiSite},
	}

	date, err := buildItemSignals(ctx, pageviews, sites, s3)
	if err != nil {
		t.Error(err)
	}
	gotDate := date.Format(time.DateOnly)
	wantDate := "2011-12-09"
	if gotDate != wantDate {
		t.Errorf("got %s, want %s", gotDate, wantDate)
	}

	got, err := s3.ReadLines("public/item_signals-20111209.csv.zst")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"item,pageviews_52w,wikitext_bytes,claims,identifiers,sitelinks",
		"Q72,5585,3142,550,85,186",
		"Q5296,314159267,2872,0,0,0",
		"Q662541,5,4973,32,9,15",
		"Q5649951,0,0,1,0,20",
		"Q107661323,0,3470,0,0,0",
	}
	if !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

// If the most recent pageview file is newer than the last dump
// of any Wikimedia site, ItemSignalsVersion() should return
// the last day of the week of the most recent pageviews file.
func TestItemSignalsVersion_FreshPageviews(t *testing.T) {
	pageviews := []string{
		"pageviews/pageviews-2023-W17.zst",
		"pageviews/pageviews-2023-W18.zst",
		"pageviews/pageviews-2023-W19.zst",
	}

	enDumped, _ := time.Parse(time.DateOnly, "2011-12-31")
	rmDumped, _ := time.Parse(time.DateOnly, "2011-11-11")
	enwikiSite := &WikiSite{Key: "enwiki", Domain: "en.wikipedia.org", LastDumped: enDumped}
	rmwikiSite := &WikiSite{Key: "rmwiki", Domain: "rm.wikipedia.org", LastDumped: rmDumped}
	sites := &WikiSites{
		Sites:   map[string]*WikiSite{"enwiki": enwikiSite, "rmwiki": rmwikiSite},
		Domains: map[string]*WikiSite{"en.wikipedia.org": enwikiSite, "rm.wikipedia.org": rmwikiSite},
	}

	got := ItemSignalsVersion(pageviews, sites).Format(time.DateOnly)
	want := "2023-05-14"
	if got != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

// If the most recent pageview file is older than the last dump
// of any Wikimedia site, ItemSignalsVersion() should return
// the date of the most recent Wikimedia site dump.
func TestItemSignalsVersion_OldPageviews(t *testing.T) {
	pageviews := []string{
		"pageviews/pageviews-2003-W17.zst",
		"pageviews/pageviews-2003-W18.zst",
		"pageviews/pageviews-2003-W19.zst",
	}

	enDumped, _ := time.Parse(time.DateOnly, "2011-12-31")
	rmDumped, _ := time.Parse(time.DateOnly, "2011-11-11")
	enwikiSite := &WikiSite{Key: "enwiki", Domain: "en.wikipedia.org", LastDumped: enDumped}
	rmwikiSite := &WikiSite{Key: "rmwiki", Domain: "rm.wikipedia.org", LastDumped: rmDumped}
	sites := &WikiSites{
		Sites:   map[string]*WikiSite{"enwiki": enwikiSite, "rmwiki": rmwikiSite},
		Domains: map[string]*WikiSite{"en.wikipedia.org": enwikiSite, "rm.wikipedia.org": rmwikiSite},
	}

	got := ItemSignalsVersion(pageviews, sites).Format(time.DateOnly)
	want := "2011-12-31"
	if got != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

func TestStoredItemSignalsVersion(t *testing.T) {
	s3 := NewFakeS3()
	got, err := StoredItemSignalsVersion(context.Background(), s3)
	if err != nil {
		t.Error(err)
	}
	if !got.IsZero() {
		t.Errorf("got %s, want zero", got.Format(time.DateOnly))
	}
	s3.data["public/item_signals-20230815.csv.zst"] = []byte("foo")
	s3.data["public/item_signals-20240131.csv.zst"] = []byte("bar")
	got, err = StoredItemSignalsVersion(context.Background(), s3)
	if err != nil {
		t.Error(err)
	}
	want, _ := time.Parse("2024-01-31", time.DateOnly)
	if got != want {
		t.Errorf("got %s, want 2024-01-31", got.Format(time.DateOnly))
	}
}

func TestItemSignalsJoiner(t *testing.T) {
	ch := make(chan extsort.SortType, 20)
	joiner := itemSignalsJoiner{out: ch}
	for _, line := range []string{
		"test.wikipedia,1,99",
		"test.wikipedia,200,198",
		"test.wikipedia,200,3",
		"test.wikipedia,200,Q72,4,550,85,186",
		"test.wikipedia,3824,Q662541,4973",
	} {
		if err := joiner.Process(line); err != nil {
			t.Error(err)
		}
	}
	joiner.Close()
	got := make([]ItemSignals, 0, 20)
	for s := range ch {
		got = append(got, s.(ItemSignals))
	}
	want := []ItemSignals{
		ItemSignals{72, 201, 4, 550, 85, 186},
		ItemSignals{662541, 0, 4973, 0, 0, 0},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}
