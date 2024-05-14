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
	logger = log.New(&bytes.Buffer{}, "", log.Lshortfile)
	ctx := context.Background()
	s3 := NewFakeS3()
	pageviewsW07 := []string{
		"rm.wikipedia,3824,3",
		"rm.wikipedia,799,1111",
	}
	pageviewsW08 := []string{
		"rm.wikipedia,3824,2",
		"rm.wikipedia,799,4444",
		"rm.wikipedia,9999,9999",
	}
	pageviews := []string{
		"pageviews/pageviews-2011-W07.zst",
		"pageviews/pageviews-2011-W08.zst",
	}
	s3.WriteLines(pageviewsW07, pageviews[0])
	s3.WriteLines(pageviewsW08, pageviews[1])

	rmDumped, _ := time.Parse(time.DateOnly, "2011-12-09")
	sites := &map[string]WikiSite{
		"rm.wikipedia.org": WikiSite{"rmwiki", "rm.wikipedia.org", rmDumped},
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

	got, err := s3.ReadLines("public/signals-20111209.csv.zst")
	if err != nil {
		t.Error(err)
	}
	want := []string{
		"item,pageviews,wikitext_bytes,claims,identifiers,sitelinks",
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
	sites := map[string]WikiSite{
		"en.wikipedia.org": WikiSite{"enwiki", "en.wikipedia.org", enDumped},
		"rm.wikipedia.org": WikiSite{"rmwiki", "rm.wikipedia.org", rmDumped},
	}

	got := ItemSignalsVersion(pageviews, &sites).Format(time.DateOnly)
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
	sites := map[string]WikiSite{
		"en.wikipedia.org": WikiSite{"enwiki", "en.wikipedia.org", enDumped},
		"rm.wikipedia.org": WikiSite{"rmwiki", "rm.wikipedia.org", rmDumped},
	}

	got := ItemSignalsVersion(pageviews, &sites).Format(time.DateOnly)
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
	s3.data["public/20230815-signals.zst"] = []byte("foo")
	s3.data["public/20240131-signals.zst"] = []byte("bar")
	got, err = StoredItemSignalsVersion(context.Background(), s3)
	if err != nil {
		t.Error(err)
	}
	want, _ := time.Parse("2024-01-31", time.DateOnly)
	if got != want {
		t.Errorf("got %s, want 2024-01-31", got.Format(time.DateOnly))
	}
}
