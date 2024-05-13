// SPDX-FileCopyrightText: 2024 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"context"
	"log"
	"slices"
	"testing"
	"time"
)

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
