// SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestBuildPageSignals(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	logger = log.New(&bytes.Buffer{}, "", log.Lshortfile)
	ctx := context.Background()
	dumps := filepath.Join("testdata", "dumps")
	s3 := NewFakeS3()
	sites, err := ReadWikiSites(dumps)
	if err != nil {
		t.Fatal(err)
	}
	for _, site := range []string{"rmwiki", "wikidatawiki"} {
		if err := buildPageSignals((*sites)[site], ctx, dumps, s3); err != nil {
			t.Fatal(err)
		}
	}

	gotLines, err := s3.ReadLines("page_signals/rmwiki-20240301-page_signals.zst")
	if err != err {
		t.Fatal(err)
	}
	wantLines := []string{
		"1,Q5296,2500",
		"3824,Q662541,4973",
		"799,Q72,3142",
	}
	if !slices.Equal(gotLines, wantLines) {
		t.Errorf("got %v, want %v", gotLines, wantLines)
	}

	// For Wikidata, the mapping from page-id to wikidata-id needs to
	// be taken from two sources. As with other wikis, table `page_props`
	// has some mappings, but for Wikidata that only contains a few templates
	// and similar internal pages. To find the wikidata-ids of pages
	// in wikidatawiki, we also need to process the SQL dumps of table `page`.
	// See https://github.com/brawer/wikidata-qrank/issues/35 for background.
	gotLines, err = s3.ReadLines("page_signals/wikidatawiki-20240401-page_signals.zst")
	if err != err {
		t.Fatal(err)
	}
	wantLines = []string{
		"1,Q107661323,3470",
		"19441465,Q5296,372",
		"200,Q72,,550,85,186",
		"5411171,Q5649951,,1,,20",
		"623646,Q662541,,32,9,15",
	}
	if !slices.Equal(gotLines, wantLines) {
		t.Errorf("got %v, want %v", gotLines, wantLines)
	}
}

func TestPageSignalsScanner(t *testing.T) {
	logger = log.New(&bytes.Buffer{}, "", log.Lshortfile)
	s3 := NewFakeS3()
	storeFakePageSignals("enwiki-20111231", "1,Q111|7,Q777", s3, t)
	storeFakePageSignals("rmwiki-20110203", "1,Q11|2,Q22|3,Q33", s3, t)
	storeFakePageSignals("rmwiki-20111111", "1,Q11|3,Q33", s3, t)
	enDumped, _ := time.Parse(time.DateOnly, "2011-12-31")
	rmDumped, _ := time.Parse(time.DateOnly, "2011-11-11")
	sites := map[string]WikiSite{
		"en.wikipedia.org": WikiSite{"enwiki", "en.wikipedia.org", enDumped},
		"rm.wikipedia.org": WikiSite{"rmwiki", "rm.wikipedia.org", rmDumped},
	}

	got := make([]string, 0, 10)
	scanner := NewPageSignalsScanner(&sites, s3)
	for scanner.Scan() {
		got = append(got, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		t.Error(err)
	}
	want := []string{
		"en.wikipedia,1,Q111",
		"en.wikipedia,7,Q777",
		"rm.wikipedia,1,Q11",
		"rm.wikipedia,3,Q33",
	}
	if !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

// StoreFakePageSignals is a helper for TestPageSignalsScanner().
func storeFakePageSignals(id string, content string, s3 *FakeS3, t *testing.T) {
	lines := strings.Split(content, "|")
	path := fmt.Sprintf("page_signals/%s-page_signals.zst", id)
	if err := s3.WriteLines(lines, path); err != nil {
		t.Error(err)
	}
}

func TestPageSignalMerger(t *testing.T) {
	logger = log.New(&bytes.Buffer{}, "", log.Lshortfile)
	var buf strings.Builder
	writer := TestingWriteCloser(&buf)
	m := NewPageSignalMerger(writer)
	for _, line := range []string{
		"11,s=1111111",
		"22,Q72",
		"22,s=830167",
		"333,Q3",
	} {
		if err := m.Process(line); err != nil {
			t.Error(err)
		}
	}
	if writer.closed {
		t.Errorf("PageSignalMerger.Close() closed output writer prematurely")
	}
	if err := m.Close(); err != nil {
		t.Error(err)
	}
	if !writer.closed {
		t.Errorf("PageSignalMerger.Close() should close output writer")
	}
	got := strings.Split(strings.TrimSuffix(buf.String(), "\n"), "\n")
	want := []string{
		"22,Q72,830167",
		"333,Q3,",
	}
	if !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}
