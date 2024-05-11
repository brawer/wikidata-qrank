// SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
)

func TestBuildPageEntities(t *testing.T) {
	logger = log.New(&bytes.Buffer{}, "", log.Lshortfile)
	ctx := context.Background()
	dumps := filepath.Join("testdata", "dumps")
	s3 := NewFakeS3()
	s3.data["page_entities/loginwiki-20240501-page_entities.zst"] = []byte("old-loginwiki")
	s3.data["page_entities/rmwiki-20010203-page_entities.zst"] = []byte("old-2001")
	s3.data["page_entities/rmwiki-20020203-page_entities.zst"] = []byte("old-2002")
	s3.data["page_entities/rmwiki-20030203-page_entities.zst"] = []byte("old-2003")
	s3.data["page_entities/rmwiki-20040203-page_entities.zst"] = []byte("old-2004")
	s3.data["page_entities/rmwiki-20050203-page_entities.zst"] = []byte("old-2005")
	sites, err := ReadWikiSites(dumps)
	if err != nil {
		t.Fatal(err)
	}
	if err := buildPageEntities(ctx, dumps, sites, s3); err != nil {
		t.Fatal(err)
	}

	// page_entities should be cached across pipeline runs
	// https://github.com/brawer/wikidata-qrank/issues/33
	got := string(s3.data["page_entities/loginwiki-20240501-page_entities.zst"])
	want := "old-loginwiki"
	if got != want {
		t.Errorf("should not re-compute previously stored page_entities")
	}

	// For rmwiki-2024, new data should have been computed and put in storage.
	// Make sure that data looks as expected.
	gotLines, err := s3.ReadLines("page_entities/rmwiki-20240301-page_entities.zst")
	if err != err {
		t.Fatal(err)
	}
	got = strings.Join(gotLines, " | ")
	want = "1,Q5296 | 3824,Q662541 | 799,Q72"
	if got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	// For Wikidata, the mapping from page-id to wikidata-id needs to
	// be taken from two sources. As with other wikis, table `page_props`
	// has some mappings, but for Wikidata that only contains a few templates
	// and similar internal pages. To find the wikidata-ids of pages
	// in wikidatawiki, we also need to process the SQL dumps of table `page`.
	// See https://github.com/brawer/wikidata-qrank/issues/35 for background.
	gotLines, err = s3.ReadLines("page_entities/wikidatawiki-20240401-page_entities.zst")
	if err != err {
		t.Fatal(err)
	}
	got = strings.Join(gotLines, " | ")
	want = "1,Q107661323 | 19441465,Q5296 | 200,Q72 | 5411171,Q5649951 | 623646,Q662541"
	if got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	// Verify that obsolete files have been cleaned up.
	stored, err := storedPageEntities(context.Background(), s3)
	if err != nil {
		t.Error(err)
	}
	got = strings.Join(stored["rmwiki"], " ")
	want = "20040203 20050203 20240301"
	if got != want {
		t.Errorf(`should clean up old page_entities; got "%s", want "%s"`, got, want)
	}
}

func TestStoredPageEntitites(t *testing.T) {
	s3 := NewFakeS3()
	for _, path := range []string{
		"page_entities/alswikibooks-20010203-page_entities.zst",
		"page_entities/alswikibooks-20050607-page_entities.zst",
		"page_entities/rmwiki-20241122-page_entities.zst",
		"page_entities/junk.txt",
	} {
		s3.data[path] = []byte("content")
	}
	got, err := storedPageEntities(context.Background(), s3)
	if err != nil {
		t.Error(err)
	}
	want := map[string][]string{
		"alswikibooks": {"20010203", "20050607"},
		"rmwiki":       {"20241122"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestPageEntitiesScanner(t *testing.T) {
	s3 := NewFakeS3()
	storeFakePageEntities("enwiki-20111231", "1,Q111\n7,Q777\n", s3, t)
	storeFakePageEntities("rmwiki-20110203", "1,Q11\n2,Q22\n3,Q33\n", s3, t)
	storeFakePageEntities("rmwiki-20111111", "1,Q11\n3,Q33\n", s3, t)
	enDumped, _ := time.Parse(time.DateOnly, "2011-12-31")
	rmDumped, _ := time.Parse(time.DateOnly, "2011-11-11")
	sites := map[string]WikiSite{
		"en.wikipedia.org": WikiSite{"enwiki", "en.wikipedia.org", enDumped},
		"rm.wikipedia.org": WikiSite{"rmwiki", "rm.wikipedia.org", rmDumped},
	}

	got := make([]string, 0, 10)
	scanner := NewPageEntitiesScanner(&sites, s3)
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

// StoreFakePageEntities is a helper for TestPageEntitiesScanner().
func storeFakePageEntities(id string, content string, s3 *FakeS3, t *testing.T) {
	var buf bytes.Buffer
	zstdLevel := zstd.WithEncoderLevel(zstd.SpeedFastest)
	writer, err := zstd.NewWriter(&buf, zstdLevel)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte(content)); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	path := fmt.Sprintf("page_entities/%s-page_entities.zst", id)
	s3.data[path] = buf.Bytes()
}
