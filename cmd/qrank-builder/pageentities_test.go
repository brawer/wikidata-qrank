// SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
)

func TestBuildPageEntities(t *testing.T) {
	logger = log.New(&bytes.Buffer{}, "", log.Lshortfile)
	ctx := context.Background()
	dumps := filepath.Join("testdata", "dumps")
	s3 := NewFakeS3()
	s3.data["page_entities/rmwiki-20010203-page_entities.zst"] = []byte("old")
	sites, err := ReadWikiSites(dumps)
	if err != nil {
		t.Fatal(err)
	}
	if err := buildPageEntities(ctx, dumps, sites, s3); err != nil {
		t.Fatal(err)
	}

	path := "page_entities/rmwiki-20240301-page_entities.zst"
	reader, err := zstd.NewReader(bytes.NewReader(s3.data[path]))
	if err != nil {
		t.Error(err)
	}
	defer reader.Close()

	var buf bytes.Buffer
	if _, err = io.Copy(&buf, reader); err != nil {
		t.Error(err)
	}
	got := buf.String()
	want := "1,Q5296\n3824,Q662541\n799,Q72\n"
	if got != want {
		t.Errorf("got %v, want %v", got, want)
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
