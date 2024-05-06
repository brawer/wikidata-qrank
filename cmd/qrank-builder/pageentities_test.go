// SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"context"
	"io"
	"log"
	"path/filepath"
	"reflect"
	"testing"

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