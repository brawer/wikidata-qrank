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

func TestBuildTitles(t *testing.T) {
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
	if err := buildPageSignals(site, ctx, dumps, s3); err != nil {
		t.Fatal(err)
	}
	if err := buildTitles(site, ctx, dumps, s3); err != nil {
		t.Fatal(err)
	}

	got, err := s3.ReadLines("titles/rmwiki-20240301-titles.zst")
	if err != nil {
		t.Fatal(err)
	}

	want := []string{
		"Obergesteln	Q662541",
		"Turitg	Q72",
		"namespace-4:Pagina_principala	Q5296",
	}

	if !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}
