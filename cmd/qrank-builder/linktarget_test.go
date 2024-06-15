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
	"strings"
	"testing"

	"golang.org/x/sync/errgroup"
)

func TestJoinLinkTargets(t *testing.T) {
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

	site := sites.Sites["itwikibooks"]
	got := make([]string, 0, 5)
	ch := make(chan string, 5)
	group, _ := errgroup.WithContext(context.Background())
	group.Go(func() error {
		defer close(ch)
		return joinLinkTargets(ctx, site, "Prop", dumps, ch)
	})
	if err := group.Wait(); err != nil {
		t.Fatal(err)
	}
	for line := range ch {
		line = strings.Join(strings.Split(line, "\t"), "|")
		got = append(got, line)
	}

	want := []string{
		"123456|Prop|Allegra,_allegra",
		"123456|Prop|In_chaschiel_ed_ina_nursa",
	}
	if !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}
