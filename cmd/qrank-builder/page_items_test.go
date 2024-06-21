// SPDX-FileCopyrightText: 2024 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"
)

func TestPageItemToBytes(t *testing.T) {
	// Serialize and then de-serialize a PageItem struct.
	pi := PageItem{Page: 111, Item: ParseItem("L123")}
	got := PageItemFromBytes(pi.ToBytes()).(PageItem)
	if !reflect.DeepEqual(got, pi) {
		t.Errorf("got %v, want %v", got, pi)
	}
}

func TestPageItemLess(t *testing.T) {
	type testcase struct {
		a, b PageItem
		want bool
	}
	for _, tc := range []testcase{
		{PageItem{1, Item(1)}, PageItem{1, Item(1)}, false},
		{PageItem{1, Item(5)}, PageItem{1, Item(1)}, false},
		{PageItem{1, Item(1)}, PageItem{5, Item(1)}, true},
		{PageItem{1, Item(1)}, PageItem{1, Item(5)}, true},
		{PageItem{1, Item(1)}, PageItem{5, Item(5)}, true},
	} {
		if got := PageItemLess(tc.a, tc.b); got != tc.want {
			t.Errorf("got %v for %v, want %v", got, tc, tc.want)
		}
	}
}

func TestBuildPageItems(t *testing.T) {
	ctx := context.Background()
	dumped, _ := time.Parse(time.DateOnly, "2024-03-01")
	rmwiki := &WikiSite{Key: "rmwiki", Domain: "rm.wikipedia.org", LastDumped: dumped}
	dumps := filepath.Join("testdata", "dumps")

	path, err := buildPageItems(ctx, rmwiki, dumps)
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(path)
	got := readPageItemsForTesting(path, t)
	want := []string{"1,Q5296", "799,Q72", "3824,Q662541"}
	if !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestBuildPageItemsForWikidata(t *testing.T) {
	ctx := context.Background()
	dumped, _ := time.Parse(time.DateOnly, "2024-04-01")
	site := &WikiSite{Key: "wikidatawiki", Domain: "www.wikidata.org", LastDumped: dumped}
	dumps := filepath.Join("testdata", "dumps")

	path, err := buildPageItems(ctx, site, dumps)
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(path)
	got := readPageItemsForTesting(path, t)
	want := []string{
		"1,Q107661323",
		"200,Q72",
		"623646,Q662541",
		"5411171,Q5649951",
		"19441465,Q5296",
	}
	if !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func readPageItemsForTesting(path string, t *testing.T) []string {
	items := make(chan PageItem, 10)
	result := make([]string, 0, 10)
	group, groupCtx := errgroup.WithContext(context.Background())
	group.Go(func() error {
		return ReadPageItems(groupCtx, path, items)
	})
	group.Go(func() error {
		for pi := range items {
			result = append(result, fmt.Sprintf("%d,%s", pi.Page, pi.Item.String()))
		}
		return nil
	})
	if err := group.Wait(); err != nil {
		t.Fatal(err)
	}
	return result
}
