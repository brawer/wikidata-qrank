// SPDX-FileCopyrightText: 2024 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func TestItemString(t *testing.T) {
	for _, item := range []string{"Q1", "Q12345", "L1", "L12345"} {
		got := ParseItem(item)
		if got.String() != item {
			t.Errorf("ParseItem(%q): got %s, want %s", item, got.String(), item)
		}
	}
	for _, s := range []string{"", "junk", "q7", "l1", "Q8x", "L123-S3"} {
		got := ParseItem(s)
		if got != NoItem {
			t.Errorf("ParseItem(%q): got %s, want NoItem", s, got.String())
		}
	}
}

func TestLatestDump(t *testing.T) {
	dir := filepath.Join("testdata", "dumps", "other", "pageview_complete")
	re := regexp.MustCompile(`^pageviews-(\d{8})-user\.bz2$`)
	got, err := LatestDump(dir, re)
	if err != nil {
		t.Error(err)
	}

	want := filepath.Join(dir, "2023", "2023-03", "pageviews-20230326-user.bz2")
	if got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestLatestDump_NoMatches(t *testing.T) {
	dir := filepath.Join("testdata", "dumps", "other", "pageview_complete")
	re := regexp.MustCompile(`^nosuchdump-(\d{8})\.bz2$`)
	if _, err := LatestDump(dir, re); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("got %v, want fs.ErrNotExist", err)
	}
}

func TestLatestDump_NoSuchDir(t *testing.T) {
	dir := filepath.Join("testdata", "dumps", "other", "no_such_dir")
	re := regexp.MustCompile(`^dump-(\d{8})\.bz2$`)
	if _, err := LatestDump(dir, re); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("got %v, want fs.ErrNotExist", err)
	}
}

func TestParseISOWeek(t *testing.T) {
	year, week, err := ParseISOWeek("2023-W07")
	if err != nil {
		t.Error(err)
	} else if year != 2023 || week != 7 {
		t.Errorf("want (2023, 7), got (%d, %d)", year, week)
	}
}

func TestParseISOWeek_BadFormat(t *testing.T) {
	if _, _, err := ParseISOWeek("2023-12-24"); err == nil {
		t.Error("want error, got nil")
	}
}

func ExampleParseISOWeek() {
	fmt.Println(ParseISOWeek("2018-W51")) // Output: 2018 51 <nil>
}

func TestISOWeekStart(t *testing.T) {
	for _, tc := range []struct {
		year     int
		day      int
		expected string
	}{
		{2018, -1, "2017-12-18"},
		{2018, 0, "2017-12-25"},
		{2018, 1, "2018-01-01"},
		{2018, 2, "2018-01-08"},
		{2019, 1, "2018-12-31"},
		{2019, 2, "2019-01-07"},
		{2019, 53, "2019-12-30"},
		{2019, 54, "2020-01-06"},
	} {
		got := ISOWeekStart(tc.year, tc.day).Format("2006-01-02")
		if tc.expected != got {
			t.Errorf("want ISOWeekStart(%d, %d) = %s, got %s", tc.year, tc.day, tc.expected, got)
		}
	}
}

func TestSortLines(t *testing.T) {
	unsorted, _ := os.CreateTemp("", "*.txt")
	unsorted.Close()
	defer os.Remove(unsorted.Name())
	os.WriteFile(unsorted.Name(), []byte("C\nB\nA\n"), 0666)

	ctx := context.Background()
	gotPath, err := SortLines(ctx, unsorted.Name())
	if err != nil {
		t.Fatal(err)
	}

	gotFile, err := os.Open(gotPath)
	if err != nil {
		t.Fatal(err)
	}
	defer gotFile.Close()

	decoder, _ := zstd.NewReader(gotFile, zstd.WithDecoderConcurrency(0))
	defer decoder.Close()
	gotBytes, err := io.ReadAll(decoder)
	if err != nil {
		t.Fatal(err)
	}

	got := strings.Split(strings.TrimSuffix(string(gotBytes), "\n"), "\n")
	want := []string{"A", "B", "C"}
	if !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}
