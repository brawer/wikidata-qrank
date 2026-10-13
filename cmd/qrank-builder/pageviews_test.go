// SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"golang.org/x/sync/errgroup"
)

func TestLatestPageviewsDump(t *testing.T) {
	day, err := LatestPageviewsDump(filepath.Join("testdata", "dumps"))
	if err != nil {
		t.Error(err)
	}
	got, want := day.Format(time.DateOnly), "2023-03-26"
	if got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestLatestPageviewsDump_NoSuchDir(t *testing.T) {
	_, err := LatestPageviewsDump("no_such_dir")
	if !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("got %v, want fs.ErrNotExist", err)
	}
}

func TestPageviewsPath(t *testing.T) {
	want := filepath.Join("foo", "other", "pageview_complete", "2018", "2018-09", "pageviews-20180930-user.bz2")
	date, _ := time.Parse(time.DateOnly, "2018-09-30")
	got := PageviewsPath("foo", date)
	if got != want {
		t.Errorf("want %v, got %v", want, got)
	}
}

func TestBuildPageviews(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	logger = log.New(&bytes.Buffer{}, "", log.Lshortfile)
	ctx := context.Background()
	dumps := filepath.Join("testdata", "dumps")
	s3 := NewFakeS3()
	s3.data["pageviews/pageviews-2011-W51.zst"] = []byte("very old")
	s3.data["pageviews/pageviews-2023-W09.zst"] = []byte("foo")
	s3.data["pageviews/pageviews-2023-W10.zst"] = []byte("bar")
	s3.data["pageviews/pageviews-2023-W11.zst"] = []byte("baz")
	s3.data["pageviews/pageviews-2023-W12.zst"] = []byte("unstamped")
	stamp := map[string]string{softwareMetadataKey: "QRank/v0.0.1"}
	for _, week := range []string{"2023-W09", "2023-W10", "2023-W11"} {
		s3.metadata["pageviews/pageviews-"+week+".zst"] = stamp
	}
	got, err := buildPageviews(ctx, dumps /*numWeeks*/, 4, s3)
	if err != nil {
		t.Error(err)
	}
	want := []string{
		"pageviews/pageviews-2023-W09.zst",
		"pageviews/pageviews-2023-W10.zst",
		"pageviews/pageviews-2023-W11.zst",
		"pageviews/pageviews-2023-W12.zst",
	}
	if !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	// The stored 2023-W12 file has no software metadata, as if it had
	// been written by an older release, so it should get re-built.
	if string(s3.data["pageviews/pageviews-2023-W12.zst"]) == "unstamped" {
		t.Errorf("buildPageviews() should re-build unstamped 2023-W12 file")
	}
	if string(s3.data["pageviews/pageviews-2023-W11.zst"]) != "baz" {
		t.Errorf("buildPageviews() should keep stamped 2023-W11 file")
	}
	if _, found := s3.data["pageviews/pageviews-2011-W51.zst"]; found {
		t.Errorf("buildPageviews() should remove 2011-W51 file, which is out of the time window")
	}
	if _, found := s3.data["pageviews/pageviews-2023-W09.zst"]; !found {
		t.Errorf("buildPageviews() should keep 2023-W09 file, which is still within the time window")
	}
}

func TestStoredPageviews(t *testing.T) {
	s3 := NewFakeS3()
	s3.data["pageviews/pageviews-2011-W51.zst"] = []byte("a")
	s3.data["pageviews/pageviews-2019-W51.gz"] = []byte("junk")
	s3.data["pageviews/pageviews-2024-W06.zst"] = []byte("b")
	got, err := storedPageviews(context.Background(), s3)
	if err != nil {
		t.Error(err)
	}
	want := []string{"2011-W51", "2024-W06"}
	if !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestBuildWeeklyPageviews(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	logger = log.New(&bytes.Buffer{}, "", log.Lshortfile)
	ctx := context.Background()
	dumps := filepath.Join("testdata", "dumps")
	path := filepath.Join(t.TempDir(), "pageviews-2023-W12.zst")
	if err := buildWeeklyPageviews(ctx, dumps, 2023, 12, path); err != nil {
		t.Error(err)
	}

	file, err := os.Open(path)
	if err != nil {
		t.Error(err)
	}
	defer file.Close()

	reader, err := zstd.NewReader(file)
	if err != nil {
		t.Error(err)
	}
	defer reader.Close()

	var buf bytes.Buffer
	if _, err = io.Copy(&buf, reader); err != nil {
		t.Error(err)
	}
	got := buf.String()

	want := `
        commons.wikimedia,2527294,1
		commons.wikimedia,32538038,1
		commons.wikimedia,35159029,1
		de.wikipedia,585473,22
		de.wikivoyage,23685,7
		en.wikipedia,63989872,3
		en.wikipedia,7082401,4
		es.wikipedia,689814,4
		fr.wikipedia,268776,3
		it.wikipedia,110310,1
		rm.wikipedia,10117,1
		rm.wikipedia,3824,3
	`
	re := regexp.MustCompile(`[^\s]+`)
	got = strings.Join(re.FindAllString(got, -1), "|")
	want = strings.Join(re.FindAllString(want, -1), "|")

	if got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestReadWeeklyPageviews(t *testing.T) {
	ch := make(chan string, 10)
	numLines := 0
	group, ctx := errgroup.WithContext(context.Background())
	group.Go(func() error {
		for _ = range ch {
			numLines += 1
		}
		return nil
	})
	group.Go(func() error {
		dumps := filepath.Join("testdata", "dumps")
		return readWeeklyPageviews(ctx, dumps, 2023, 12, ch)
	})
	if err := group.Wait(); err != nil {
		t.Error(err)
	}
	if numLines != 28 {
		t.Errorf("got %d, want 28", numLines)
	}
}

func TestReadWeeklyPageviews_Canceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ch := make(chan string, 2)
	dumps := filepath.Join("testdata", "dumps")
	if err := readWeeklyPageviews(ctx, dumps, 2023, 12, ch); err != context.Canceled {
		t.Errorf("want context.Canceled, got %v", err)
	}
}

func TestReadWeeklyPageviews_MissingFiles(t *testing.T) {
	ctx := context.Background()
	ch := make(chan string, 2)
	if err := readWeeklyPageviews(ctx, "bad-path", 2021, 12, ch); err == nil {
		t.Error("want error, got nil")
	}
}

func TestReadDailyPageviews(t *testing.T) {
	date, _ := time.Parse(time.DateOnly, "2023-03-20")
	path := PageviewsPath(filepath.Join("testdata", "dumps"), date)
	ch := make(chan string, 1)
	go func() {
		defer close(ch)
		ctx := context.Background()
		if err := readDailyPageviews(ctx, path, ch); err != nil {
			t.Error(err)
		}
	}()

	got := make([]string, 0)
	for line := range ch {
		got = append(got, line)
	}

	want := []string{
		"commons.wikimedia,32538038,1",
		"de.wikipedia,585473,4",
		"de.wikivoyage,23685,1",
		"en.wikipedia,7082401,2",
		"en.wikipedia,63989872,1",
		"es.wikipedia,689814,2",
		"fr.wikipedia,268776,1",
		"rm.wikipedia,10117,1",
		"rm.wikipedia,3824,1",
	}

	if !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestReadDailyPageviews_Canceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	date, _ := time.Parse(time.DateOnly, "2023-03-20")
	path := PageviewsPath(filepath.Join("testdata", "dumps"), date)
	ch := make(chan string, 100)
	if err := readDailyPageviews(ctx, path, ch); err != context.Canceled {
		t.Errorf("want context.Canceled, got %v", err)
	}
}

func TestReadDailyPageviews_FileNotFound(t *testing.T) {
	ctx := context.Background()
	ch := make(chan string, 2)
	if err := readDailyPageviews(ctx, "no-such-file.bz2", ch); err == nil {
		t.Error("want error, got nil")
	}
}

func TestMergeCounts(t *testing.T) {
	ch := make(chan string, 2)
	var buf bytes.Buffer

	group, ctx := errgroup.WithContext(context.Background())
	group.Go(func() error {
		return MergeCounts(ctx, ch, &buf)
	})
	group.Go(func() error {
		ch <- "foo,A,77"
		ch <- "qux,X,33"
		ch <- "qux,X,1"
		ch <- "qux,Y,7"
		close(ch)
		return nil
	})
	if err := group.Wait(); err != nil {
		t.Error(err)
	}
	want := "foo,A,77\nqux,X,34\nqux,Y,7\n"
	if got := buf.String(); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestMergeCounts_Canceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ch := make(chan string, 2)
	var buf bytes.Buffer
	if err := MergeCounts(ctx, ch, &buf); err != context.Canceled {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}
