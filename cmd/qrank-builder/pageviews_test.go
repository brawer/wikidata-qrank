// SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
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

func TestPageviewsPath(t *testing.T) {
	want := filepath.Join("foo", "other", "pageview_complete", "2018", "2018-09", "pageviews-20180930-user.bz2")
	date, _ := time.Parse(time.DateOnly, "2018-09-30")
	got := PageviewsPath("foo", date)
	if got != want {
		t.Errorf("want %v, got %v", want, got)
	}
}

func TestReadPageviews(t *testing.T) {
	tests := []struct{ input, expected string }{
		{"", ""},
		{"only three columns", ""},
		{
			"als.wikipedia Ägypte 4623 mobile-web 2 N1P1\n" +
				"als.wikipedia Ägypte 8911 desktop 3 A2X1\n" +
				"ang.wikipedia Lech_Wałęsa 10374 desktop 1 Q1",
			"gsw.wikipedia/ägypte 5|ang.wikipedia/lech_wałęsa 1",
		},
		{
			"en-wg.wikipedia/Talk:Main_Page  67072 desktop 4 B4",
			"",
		},
		{
			"zh-min-nan.wikipedia Ìn-tō͘-chi-ná 670272 desktop 1 J1",
			"nan.wikipedia/ìn-tō͘-chi-ná 1",
		},
	}
	for _, c := range tests {
		checkReadPageviews(t, c.input, c.expected)
	}
}

func checkReadPageviews(t *testing.T, input, expected string) {
	ch := make(chan string, 5)
	g, ctx := errgroup.WithContext(context.Background())
	g.Go(func() error {
		defer close(ch)
		return readPageviews(false, strings.NewReader(input), ch, ctx)
	})
	if err := g.Wait(); err != nil {
		t.Error(err)
		return
	}
	result := make([]string, 0, 5)
	for s := range ch {
		result = append(result, s)
	}
	got := strings.Join(result, "|")
	if expected != got {
		t.Error(fmt.Sprintf("expected %s for %s, got %s", expected, input, got))
		return
	}
}

func TestReadPageviewsCancel(t *testing.T) {
	ch := make(chan string, 1)
	ctx, cancel := context.WithCancel(context.Background())
	g, subCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		input := ("en.wikipedia Bar 18911 desktop 3 A2\n" +
			"en.wikipedia Foo 10374 desktop 1 Q1\n")
		return readPageviews(false, strings.NewReader(input), ch, subCtx)
	})
	cancel()
	if err := g.Wait(); err != context.Canceled {
		t.Error("expected context.Canceled, got", err)
	}
}

func TestCombineCounts(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"bar 22|bar 17|foo 7", "bar 39\nfoo 7\n"},

		// https://github.com/brawer/wikidata-qrank/issues/3
		{"whitespace\u0085char 666", "whitespace\u0085char 666\n"},
		{"multiple columns 666", ""},
	}

	for _, tc := range tests {
		input := strings.Split(tc.input, "|")
		ch := make(chan string, len(input))
		for _, s := range input {
			ch <- s
		}
		close(ch)

		var buf bytes.Buffer
		ctx := context.Background()
		if err := combineCounts(ch, &buf, ctx); err != nil {
			t.Error(err)
			return
		}
		got := buf.String()
		if tc.expected != got {
			t.Errorf("expected %q, got %q", tc.expected, got)
		}
	}
}

func TestBuildWeeklyPageviews(t *testing.T) {
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
