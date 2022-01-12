// SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"golang.org/x/sync/errgroup"
)

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
