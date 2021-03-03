package main

import (
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
			"als.wikipedia Ägypten 4623 mobile-web 2 N1P1\n" +
				"als.wikipedia Ägypten 8911 desktop 3 A2X1\n" +
				"ang.wikipedia Lech_Wałęsa 10374 desktop 1 Q1",
			"als.wikipedia/ägypten 5|ang.wikipedia/lech_wałęsa 1",
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
