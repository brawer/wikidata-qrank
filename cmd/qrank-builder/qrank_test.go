// SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"log"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestBuildQRankFiles(t *testing.T) {
	logger = log.New(&bytes.Buffer{}, "", log.Lshortfile)
	ctx := context.Background()
	s3 := NewFakeS3()
	signals := []string{
		"item,pageviews_52w,wikitext_bytes,claims,identifiers,sitelinks",
		"Q1,7,0,0,0,0",
		"Q2,0,100,3,1,2",
		"Q3,42,0,0,0,0",
		"Q4,7,0,0,0,0",
	}
	if err := s3.WriteLines(signals, "public/item_signals-20261011.csv.zst"); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{
		"public/qrank-20240311.csv.gz",
		"public/qrank-20240318.csv.gz",
		"public/qrank-20240325.csv.gz",
		"public/qrank-stats-20240311.json",
	} {
		s3.data[path] = []byte("old")
	}

	date, _ := time.Parse(time.DateOnly, "2026-10-11")
	if err := buildQRankFiles(ctx, date, s3); err != nil {
		t.Fatal(err)
	}

	got, err := gunzipString(s3.data["public/qrank-20261011.csv.gz"])
	if err != nil {
		t.Fatal(err)
	}
	want := "Entity,QRank\nQ3,42\nQ1,7\nQ4,7\n"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}

	stats := string(s3.data["public/qrank-stats-20261011.json"])
	if !strings.Contains(stats, `["Q3",1,42]`) {
		t.Errorf("stats should contain top item, got %s", stats)
	}

	// Only the newest 3 rankings should remain in storage.
	var stored []string
	for path := range s3.data {
		if strings.HasPrefix(path, "public/qrank") {
			stored = append(stored, path)
		}
	}
	slices.Sort(stored)
	wantStored := []string{
		"public/qrank-20240318.csv.gz",
		"public/qrank-20240325.csv.gz",
		"public/qrank-20261011.csv.gz",
		"public/qrank-stats-20240311.json",
		"public/qrank-stats-20261011.json",
	}
	if !slices.Equal(stored, wantStored) {
		t.Errorf("got %v, want %v", stored, wantStored)
	}

	// A second call should not rebuild what is already stored.
	s3.data["public/qrank-20261011.csv.gz"] = []byte("unchanged")
	if err := buildQRankFiles(ctx, date, s3); err != nil {
		t.Fatal(err)
	}
	if string(s3.data["public/qrank-20261011.csv.gz"]) != "unchanged" {
		t.Errorf("buildQRankFiles() should not rebuild a stored ranking")
	}
}

func gunzipString(data []byte) (string, error) {
	reader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return "", err
	}
	defer reader.Close()
	b, err := io.ReadAll(reader)
	return string(b), err
}
