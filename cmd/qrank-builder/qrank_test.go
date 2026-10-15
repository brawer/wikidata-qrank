// SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
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
		itemSignalsHeader,
		"1,7,0,0,0,0",
		"2,0,100,3,1,2",
		"3,42,0,0,0,0",
		"4,7,0,0,0,0",
	}
	if err := s3.WriteLines(signals, "public/item_signals-20261011.parquet"); err != nil {
		t.Fatal(err)
	}
	date, _ := time.Parse(time.DateOnly, "2026-10-11")
	prov := &ItemSignalsProvenance{
		Date:      "2026-10-11",
		Dumps:     DumpsProvenance{Newest: "2026-10-01", Wikis: 7},
		Pageviews: PageviewsProvenance{FirstDay: "2025-10-13", LastDay: "2026-10-11", Weeks: 52},
		Software:  []string{"QRank/v2.2.0+12a657d4df6a"},
	}
	if err := storeItemSignalsProvenance(ctx, prov, s3); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{
		"public/qrank-20240311.csv.gz",
		"public/qrank-20240318.csv.gz",
		"public/qrank-20240325.csv.gz",
		"public/qrank-stats-20240311.json",
		"public/qrank-20240311.cdx.json",
	} {
		s3.data[path] = []byte("old")
	}

	s3.puts = nil
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

	// The BOM should go first and the data package last, so that
	// a client can fetch every file that the data package names.
	wantPuts := []string{
		"public/qrank-20261011.cdx.json",
		"public/qrank-stats-20261011.json",
		"public/qrank-20261011.csv.gz",
		"public/datapackage.json",
	}
	if !slices.Equal(s3.puts, wantPuts) {
		t.Errorf("got puts %v, want %v", s3.puts, wantPuts)
	}

	var bom cdxBOM
	if err := json.Unmarshal(s3.data["public/qrank-20261011.cdx.json"], &bom); err != nil {
		t.Fatal(err)
	}
	qrankSHA256 := fmt.Sprintf("%x", sha256.Sum256(s3.data["public/qrank-20261011.csv.gz"]))
	if got := bom.Metadata.Component.Hashes[0].Content; got != qrankSHA256 {
		t.Errorf("BOM has SHA-256 %s, want %s", got, qrankSHA256)
	}
	signalsSHA256 := fmt.Sprintf("%x", sha256.Sum256(s3.data["public/item_signals-20261011.parquet"]))
	if got := bom.Components[0].Hashes[0].Content; got != signalsSHA256 {
		t.Errorf("BOM has item signals SHA-256 %s, want %s", got, signalsSHA256)
	}
	if got := bom.Components[1].Version; got != "2026-10-01" {
		t.Errorf("BOM has dumps version %q, want 2026-10-01", got)
	}

	var dp fdPackage
	if err := json.Unmarshal(s3.data[datapackagePath], &dp); err != nil {
		t.Fatal(err)
	}
	if dp.Version != "2026-10-11" {
		t.Errorf("data package has version %q, want 2026-10-11", dp.Version)
	}
	for _, r := range dp.Resources {
		content := s3.data["public/"+r.Path]
		if want := fmt.Sprintf("sha256:%x", sha256.Sum256(content)); r.Hash != want {
			t.Errorf("data package lists %s with hash %s, want %s", r.Path, r.Hash, want)
		}
		if r.Bytes != int64(len(content)) {
			t.Errorf("data package lists %s with %d bytes, want %d", r.Path, r.Bytes, len(content))
		}
	}

	// Only the newest 3 rankings should remain in storage. BOMs are kept forever.
	var stored []string
	for path := range s3.data {
		if strings.HasPrefix(path, "public/qrank") {
			stored = append(stored, path)
		}
	}
	slices.Sort(stored)
	wantStored := []string{
		"public/qrank-20240311.cdx.json",
		"public/qrank-20240318.csv.gz",
		"public/qrank-20240325.csv.gz",
		"public/qrank-20261011.cdx.json",
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

	// Data packages of earlier releases did not list the item signals.
	// They should get re-built, so that they list them.
	s3.data[datapackagePath] = []byte(`{"version": "2026-10-11", "resources": [{"path": "qrank-20261011.csv.gz"}]}`)
	if err := buildQRankFiles(ctx, date, s3); err != nil {
		t.Fatal(err)
	}
	if string(s3.data["public/qrank-20261011.csv.gz"]) == "unchanged" {
		t.Errorf("buildQRankFiles() should rebuild when the data package lacks item signals")
	}
	s3.data["public/qrank-20261011.csv.gz"] = []byte("unchanged")

	// If the data package names another version, everything should get
	// re-built, as after a crash before the data package was stored.
	s3.data[datapackagePath] = []byte(`{"version": "2026-10-04"}`)
	if err := buildQRankFiles(ctx, date, s3); err != nil {
		t.Fatal(err)
	}
	if string(s3.data["public/qrank-20261011.csv.gz"]) == "unchanged" {
		t.Errorf("buildQRankFiles() should rebuild when the data package is stale")
	}
}

// Rankings built by releases before the BOM existed have no BOM and
// no data package. They should get re-built to add both.
func TestBuildQRankFiles_MissingBOM(t *testing.T) {
	logger = log.New(&bytes.Buffer{}, "", log.Lshortfile)
	ctx := context.Background()
	s3 := NewFakeS3()
	signals := []string{itemSignalsHeader, "1,7,0,0,0,0"}
	if err := s3.WriteLines(signals, "public/item_signals-20261011.parquet"); err != nil {
		t.Fatal(err)
	}
	date, _ := time.Parse(time.DateOnly, "2026-10-11")
	prov := &ItemSignalsProvenance{Date: "2026-10-11", Software: []string{"QRank/v2.2.0"}}
	if err := storeItemSignalsProvenance(ctx, prov, s3); err != nil {
		t.Fatal(err)
	}
	s3.data["public/qrank-20261011.csv.gz"] = []byte("old")
	s3.data["public/qrank-stats-20261011.json"] = []byte("old")

	if err := buildQRankFiles(ctx, date, s3); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"public/qrank-20261011.cdx.json", datapackagePath} {
		if _, found := s3.data[path]; !found {
			t.Errorf("buildQRankFiles() should store %s", path)
		}
	}
	if string(s3.data["public/qrank-20261011.csv.gz"]) == "old" {
		t.Errorf("buildQRankFiles() should rebuild the ranking")
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
