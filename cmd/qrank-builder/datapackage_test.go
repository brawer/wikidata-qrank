// SPDX-FileCopyrightText: 2026 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// datapackageTestInputs is a deterministic set of inputs matching
// testdata/datapackage.golden.json. The digests are the SHA-256 of sentinel
// strings, so they are valid-length hex without pretending to hash real files.
func datapackageTestInputs() datapackageInputs {
	digest := func(s string) string {
		h := sha256.Sum256([]byte(s))
		return hex.EncodeToString(h[:])
	}
	return datapackageInputs{
		Date:    time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC),
		QRank:   datapackageFile{Path: "qrank-20261004.csv.gz", Bytes: 112233445, SHA256: digest("qrank golden CSV fixture")},
		Signals: datapackageFile{Path: "item_signals-20261004.parquet", Bytes: 334455667, SHA256: digest("item signals golden Parquet fixture")},
		Stats:   datapackageFile{Path: "qrank-stats-20261004.json", Bytes: 41234, SHA256: digest("qrank golden stats fixture")},
		BOM:     datapackageFile{Path: "qrank-20261004.cdx.json", Bytes: 5678, SHA256: digest("qrank golden BOM fixture")},
	}
}

func TestWriteDatapackage_Golden(t *testing.T) {
	path := filepath.Join(t.TempDir(), "datapackage.json")
	if err := writeDatapackage(path, datapackageTestInputs()); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	golden := filepath.Join("testdata", "datapackage.golden.json")
	if *updateGolden {
		if err := os.WriteFile(golden, got, 0644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Errorf("generated datapackage.json does not match %s.\n--- got ---\n%s", golden, got)
	}

	var doc struct {
		Schema    string `json:"$schema"`
		Name      string `json:"name"`
		Version   string `json:"version"`
		Resources []struct {
			Name string `json:"name"`
			Path string `json:"path"`
			Hash string `json:"hash"`
		} `json:"resources"`
	}
	if err := json.Unmarshal(got, &doc); err != nil {
		t.Fatalf("datapackage.json is not valid JSON: %v", err)
	}
	if !strings.HasPrefix(doc.Schema, "https://datapackage.org/profiles/2.0/") {
		t.Errorf("$schema = %q, want a Frictionless v2 profile", doc.Schema)
	}
	if doc.Name != "qrank" || doc.Version != "2026-10-04" {
		t.Errorf("name/version = %q/%q, want qrank/2026-10-04", doc.Name, doc.Version)
	}
	if len(doc.Resources) != 4 {
		t.Fatalf("got %d resources, want 4", len(doc.Resources))
	}
	for _, r := range doc.Resources {
		if !strings.HasPrefix(r.Hash, "sha256:") || len(r.Hash) != len("sha256:")+64 {
			t.Errorf("resource %q hash = %q, want sha256:<64 hex>", r.Name, r.Hash)
		}
		// Relative paths only, so they resolve next to datapackage.json.
		if strings.ContainsAny(r.Path, "/:") {
			t.Errorf("resource %q path = %q, want a bare relative filename", r.Name, r.Path)
		}
	}
}

func TestDatapackageID_Stable(t *testing.T) {
	// The dataset identity must be the same for every build, forever. If this
	// changes, downstream catalogs treat it as a different dataset.
	const want = "9c4a6a04-c4b3-544b-8866-35e43a2a6817"
	if datapackageID != want {
		t.Errorf("datapackageID = %q, want %q (dataset identity must never change)", datapackageID, want)
	}
}

func TestStoredDatapackageVersion(t *testing.T) {
	s3 := NewFakeS3()
	s3.data[datapackagePath] = []byte(`{"name": "qrank", "version": "2026-10-04"}`)
	got, err := storedDatapackageVersion(context.Background(), s3)
	if err != nil {
		t.Fatal(err)
	}
	if got != "2026-10-04" {
		t.Errorf("got %q, want 2026-10-04", got)
	}
}
