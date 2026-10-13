// SPDX-FileCopyrightText: 2026 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

var updateGolden = flag.Bool("update-golden", false, "rewrite testdata/*.golden.* files")

// bomTestInputs is a fully-populated, deterministic set of inputs matching
// testdata/bom.golden.cdx.json. Each digest is the real SHA-256/512 of a
// sentinel string standing in for the QRank file it describes, so the values
// are valid-length hex and stable without pretending to be digests of a real file.
func bomTestInputs() bomInputs {
	digest256 := func(s string) string {
		h := sha256.Sum256([]byte(s))
		return hex.EncodeToString(h[:])
	}
	digest512 := func(s string) string {
		h := sha512.Sum512([]byte(s))
		return hex.EncodeToString(h[:])
	}
	return bomInputs{
		Date:   time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC),
		SHA256: digest256("qrank golden CSV fixture"),
		SHA512: digest512("qrank golden CSV fixture"),
		Provenance: &ItemSignalsProvenance{
			Date:      "2026-10-04",
			Dumps:     DumpsProvenance{Newest: "2026-10-01", Wikis: 1012},
			Pageviews: PageviewsProvenance{FirstDay: "2025-10-06", LastDay: "2026-10-04", Weeks: 52},
			Software:  []string{"QRank/v2.2.0+12a657d4df6a", "QRank/v2.3.0+0b1c2d3e4f5a"},
		},
		Software: "QRank/v2.3.0+0b1c2d3e4f5a",
		Revision: "0b1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b9c",
		Modified: false,
	}
}

func TestWriteBOM_Golden(t *testing.T) {
	path := filepath.Join(t.TempDir(), "qrank-20261004.cdx.json")
	if err := writeBOM(path, bomTestInputs()); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	golden := filepath.Join("testdata", "bom.golden.cdx.json")
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
		t.Errorf("generated BOM does not match %s.\n--- got ---\n%s", golden, got)
	}

	var doc map[string]any
	if err := json.Unmarshal(got, &doc); err != nil {
		t.Fatalf("BOM is not valid JSON: %v", err)
	}
	if doc["bomFormat"] != "CycloneDX" || doc["specVersion"] != "1.7" {
		t.Errorf("bomFormat/specVersion = %v/%v, want CycloneDX/1.7", doc["bomFormat"], doc["specVersion"])
	}
}

func TestWriteBOM_Deterministic(t *testing.T) {
	dir := t.TempDir()
	a := filepath.Join(dir, "a.cdx.json")
	b := filepath.Join(dir, "b.cdx.json")
	if err := writeBOM(a, bomTestInputs()); err != nil {
		t.Fatal(err)
	}
	if err := writeBOM(b, bomTestInputs()); err != nil {
		t.Fatal(err)
	}
	ba, _ := os.ReadFile(a)
	bb, _ := os.ReadFile(b)
	if string(ba) != string(bb) {
		t.Error("writeBOM is not deterministic for identical inputs")
	}
}

func TestParseBuilderVersion(t *testing.T) {
	for _, tt := range []struct {
		software string
		want     builderVersion
		version  string
	}{
		{"QRank/v2.2.0+12a657d4df6a", builderVersion{Release: "2.2.0", Revision: "12a657d4df6a"}, "2.2.0"},
		{"QRank/v2.2.0+12a657d4df6a-modified", builderVersion{Release: "2.2.0", Revision: "12a657d4df6a", Modified: true}, "2.2.0"},
		{"QRank/v2.2.0", builderVersion{Release: "2.2.0"}, "2.2.0"},
		{"QRank/git-12a657d4df6a", builderVersion{Revision: "12a657d4df6a"}, "git-12a657d4df6a"},
		{"QRank", builderVersion{}, ""},
		{"unknown", builderVersion{}, "unknown"},
	} {
		t.Run(tt.software, func(t *testing.T) {
			got := parseBuilderVersion(tt.software)
			tt.want.Software = tt.software
			if got != tt.want {
				t.Errorf("got %+v, want %+v", got, tt.want)
			}
			if v := got.Version(); v != tt.version {
				t.Errorf("Version() = %q, want %q", v, tt.version)
			}
		})
	}
}

func TestBuilderVersionPURL(t *testing.T) {
	const full = "12a657d4df6a1782b230b2e9de9159e285aa9de2"
	for _, tt := range []struct {
		name, software, fullRevision, want string
	}{
		{"running build", "QRank/v2.2.0+12a657d4df6a", full, "pkg:github/brawer/wikidata-qrank@" + full},
		{"older release", "QRank/v2.2.0+12a657d4df6a", "", "pkg:github/brawer/wikidata-qrank@v2.2.0"},
		{"modified release", "QRank/v2.2.0+12a657d4df6a-modified", full, "pkg:github/brawer/wikidata-qrank@v2.2.0"},
		{"git build", "QRank/git-12a657d4df6a", "", "pkg:github/brawer/wikidata-qrank@12a657d4df6a"},
		{"modified git build", "QRank/git-12a657d4df6a-modified", "", ""},
		{"unknown", "unknown", "", ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := parseBuilderVersion(tt.software).PURL(tt.fullRevision); got != tt.want {
				t.Errorf("PURL() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestDataPURL(t *testing.T) {
	in := bomTestInputs()
	want := "pkg:generic/qrank@2026-10-04?checksum=sha256:" + in.SHA256 +
		"&download_url=https%3A%2F%2Fqrank.toolforge.org%2Fdownload%2Fqrank-20261004.csv.gz"
	if got := in.dataPURL(); got != want {
		t.Errorf("dataPURL() = %q, want %q", got, want)
	}
}

// The QRank step may run with a newer release than the one that built
// the item signals. Both should then be listed as tools.
func TestBuildBOM_AddsRunningBuild(t *testing.T) {
	in := bomTestInputs()
	in.Provenance.Software = []string{"QRank/v2.2.0+12a657d4df6a"}
	bom := buildBOM(in)
	var got []string
	for _, tool := range bom.Metadata.Tools.Components {
		got = append(got, tool.BOMRef)
	}
	want := []string{"qrank-builder-2.2.0", "qrank-builder-2.3.0"}
	if !slices.Equal(got, want) {
		t.Errorf("got tools %v, want %v", got, want)
	}
}

// TestBuildBOM_NTIASupplier guards the NTIA "minimum elements": a supplier
// on the document and on every component (see FOSSA NTIA SBOM validator).
func TestBuildBOM_NTIASupplier(t *testing.T) {
	bom := buildBOM(bomTestInputs())
	if bom.Metadata.Supplier == nil || bom.Metadata.Supplier.Name == "" {
		t.Error("metadata.supplier is missing")
	}
	if bom.Metadata.Component.Supplier == nil {
		t.Error("metadata.component.supplier is missing")
	}
	for i, c := range bom.Metadata.Tools.Components {
		if c.Supplier == nil || c.Supplier.Name == "" {
			t.Errorf("metadata.tools.components[%d] (%s) has no supplier", i, c.Name)
		}
	}
	for i, c := range bom.Components {
		if c.Supplier == nil || c.Supplier.Name == "" {
			t.Errorf("components[%d] (%s) has no supplier", i, c.Name)
		}
		if c.Version == "" {
			t.Errorf("components[%d] (%s) has no version", i, c.Name)
		}
	}
}

// The QRank file should depend only on its source data, while the
// builder is modelled as a tool in the formulation, never as a dependency.
func TestBuildBOM_Provenance(t *testing.T) {
	bom := buildBOM(bomTestInputs())
	deps := map[string][]string{}
	for _, d := range bom.Dependencies {
		deps[d.Ref] = d.DependsOn
	}
	if got, want := deps["qrank-20261004"], []string{"wikimedia-dumps", "wikimedia-pageviews"}; !slices.Equal(got, want) {
		t.Errorf("qrank dependsOn %v, want %v", got, want)
	}
	if len(bom.Formulation) != 1 || len(bom.Formulation[0].Workflows) != 1 {
		t.Fatalf("formulation = %+v, want one workflow", bom.Formulation)
	}
	wf := bom.Formulation[0].Workflows[0]
	if len(wf.ResourceReferences) != len(bom.Metadata.Tools.Components) {
		t.Errorf("workflow references %d tools, want %d", len(wf.ResourceReferences), len(bom.Metadata.Tools.Components))
	}
	if len(wf.Outputs) != 1 || wf.Outputs[0].Resource.Ref != "qrank-20261004" {
		t.Errorf("workflow outputs = %+v, want the QRank file", wf.Outputs)
	}
}
