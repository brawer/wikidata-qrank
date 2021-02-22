package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFindEntitiesDump(t *testing.T) {
	dumpsDir := t.TempDir()
	dir := filepath.Join(dumpsDir, "wikidatawiki", "entities")
	if err := os.MkdirAll(filepath.Join(dir, "20250215"), 0755); err != nil {
		t.Error(err)
		return
	}

	dumpPath := filepath.Join(dir, "20250215", "wikidata-20250215-all.json.bz2")
	if f, err := os.Create(dumpPath); err == nil {
		f.Close()
	} else {
		t.Error(err)
		return
	}

	err := os.Symlink(filepath.Join("20250215", "wikidata-20250215-all.json.bz2"),
		filepath.Join(dir, "latest-all.json.bz2"))
	if err != nil {
		t.Error(err)
		return
	}

	expectedPath := filepath.Join(dir, "20250215", "wikidata-20250215-all.json.bz2")
	date, path, err := findEntitiesDump(dumpsDir)
	if err != nil {
		t.Error(err)
		return
	}

	if d := date.Format("2006-01-02"); d != "2025-02-15" {
		t.Errorf("expected 2025-02-15, got %s", d)
	}

	got, _ := os.Stat(path)
	expected, _ := os.Stat(expectedPath)
	if !os.SameFile(expected, got) {
		t.Errorf("expected %q, got %q", expectedPath, path)
	}
}
