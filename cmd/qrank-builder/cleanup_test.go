package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFindLatestStats(t *testing.T) {
	dir, _ := os.MkdirTemp(t.TempDir(), "TestFindLatestStats-*")
	defer os.RemoveAll(dir)
	os.Create(filepath.Join(dir, "stats-20210228.json"))
	os.Create(filepath.Join(dir, "stats-19921213.json"))
	latest, err := findLatestStats(dir)
	if err != nil {
		t.Error(err)
		return
	}
	got := latest.Format("2006-01-02")
	if got != "2021-02-28" {
		t.Errorf("expected 2021-02-28, got %s", got)
		return
	}
}

func TestCleanupCache(t *testing.T) {
	dir, _ := os.MkdirTemp(t.TempDir(), "TestCleanupCache-*")
	defer os.RemoveAll(dir)
	old := []string{
		"qrank-19921213.gz", "qviews-20201215.br",
		"sitelinks-19720410.br", "sitelinks-19720526.br",
		"stats-19720410.json", "stats-19720526.json",
	}
	recent := []string{
		"qrank-20210328.br", "qviews-20210328.br",
		"sitelinks-20210328.br", "stats-20210328.json",
		"qrank-20210415.br", "qviews-20210415.br",
		"sitelinks-20210415.br", "stats-20210415.json",
	}
	for _, f := range old {
		os.Create(filepath.Join(dir, f))
	}
	for _, f := range recent {
		os.Create(filepath.Join(dir, f))
	}
	if err := CleanupCache(dir); err != nil {
		t.Error(err)
		return
	}
	for _, f := range old {
		_, err := os.Stat(filepath.Join(dir, f))
		if !os.IsNotExist(err) {
			t.Errorf("expected %s to get deleted", f)
		}
	}
	for _, f := range recent {
		_, err := os.Stat(filepath.Join(dir, f))
		if err != nil {
			t.Errorf("expected %s to not get deleted", f)
		}
	}
}
