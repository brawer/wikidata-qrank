// SPDX-FileCopyrightText: 2024 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestReadWikiSites(t *testing.T) {
	sites, err := ReadWikiSites(filepath.Join("testdata", "dumps"))
	if err != nil {
		t.Error(err)
	}

	tests := []struct{ key, domain, lastDumped string }{
		{"loginwiki", "login.wikimedia.org", "2024-05-01"},
		{"rmwiki", "rm.wikipedia.org", "2024-03-01"},
		{"wikidatawiki", "www.wikidata.org", "2024-04-01"},
	}
	for _, tc := range tests {
		site := (*sites)[tc.key]
		if site.Domain != tc.domain {
			t.Errorf(`got "%s", want "%s", for sites["%s"].Domain`, site.Domain, tc.domain, tc.key)
		}
		lastDumped := site.LastDumped.Format(time.DateOnly)
		if lastDumped != tc.lastDumped {
			t.Errorf(`got %s, want %s, for sites["%s"].LastDumped`, lastDumped, tc.lastDumped, tc.key)
		}
	}
}

func TestReadWikiSites_BadPath(t *testing.T) {
	_, err := ReadWikiSites(filepath.Join("testdata", "no-such-dir"))
	if !os.IsNotExist(err) {
		t.Errorf("want os.NotExists, got %v", err)
	}
}
