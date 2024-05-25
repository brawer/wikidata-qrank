// SPDX-FileCopyrightText: 2024 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestReadWikiSites(t *testing.T) {
	client := &http.Client{Transport: &FakeWikiSite{}}
	iwmap, err := FetchInterwikiMap(client)
	if err != nil {
		t.Fatal(err)
	}

	sites, err := ReadWikiSites(filepath.Join("testdata", "dumps"), &iwmap)
	if err != nil {
		t.Fatal(err)
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
	_, err := ReadWikiSites(filepath.Join("testdata", "no-such-dir"), nil)
	if !os.IsNotExist(err) {
		t.Errorf("want os.NotExists, got %v", err)
	}
}

// A fake HTTP transport that simulates a Wikimedia site for testing.
type FakeWikiSite struct {
	Broken bool
}

func (f *FakeWikiSite) RoundTrip(req *http.Request) (*http.Response, error) {
	header := make(http.Header)

	if f.Broken {
		header.Add("Content-Type", "text/plain")
		body := io.NopCloser(bytes.NewBufferString("Service Unavailable"))
		return &http.Response{StatusCode: 503, Body: body, Header: header}, nil
	}

	if req.URL.String() == "https://noc.wikimedia.org/conf/interwiki.php.txt" {
		path := filepath.Join("testdata", "interwikimap.php.txt")
		body, _ := os.Open(path)
		return &http.Response{StatusCode: 200, Body: body, Header: header}, nil
	}

	return nil, fmt.Errorf("unexpected request: %s", req.URL.String())
}
