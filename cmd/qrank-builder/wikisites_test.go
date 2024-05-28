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
	"reflect"
	"testing"
	"time"
)

func TestReadWikiSites(t *testing.T) {
	client := &http.Client{Transport: &FakeWikiSite{}}
	sites, err := ReadWikiSites(client, filepath.Join("testdata", "dumps"))
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct{ key, domain, lastDumped string }{
		{"loginwiki", "login.wikimedia.org", "2024-05-01"},
		{"rmwiki", "rm.wikipedia.org", "2024-03-01"},
		{"wikidatawiki", "www.wikidata.org", "2024-04-01"},
	}
	for _, tc := range tests {
		site := sites.Sites[tc.key]
		if site.Domain != tc.domain {
			t.Errorf(`got "%s", want "%s", for sites["%s"].Domain`, site.Domain, tc.domain, tc.key)
		}
		if sites.Sites[tc.key] != sites.Domains[tc.domain] {
			t.Errorf("sites.Sites[%q] should be same as sites.Domains[%q]", tc.key, tc.domain)
		}
		lastDumped := site.LastDumped.Format(time.DateOnly)
		if lastDumped != tc.lastDumped {
			t.Errorf(`got %s, want %s, for sites["%s"].LastDumped`, lastDumped, tc.lastDumped, tc.key)
		}
	}

	for _, tc := range []struct {
		wiki   string
		prefix string
		want   string
	}{
		{"rmwiki", "d", "wikidatawiki"},      // __global:d => wikidatawiki
		{"rmwiki", "b", "rmwikibooks"},       // rmwiki:b => rmwikibooks
		{"rmwiki", "unknown", ""},            // no such prefix
		{"rmwiki", "rm", "rmwiki"},           // _wiki:rm => rmwiki
		{"rmwikibooks", "rm", "rmwikibooks"}, // _wikibooks:rm => rmwikibooks
	} {
		got := ""
		if target := sites.Sites[tc.wiki].ResolveInterwikiPrefix(tc.prefix); target != nil {
			got = target.Key
		}
		if got != tc.want {
			t.Errorf("got %q, want %q", got, tc.want)
		}
	}

	got := make(map[string]string, 18)
	for key, value := range sites.Sites["rmwiki"].Namespaces {
		got[key] = fmt.Sprintf("%d,%q,%q", value.ID, value.Canonical, value.Localized)
	}
	want := map[string]string{
		"":           `0,"",""`,
		"-1":         `-1,"Special","Spezial"`,
		"-2":         `-2,"Media","Multimedia"`,
		"0":          `0,"",""`,
		"1":          `1,"Talk","Discussiun"`,
		"2":          `2,"User","Utilisader"`,
		"4":          `4,"Project","Wikipedia"`,
		"Discussiun": `1,"Talk","Discussiun"`,
		"Media":      `-2,"Media","Multimedia"`,
		"Multimedia": `-2,"Media","Multimedia"`,
		"Project":    `4,"Project","Wikipedia"`,
		"Special":    `-1,"Special","Spezial"`,
		"Spezial":    `-1,"Special","Spezial"`,
		"Talk":       `1,"Talk","Discussiun"`,
		"User":       `2,"User","Utilisader"`,
		"Utilisader": `2,"User","Utilisader"`,
		"Wikipedia":  `4,"Project","Wikipedia"`,
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestReadWikiSites_BadPath(t *testing.T) {
	_, err := ReadWikiSites(nil, filepath.Join("testdata", "no-such-dir"))
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
