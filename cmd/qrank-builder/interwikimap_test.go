// SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"net/http"
	"reflect"
	"testing"
)

func TestInterwikiMap(t *testing.T) {
	client := &http.Client{Transport: &FakeWikiSite{}}
	iwm, err := FetchInterwikiMap(client)
	if err != nil {
		t.Fatal(err)
	}

	got := iwm.Build("rmwikibooks")
	want := map[string]string{
		"advisory":      "advisory.wikimedia.org",
		"c":             "commons.wikimedia.org",
		"chapter":       "rm.wikimedia.org",
		"commons":       "commons.wikimedia.org",
		"d":             "www.wikidata.org",
		"de":            "de.wikibooks.org",
		"gsw":           "als.wikibooks.org",
		"metawiki":      "meta.wikimedia.org",
		"metawikimedia": "meta.wikimedia.org",
		"rm":            "rm.wikibooks.org",
		"s":             "rm.wikisource.org",
		"v":             "rm.wikiversity.org",
		"voy":           "rm.wikivoyage.org",
		"w":             "rm.wikipedia.org",
		"wikt":          "rm.wiktionary.org",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}
