// SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"net/http"
	"reflect"
	"testing"
)

func TestFetchInterwikiMap(t *testing.T) {
	client := &http.Client{Transport: &FakeWikiSite{}}
	site := WikiSite{Key: "rmwiki", Domain: "rm.wikipedia.org"}
	got, err := FetchInterwikiMap(client, site)
	if err != nil {
		t.Fatal(err)
	}

	want := InterwikiMap{
		"advisory": "advisory.wikimedia.org",
		"b":        "rm.wikibooks.org",
		"chapter":  "rm.wikimedia.org",
		"d":        "www.wikidata.org",
		"de":       "de.wikipedia.org",
		"m":        "meta.wikimedia.org",
		"meta":     "meta.wikimedia.org",
		"n":        "rm.wikinews.org",
		"q":        "rm.wikiquote.org",
		"rm":       "rm.wikipedia.org",
		"s":        "rm.wikisource.org",
		"v":        "rm.wikiversity.org",
		"voy":      "rm.wikivoyage.org",
		"w":        "en.wikipedia.org",
		"wikt":     "rm.wiktionary.org",
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %q, want %q", got, want)
	}
}
