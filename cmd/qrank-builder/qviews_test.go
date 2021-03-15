// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestBuildQViews(t *testing.T) {
	sitelinks := filepath.Join(t.TempDir(), "TestQViews-sitelinks.br")
	writeBrotli(sitelinks,
		"af.wikipedia/wards_of_the_city_of_london Q7969215\n"+
			"am.wikipedia/ዙሪክ Q72\n"+
			"az.wikipedia/simona_de_bovuar Q7197\n"+
			"az.wikipedia/sürix Q72\n")

	pv1 := filepath.Join(t.TempDir(), "TestQViews-pageviews-1.br")
	pv2 := filepath.Join(t.TempDir(), "TestQViews-pageviews-2.br")
	writeBrotli(pv1,
		"am.wikipedia/ዙሪክ 7\n"+
			"az.wikipedia/simona_de_bovuar 2\n")
	writeBrotli(pv2,
		"am.wikipedia/ዙሪክ 1\n"+
			"az.wikipedia/simona_de_bovuar 58\n"+
			"az.wikipedia/sürix 5\n"+
			"ca.wikipedia/winterthur 11\n")

	path, err := buildQViews(false, time.Now(),
		sitelinks, []string{pv1, pv2},
		t.TempDir(), context.Background())
	if err != nil {
		t.Error(err)
		return
	}

	expected := "Q72 13\nQ7197 60\n"
	got := readBrotliFile(path)
	if expected != got {
		t.Errorf("expected %q, got %q", expected, got)
	}
}
