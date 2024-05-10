// SPDX-FileCopyrightText: 2024 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"compress/gzip"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

// WikiSite keeps what we know about a Wikimedia site such as en.wikipedia.org.
type WikiSite struct {
	Key        string    // Wikimedia key, such as "enwiki"
	Domain     string    // Internet domain, such as "en.wikipedia.org"
	LastDumped time.Time // Date of last complete database dump
}

func ReadWikiSites(dumps string) (*map[string]WikiSite, error) {
	dirContent, err := os.ReadDir(dumps)
	if err != nil {
		return nil, err
	}
	dumpDirs := make(map[string]os.DirEntry, len(dirContent))
	for _, d := range dirContent {
		dumpDirs[d.Name()] = d
	}

	sites := make(map[string]WikiSite, 400)
	f, err := os.Open(filepath.Join(
		dumps, "metawiki", "latest/metawiki-latest-sites.sql.gz",
	))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	gz, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	defer gz.Close()

	reader, err := NewSQLReader(gz)
	if err != nil {
		return nil, err
	}

	columns := reader.Columns()
	globalKeyCol := slices.Index(columns, "site_global_key")
	domainCol := slices.Index(columns, "site_domain")
	for {
		row, err := reader.Read()
		if row == nil {
			break
		}
		if err != nil {
			return nil, err
		}

		site := WikiSite{
			Key:    row[globalKeyCol],
			Domain: decodeDomain(row[domainCol]),
		}
		if dirent, ok := dumpDirs[site.Key]; !ok || !dirent.IsDir() {
			continue
		}

		for _, f := range []string{"page.sql.gz", "page_props.sql.gz"} {
			latestFile := fmt.Sprintf("%s-latest-%s", site.Key, f)
			latestPath := filepath.Join(dumps, site.Key, "latest", latestFile)
			if latest, err := filepath.EvalSymlinks(latestPath); err == nil {
				dir, _ := filepath.Split(latest)
				_, version := filepath.Split(filepath.Dir(dir))
				if dumped, err := time.Parse("20060102", version); err == nil {
					if site.LastDumped.IsZero() || dumped.Before(site.LastDumped) {
						site.LastDumped = dumped

					}
				}
			}
		}

		if !site.LastDumped.IsZero() {
			sites[site.Key] = site
		}
	}

	return &sites, nil
}

func decodeDomain(s string) string {
	s = strings.TrimSuffix(s, ".")
	runes := []rune(s)
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}
	return string(runes)
}
