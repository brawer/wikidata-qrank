// SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

// InterwikiMap contains the mapping from interwiki prefix to target URL,
// such as "foundationsite" to "https://wikimediafoundation.org/$1".
// We only preserve the target site domain, not any of the other fields
// that are (sometimes) present in the `interwikimap` tables. Also,
// we do not keep information for wikis that do not follow the same
// URL path convention as Wikimedia wikis.
type InterwikiMap map[string]string

// Fetches the interwiki map for a Wikimedia site.
//
// As of May 2024, the `interwikimap` table is not part of the SQL dumps
// that are available in the Wikimedia datacenter,  we need to fetch it
// over the network from the live site.
// https://phabricator.wikimedia.org/T365475
func FetchInterwikiMap(client *http.Client, site WikiSite) (InterwikiMap, error) {
	u := "/w/api.php?action=query&meta=siteinfo&siprop=interwikimap&format=json"
	u = fmt.Sprintf("https://%s%s", site.Domain, u)
	resp, err := client.Get(u)
	if err != nil {
		return nil, err
	}

	contentType := resp.Header.Get("Content-Type")
	if strings.ContainsRune(contentType, ';') { // application/json;charset=utf-8
		contentType = strings.Split(contentType, ";")[0]
	}

	if resp.StatusCode != 200 || contentType != "application/json" {
		return nil, fmt.Errorf("failed to fetch %s; StatusCode=%d, Content-Type=%q", u, resp.StatusCode, contentType)
	}

	type entry struct {
		Prefix string `json:"prefix"`
		Url    string `json:"url"`
	}
	type query struct {
		InterwikiMap []entry `json:"interwikimap"`
	}
	var reply struct {
		Query query `json:"query"`
		Foo   string
	}
	if err := json.NewDecoder(resp.Body).Decode(&reply); err != nil {
		return nil, err
	}

	result := make(InterwikiMap, len(reply.Query.InterwikiMap))
	for _, e := range reply.Query.InterwikiMap {
		p, err := url.Parse(e.Url)
		if err != nil {
			return nil, err
		}
		if (p.Scheme == "http" || p.Scheme == "https") && p.Path == "/wiki/$1" {
			result[e.Prefix] = p.Host
		}

	}

	if len(result) == 0 {
		return nil, fmt.Errorf("empty InterwikiMap for %s", site.Domain)
	}

	return result, nil
}
