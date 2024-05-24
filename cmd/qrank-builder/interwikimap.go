// SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
)

// InterwikiMap contains the mapping from interwiki prefix to target URL,
// such as "foundationsite" to "https://wikimediafoundation.org/$1".
type InterwikiMap map[string]string

// Fetches the interwiki map for Wikimedia sites.
//
// As of May 2024, the `interwikimap` table is not part of the SQL dumps
// that are available in the Wikimedia datacenter,  so we need to fetch it
// over the network from the live site. Insead of querying all ~1000 sites,
// we retrieve a PHP snippet that the live Wikimedia sites uses for serving
// production. That cache file is not exactly well documented, but its use
// was recommended to us in https://phabricator.wikimedia.org/T365475.
// See also https://www.mediawiki.org/wiki/Manual:Interwiki_cache.
func FetchInterwikiMap(client *http.Client) (InterwikiMap, error) {
	u := "https://noc.wikimedia.org/conf/interwiki.php.txt"
	resp, err := client.Get(u)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("failed to fetch %s; StatusCode=%d", u, resp.StatusCode)
	}

	// We don’t impose any limit on body size; we trust Wikimedia to not attack us.
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	result := make(InterwikiMap, 15000)
	re := regexp.MustCompile("'(.+?)' => '(.+?)'")

	// As of May 2024, the live file contains 146 duplicate entries. With
	// one exception, they are not repetitions but have conflicting values.
	// But it appears that it’s always the last entry that should win, so we
	// can simply overwrite the current value if a key is already present.
	// https://phabricator.wikimedia.org/T365679
	for _, s := range re.FindAllSubmatch(body, -1) {
		key, value := string(s[1]), string(s[2])
		if strings.HasPrefix(key, "__sites:") {
			result[key] = value
			continue
		}

		// We only care about interwiki links to sites that are operated
		// by the Wikimedia foundation.
		if !strings.HasPrefix(value, "1 ") {
			continue
		}
		if u, err := url.Parse(value[2:len(value)]); err == nil {
			if u.EscapedPath() == "/wiki/$1" {
				result[key] = u.Hostname()
			}
		}
	}

	if len(result) == 0 {
		return nil, fmt.Errorf("empty InterwikiMap")
	}

	return result, nil
}

// Build returns the interwikimap for a wiki site such as `rmwikibooks`.
// The returned map is useful for resolving links across wikis,
// for example when interpreting the SQL dump of the `iwlinks` table.
func (m InterwikiMap) Build(siteid string) map[string]string {
	result := make(map[string]string, 200)

	prefixes := make([]string, 0, 3)
	prefixes = append(prefixes, "__global:")
	if siteType, ok := m["__sites:"+siteid]; ok {
		prefix := fmt.Sprintf("_%s:", siteType)
		prefixes = append(prefixes, prefix)
	}
	prefixes = append(prefixes, siteid+":")
	for key, value := range m {
		for _, prefix := range prefixes {
			if strings.HasPrefix(key, prefix) {
				k := key[len(prefix):len(key)]
				result[k] = value
			}
		}
	}

	return result
}
