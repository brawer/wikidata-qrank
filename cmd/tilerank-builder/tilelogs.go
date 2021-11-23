package main

import (
	"fmt"
	"io"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"time"
)

// Returns a list of weeks for which OpenStreetMap has tile logs.
// Weeks are returned in ISO 8601 format such as "2021-W07".
// The reslut is sorted from least to most recent week.
// We return only those weeks where OpenStreetMap has tile logs
// for all seven days.
func GetAvailableWeeks(client *http.Client) ([]string, error) {
	url := "https://planet.openstreetmap.org/tile_logs/"
	r, err := client.Get(url)
	if err != nil {
		return nil, err
	}

	// Only accept HTTP responses with status code 200 OK
	// and when the Content-Type header is HTML.
	contentType := r.Header.Get("Content-Type")
	if strings.ContainsRune(contentType, ';') { // text/html;charset=UTF-8
		contentType = strings.Split(contentType, ";")[0]
	}
	if r.StatusCode != 200 || contentType != "text/html" {
		return nil, fmt.Errorf("failed to fetch %s, StatusCode=%d Content-Type=%s", url, r.StatusCode, contentType)
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}

	// Find out what weeks are available. For each week, we keep a bitmask
	// that tells for which days of that week the OSM Planet server
	// has log files available. For example, if this map contains
	// the entry 202107 → 5 (in binary: 0000101), the server has log files
	// for Tuesday (0000100) and Sunday (0000001) for the 7th week of 2021.
	// That is, Tuesday, February 16, and Sunday, February 21.
	re := regexp.MustCompile(`<a href="tiles-(\d{4}-\d\d-\d\d)\.txt\.xz">`)
	available := make(map[int]int8) // (year*100+isoweek) → 7 bits
	for _, m := range re.FindAllSubmatch(body, -1) {
		if t, err := time.Parse("2006-01-02", string(m[1])); err == nil {
			year, week := t.ISOWeek()
			available[year*100+week] |= 1 << int8(t.Weekday())
		}
	}

	// To our callers, we return weeks in ISO 8601 format, eg. "2021-W07".
	result := make([]string, 0, len(available))
	for week, days := range available {
		if days == 127 { // server has logs for all seven days of this week
			isoWeekString := fmt.Sprintf("%04d-W%02d", week/100, week%100)
			result = append(result, isoWeekString)
		}
	}
	sort.Strings(result)
	return result, nil
}
