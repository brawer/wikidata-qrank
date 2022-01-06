// SPDX-License-Identifier: MIT

package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/andybalholm/brotli"
	"github.com/lanrat/extsort"
	// "github.com/minio/minio-go/v7"
	"github.com/ulikunitz/xz"
	"golang.org/x/sync/errgroup"
)

// Return a list of weeks for which OpenStreetMap has tile logs.
// Weeks are returned in ISO 8601 format such as "2021-W07".
// The result is sorted from least to most recent week.
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

var tileLogRegexp = regexp.MustCompile(`^(\d+)/(\d+)/(\d+)\s+(\d+)$`)

// GetTileLogs returns an io.Reader for the sorted log records of a week.
// If cachedir contains already contains cached records for the requested week,
// the data will be read from local disk. Otherwise, the seven daily log files
// for the requested week are fetched from the OpenStreetMap planet server,
// uncompressed, sorted by TileKey, and stored as a compressed file into
// cachedir.
func GetTileLogs(week string, client *http.Client, cachedir string, storage StorageClient) (io.Reader, error) {
	ctx := context.Background()

	// path := fmt.Sprintf("internal/osmviews-builder/tilelogs-%s.br", week)

	path := filepath.Join(cachedir, fmt.Sprintf("tilelogs-%s.br", week))
	if f, err := os.Open(path); err == nil {
		return brotli.NewReader(f), nil
	}

	if logger != nil {
		logger.Printf("building %s", path)
	}

	if err := os.MkdirAll(cachedir, os.ModePerm); err != nil {
		return nil, err
	}

	ch := make(chan extsort.SortType, 100000)
	g, subCtx := errgroup.WithContext(ctx)
	config := extsort.DefaultConfig()
	config.NumWorkers = runtime.NumCPU()
	sorter, outChan, errChan := extsort.New(ch, TileCountFromBytes, TileCountLess, config)
	g.Go(func() error {
		return fetchWeeklyTileLogs(week, client, ch, subCtx)
	})
	g.Go(func() error {
		sorter.Sort(ctx) // not subCtx, as per extsort docs
		return nil
	})
	if err := g.Wait(); err != nil {
		return nil, err
	}

	// We write to a temporary file first, and rename it atomically
	// once it is finished in usable state. This prevents hiccups
	// in case the process crashes (or the machine dies) while the
	// output file is being written.
	tmppath := path + ".tmp"
	tmpfile, err := os.Create(tmppath)
	if err != nil {
		return nil, err
	}
	defer tmpfile.Close()
	writer := brotli.NewWriterLevel(tmpfile, 9)
	defer writer.Close()

	var last TileCount
	for data := range outChan {
		cur := data.(TileCount)
		if cur.Key != last.Key {
			if last.Count > 0 {
				zoom, x, y := last.Key.ZoomXY()
				fmt.Fprintf(writer, "%d/%d/%d %d\n", zoom, x, y, last.Count)
			}
			last = cur
		} else {
			last.Count += cur.Count
		}
	}
	if last.Count > 0 {
		zoom, x, y := last.Key.ZoomXY()
		fmt.Fprintf(writer, "%d/%d/%d %d\n", zoom, x, y, last.Count)
	}

	// Check for errors from the external sorting library.
	if err := <-errChan; err != nil {
		return nil, err
	}

	// Close writer/compressor, ask kernel to ensure temp file is on disk, and close it.
	if err := writer.Close(); err != nil {
		return nil, err
	}
	if err := tmpfile.Sync(); err != nil {
		return nil, err
	}
	if err := tmpfile.Close(); err != nil {
		return nil, err
	}

	// Now that we have the result on disk, rename it to final path.
	if err := os.Rename(tmppath, path); err != nil {
		return nil, err
	}

	// Open the file for reading and return a reader for it.
	if f, err := os.Open(path); err == nil {
		return brotli.NewReader(f), nil
	} else {
		return nil, err
	}
}

func fetchWeeklyTileLogs(week string, client *http.Client, ch chan<- extsort.SortType, ctx context.Context) error {
	defer close(ch)

	// Fetch the tile logs for the seven days in this week, in parallel.
	parsedYear, parsedWeek, err := ParseWeek(week)
	if err != nil {
		return err
	}

	firstDay := weekStart(parsedYear, parsedWeek)
	g, subCtx := errgroup.WithContext(ctx)
	for i := 0; i < 7; i++ {
		day := firstDay.AddDate(0, 0, i)
		g.Go(func() error {
			return fetchTileLogs(day, client, ch, subCtx)
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	return nil
}

func fetchTileLogs(day time.Time, client *http.Client, ch chan<- extsort.SortType, ctx context.Context) error {
	url := fmt.Sprintf(
		"https://planet.openstreetmap.org/tile_logs/tiles-%04d-%02d-%02d.txt.xz",
		day.Year(), day.Month(), day.Day())
	r, err := client.Get(url)
	if err != nil {
		return err
	}

	reader, err := xz.NewReader(r.Body)
	if err != nil {
		return err
	}

	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		// Check if our task has been canceled. Typically this can happen
		// because of an error in another goroutine in the same x.sync.errroup.
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if tc := ParseTileCount(scanner.Text()); tc.Count > 0 {
			ch <- tc
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}

	return nil
}

// Reverse of Go’s time.ISOWeek() function.
func weekStart(year, week int) time.Time {
	// Find the first Monday before July 1 of the given year.
	t := time.Date(year, 7, 1, 0, 0, 0, 0, time.UTC)
	if wd := t.Weekday(); wd == time.Sunday {
		t = t.AddDate(0, 0, -6)
	} else {
		t = t.AddDate(0, 0, -int(wd)+1)
	}

	_, w := t.ISOWeek()
	return t.AddDate(0, 0, (week-w)*7)
}

var isoWeekRegexp = regexp.MustCompile(`(\d{4})-W(\d{2})`)

// ParseWeek gives the year and week for an ISO week string like "2018-W34".
func ParseWeek(s string) (year int, week int, err error) {
	match := isoWeekRegexp.FindStringSubmatch(s)
	if match == nil || len(match) != 3 {
		return 0, 0, fmt.Errorf("week not in ISO 8601 format: %s", s)
	}

	year, _ = strconv.Atoi(match[1])
	week, _ = strconv.Atoi(match[2])
	return year, week, nil
}
