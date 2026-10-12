// SPDX-FileCopyrightText: 2024 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/text/cases"

	"github.com/klauspost/compress/zstd"
	"github.com/lanrat/extsort"
)

type Item uint64

const NoItem = Item(0)
const lexemeMask uint64 = 0x8000000000000000

func ParseItem(s string) Item {
	if len(s) < 2 {
		return NoItem
	}
	n, err := strconv.ParseInt(s[1:len(s)], 10, 64)
	if err != nil || n <= 0 {
		return NoItem
	}
	if s[0] == 'Q' {
		return Item(n)
	}
	if s[0] == 'L' {
		return Item(uint64(n) | lexemeMask)
	}
	return NoItem
}

func (i Item) String() string {
	if uint64(i)&lexemeMask == 0 {
		return fmt.Sprintf("Q%d", i)
	} else {
		return fmt.Sprintf("L%d", uint64(i)&0x7fff_ffff_ffff_ffff)
	}
}

// LatestDump finds the most recent Wikimedia dump file with a matching name.
func LatestDump(dir string, re *regexp.Regexp) (string, error) {
	years := make([]string, 0)
	reYear := regexp.MustCompile(`^\d{4}$`)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", err
	}
	for _, e := range entries {
		name := e.Name()
		if reYear.MatchString(name) {
			years = append(years, name)
		}
	}
	sort.Slice(years, func(i, j int) bool { return years[i] >= years[j] })

	reMonth := regexp.MustCompile(`^\d{4}-\d{2}$`)
	for _, year := range years {
		yearDir := filepath.Join(dir, year)
		months := make([]string, 0, 12)
		monthEntries, err := os.ReadDir(yearDir)
		if err != nil {
			return "", err
		}
		for _, e := range monthEntries {
			name := e.Name()
			if reMonth.MatchString(name) {
				months = append(months, name)
			}
		}
		sort.Slice(months, func(i, j int) bool { return months[i] >= months[j] })
		files := make([]string, 0, 100)
		for _, month := range months {
			monthDir := filepath.Join(yearDir, month)
			fileEntries, err := os.ReadDir(monthDir)
			if err != nil {
				return "", err
			}
			for _, e := range fileEntries {
				if re.MatchString(e.Name()) {
					files = append(files, e.Name())
				}
			}
			sort.Slice(files, func(i, j int) bool { return files[i] >= files[j] })
			if len(files) > 0 {
				return filepath.Join(monthDir, files[0]), nil
			}
		}
	}

	return "todo", fs.ErrNotExist
}

// Caser is stateless and safe to use concurrently by multiple goroutines.
// https://pkg.go.dev/golang.org/x/text/cases#Fold
var caser = cases.Fold()

var isoWeekRegexp = regexp.MustCompile(`(\d{4})-W(\d{2})`)

// ParseISOWeek gives the year and week for an ISO week string like "2018-W34".
func ParseISOWeek(s string) (year int, week int, err error) {
	match := isoWeekRegexp.FindStringSubmatch(s)
	if match == nil || len(match) != 3 {
		return 0, 0, fmt.Errorf("week not in ISO 8601 format: %s", s)
	}

	year, _ = strconv.Atoi(match[1])
	week, _ = strconv.Atoi(match[2])
	return year, week, nil
}

// ISOWeekStart returns the first monday of the given ISO week.
// It is the reverse of Go’s time.ISOWeek() function, which appears
// to be missing from the standard library.
func ISOWeekStart(year, week int) time.Time {
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

func SortLines(ctx context.Context, path string) (string, error) {
	outFile, err := os.CreateTemp("", "*-sorted.zst")
	if err != nil {
		return "", err
	}
	zstdLevel := zstd.WithEncoderLevel(zstd.SpeedBestCompression)
	writer, err := zstd.NewWriter(outFile, zstdLevel)
	if err != nil {
		return "", err
	}
	linesChan := make(chan string, 10000)
	config := extsort.DefaultConfig()
	config.ChunkSize = 8 * 1024 * 1024 / 64 // 8 MiB, 64 Bytes/line avg
	config.NumWorkers = runtime.NumCPU()
	sorter, sortedChan, errChan := extsort.Strings(linesChan, config)
	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error {
		defer close(linesChan)

		r, err := os.Open(path)
		if err != nil {
			return err
		}
		defer r.Close()

		scanner := bufio.NewScanner(r)
		for scanner.Scan() {
			select {
			case <-groupCtx.Done():
				return groupCtx.Err()
			case linesChan <- scanner.Text():
			}
		}
		return scanner.Err()
	})
	group.Go(func() error {
		sorter.Sort(groupCtx)
		for {
			select {
			case <-groupCtx.Done():
				return groupCtx.Err()

			case line, more := <-sortedChan:
				if !more {
					return nil
				}
				var buf bytes.Buffer
				buf.WriteString(line)
				buf.WriteByte('\n')
				if _, err := buf.WriteTo(writer); err != nil {
					return err
				}
			}
		}
	})
	if err := group.Wait(); err != nil {
		return "", err
	}
	if err := <-errChan; err != nil {
		return "", err
	}
	if err := writer.Close(); err != nil {
		return "", err
	}
	if err := outFile.Close(); err != nil {
		return "", err
	}
	return outFile.Name(), nil
}
