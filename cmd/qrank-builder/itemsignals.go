// SPDX-FileCopyrightText: 2024 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"regexp"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/minio/minio-go/v7"
)

// BuildItemSignals builds per-item signals and puts them in storage.
// If the signals file is already in storage, it does not get re-built.
func buildItemSignals(ctx context.Context, pageviews []string, sites *map[string]WikiSite, s3 S3) (time.Time, error) {
	stored, err := StoredItemSignalsVersion(ctx, s3)
	if err != nil {
		return time.Time{}, err
	}

	newest := ItemSignalsVersion(pageviews, sites)
	if !newest.After(stored) {
		s := stored.Format(time.DateOnly)
		n := newest.Format(time.DateOnly)
		logger.Printf("signals in storage are still fresh: stored=%s, newest=%s", s, n)
		return stored, nil
	}

	newestYMD := newest.Format("20060102")
	destPath := fmt.Sprintf("public/signals-%s.csv.zst", newestYMD)
	logger.Printf("building %s", destPath)
	outFile, err := os.CreateTemp("", "*-item_signals.csv.zst")
	if err != nil {
		return time.Time{}, err
	}
	zstdLevel := zstd.WithEncoderLevel(zstd.SpeedBestCompression)
	writer, err := zstd.NewWriter(outFile, zstdLevel)
	if err != nil {
		return time.Time{}, err
	}

	// Write column titles.
	columns := []string{
		"item",
		"pageviews",
		"wikitext_bytes",
		"claims",
		"identifiers",
		"sitelinks",
	}
	var buf bytes.Buffer
	for i, col := range columns {
		if i != 0 {
			if _, err := buf.WriteString(","); err != nil {
				return time.Time{}, err
			}
		}
		if _, err := buf.WriteString(col); err != nil {
			return time.Time{}, err
		}
	}
	if _, err := buf.WriteString("\n"); err != nil {
		return time.Time{}, err
	}
	if _, err := writer.Write(buf.Bytes()); err != nil {
		return time.Time{}, err
	}

	// TODO: Actually build and write the signals. Not yet implemented.

	if err := writer.Close(); err != nil {
		return time.Time{}, err
	}
	if err := outFile.Close(); err != nil {
		return time.Time{}, err
	}

	if err := PutInStorage(ctx, outFile.Name(), s3, "qrank", destPath, "application/zstd"); err != nil {
		return time.Time{}, err
	}

	if err := os.Remove(outFile.Name()); err != nil {
		return time.Time{}, err
	}

	return newest, nil
}

func ItemSignalsVersion(pageviews []string, sites *map[string]WikiSite) time.Time {
	var date time.Time
	re := regexp.MustCompile(`^pageviews/pageviews-(\d{4}-W\d{2}).zst$`)
	for _, pv := range pageviews {
		if match := re.FindStringSubmatch(pv); match != nil {
			if year, week, err := ParseISOWeek(match[1]); err == nil {
				weekStart := ISOWeekStart(year, week)
				weekEnd := weekStart.AddDate(0, 0, 6) // weekStart + 6 days
				if weekEnd.After(date) {
					date = weekEnd
				}
			}
		}
	}

	for _, site := range *sites {
		if site.LastDumped.After(date) {
			date = site.LastDumped
		}
	}

	return date
}

// StoredItemSignalsVersion returns the version of the signals file in storage.
// If there is no such file, the result is the zero time.Time without error.
func StoredItemSignalsVersion(ctx context.Context, s3 S3) (time.Time, error) {
	re := regexp.MustCompile(`^public/signals-(\d{8})-page_signals.zst$`)
	var result time.Time
	opts := minio.ListObjectsOptions{Prefix: "public/"}
	for obj := range s3.ListObjects(ctx, "qrank", opts) {
		if obj.Err != nil {
			return time.Time{}, obj.Err
		}
		if match := re.FindStringSubmatch(obj.Key); match != nil {
			if t, err := time.Parse(match[1], "20060201"); err == nil {
				if t.After(result) {
					result = t
				}
			}
		}
	}

	return result, nil
}
