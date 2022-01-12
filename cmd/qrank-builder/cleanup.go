// SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

func findLatestStats(path string) (time.Time, error) {
	var t time.Time
	files, err := os.ReadDir(path)
	if err != nil {
		return t, err
	}

	for _, f := range files {
		fn := f.Name()
		if strings.HasPrefix(fn, "stats-") && strings.HasSuffix(fn, ".json") {
			d := fn[6 : len(fn)-5]
			if len(d) == 8 {
				if t2, err := time.Parse("20060102", d); err == nil {
					t = t2
				}
			}
		}
	}

	return t, nil

}

func CleanupCache(path string) error {
	re, err := regexp.Compile(`^(qrank|qviews|sitelinks|stats)-(\d{6,8})\.(br|gz|json)$`)
	if err != nil {
		return err
	}

	files, err := os.ReadDir(path)
	if err != nil {
		return err
	}

	latest, err := findLatestStats(path)
	if err != nil {
		return err
	}

	// If we can't find any stats-*.json files, the pipeline has never
	// run successfully. This is not an error, but we'd rather not
	// clean up anything (delete old files) in this case.
	if latest.IsZero() {
		return nil
	}

	// We delete anything older than 1 month before the latest successful
	// run.
	ageLimit := latest.AddDate(0, -1, 0) // minus one month
	for _, f := range files {
		match := re.FindStringSubmatch(f.Name())
		if match != nil && len(match) >= 2 {
			d, err := time.Parse("20060102", match[2])
			if err != nil {
				continue
			}
			if d.Before(ageLimit) {
				fpath := filepath.Join(path, f.Name())
				if logger != nil {
					logger.Printf("Deleting %s because it is at least 1 month older than the latest successful pipeline run, which was on %04d-%02d-%02d", fpath, latest.Year(), latest.Month(), latest.Day())
				}
				if err := os.Remove(fpath); err != nil {
					return err
				}
			}
		}
	}

	return nil
}
