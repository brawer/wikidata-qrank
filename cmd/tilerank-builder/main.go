// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
)

var logger *log.Logger

func main() {
	logfile, err := createLogFile()
	if err != nil {
		log.Fatal(err)
	}
	defer logfile.Close()
	logger = log.New(logfile, "", log.Ldate|log.Ltime|log.LUTC|log.Lshortfile)

	ctx := context.Background()
	cachedir := "cache"
	maxWeeks := 3 * 52 // 3 years
	tilecounts, err := fetchWeeklyLogs(cachedir, maxWeeks)
	if err != nil {
		logger.Fatal(err)
	}
	path := filepath.Join(cachedir, "out.tif")
	if err := paint(path, 18, tilecounts, ctx); err != nil {
		logger.Fatal(err)
	}
}

// Create a file for keeping logs. If the file already exists, its
// present content is preserved, and new log entries will get appended
// after the existing ones.
func createLogFile() (*os.File, error) {
	logpath := filepath.Join("logs", "tilerank-builder.log")
	if err := os.MkdirAll("logs", os.ModePerm); err != nil {
		return nil, err
	}

	logfile, err := os.OpenFile(logpath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	return logfile, nil
}

// Fetch log data for up to `maxWeeks` weeks from planet.openstreetmap.org.
// For each week, the seven daily log files are fetched from OpenStreetMap,
// and combined into a one single compressed file, stored on local disk.
// If this weekly file already exists on disk, we return its content directly
// without re-fetching that week from the server. Therefore, if this tool
// is run periodically, it will only fetch the content that has not been
// downloaded before. The result is an array of readers, one for each week.
func fetchWeeklyLogs(cachedir string, maxWeeks int) ([]io.Reader, error) {
	client := &http.Client{}
	weeks, err := GetAvailableWeeks(client)
	if err != nil {
		return nil, err
	}

	if len(weeks) > maxWeeks {
		weeks = weeks[len(weeks)-maxWeeks:]
	}

	if logger != nil {
		logger.Printf(
			"found %d weeks with OpenStreetMap tile logs, from %s to %s",
			len(weeks), weeks[0], weeks[len(weeks)-1])
	}

	readers := make([]io.Reader, 0, len(weeks))
	for _, week := range weeks {
		if r, err := GetTileLogs(week, client, cachedir); err == nil {
			readers = append(readers, r)
		} else {
			return nil, err
		}
	}

	return readers, nil
}
