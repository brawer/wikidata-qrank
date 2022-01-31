// SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
)

var logger *log.Logger

func main() {
	ctx := context.Background()

	cachedir := flag.String("cache", "cache/osmviews-builder", "path to cache directory")
	storagekey := flag.String("storage-key", "", "path to key with storage access credentials")
	flag.Parse()

	logfile, err := createLogFile()
	if err != nil {
		log.Fatal(err)
	}
	defer logfile.Close()
	logger = log.New(logfile, "", log.Ldate|log.Ltime|log.LUTC|log.Lshortfile)

	var storage Storage
	if *storagekey != "" {
		storage, err = NewStorage(*storagekey)
		if err != nil {
			logger.Fatal(err)
		}
		bucketExists, err := storage.BucketExists(ctx, "qrank")
		if err != nil {
			logger.Fatal(err)
		}
		if !bucketExists {
			logger.Fatal("storage bucket \"qrank\" does not exist")
		}
	}

	maxWeeks := 52 // 1 year
	tilecounts, lastWeek, err := fetchWeeklyLogs(*cachedir, storage, maxWeeks)
	if err != nil {
		logger.Fatal(err)
	}

	// Construct a file path for the output file. As part of the file name,
	// we use the date of the last day of the last week whose data is being
	// painted. That needs less explanation to users than some file name
	// convention involving ISO weeks, which are less commonly known.
	year, week, err := ParseWeek(lastWeek)
	if err != nil {
		logger.Fatal(err)
	}
	lastDay := weekStart(year, week).AddDate(0, 0, 6)
	date := lastDay.Format("20060102")
	bucket := "qrank"
	localpath := filepath.Join(*cachedir, fmt.Sprintf("osmviews-%s.tiff", date))
	localStatsPath := filepath.Join(*cachedir, fmt.Sprintf("osmviews-stats-%s.json", date))
	localStatsPlotPath := filepath.Join(*cachedir, fmt.Sprintf("osmviews-statsplot-%s.png", date))
	remotepath := fmt.Sprintf("public/osmviews-%s.tiff", date)
	remoteStatsPath := fmt.Sprintf("public/osmviews-stats-%s.json", date)

	// Check if the output file already exists in storage.
	// If we can retrieve object stats without an error, we don’t need
	// to do anything and are completely done.
	if storage != nil {
		_, err := storage.Stat(ctx, bucket, remotepath)
		hasGeoTiff := err != nil
		_, err = storage.Stat(ctx, bucket, remoteStatsPath)
		hasStats := err != nil
		if hasGeoTiff && hasStats {
			msg := fmt.Sprintf("Already in storage: %s/%s and %s/%s", bucket, remotepath, bucket, remoteStatsPath)
			fmt.Println(msg)
			if logger != nil {
				logger.Println(msg)
			}
			return
		}
	}

	// Paint the output GeoTIFF file.
	if err := paint(localpath, 18, tilecounts, ctx); err != nil {
		logger.Fatal(err)
	}

	if err := BuildStats(localpath, localStatsPath, localStatsPlotPath); err != nil {
		logger.Fatal(err)
	}

	// Upload the output file to storage, and garbage-collect old files.
	if storage != nil {
		err := storage.PutFile(ctx, bucket, remotepath, localpath, "image/tiff")
		if err != nil {
			logger.Fatal(err)
		}

		err = storage.PutFile(ctx, bucket, remoteStatsPath, localStatsPath, "application/json")
		if err != nil {
			logger.Fatal(err)
		}

		msg := fmt.Sprintf("Uploaded to storage: %s/%s and %s/%s\n", bucket, remotepath, bucket, remoteStatsPath)
		fmt.Println(msg)
		logger.Println(msg)

		if err := Cleanup(storage); err != nil {
			logger.Fatal(err)
		}
	}
}

// Create a file for keeping logs. If the file already exists, its
// present content is preserved, and new log entries will get appended
// after the existing ones.
func createLogFile() (*os.File, error) {
	logpath := filepath.Join("logs", "osmviews-builder.log")
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
// downloaded before. The result is an array of readers (one for each week),
// and the ISO week string (like "2021-W28") for the last available week.
func fetchWeeklyLogs(cachedir string, storage Storage, maxWeeks int) ([]io.Reader, string, error) {
	client := &http.Client{}
	weeks, err := GetAvailableWeeks(client)
	if err != nil {
		return nil, "", err
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
		if r, err := GetTileLogs(week, client, cachedir, storage); err == nil {
			readers = append(readers, r)
		} else {
			return nil, "", err
		}
	}

	return readers, weeks[len(weeks)-1], nil
}
