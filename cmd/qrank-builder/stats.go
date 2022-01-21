// SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type Sample []interface{} // [ID, Rank, Value]

type Stats struct {
	Median  int
	Samples []Sample
}

func buildStats(date time.Time, qrankPath string, topN int, numSamples int, outDir string) (string, error) {
	// To compute our stats, we do two passes over the QRank file.
	// First, a pass to count the number of lines in the file;
	// second, a pass that actually computes the stats.
	qrankFile, err := os.Open(qrankPath)
	if err != nil {
		return "", err
	}
	defer qrankFile.Close()

	qrankReader, err := gzip.NewReader(qrankFile)
	if err != nil {
		return "", err
	}

	numRanks, err := countLines(qrankReader)
	if err != nil {
		return "", err
	}
	numRanks -= 1 // Don’t count CSV header.
	medianRank := numRanks/2 + 1

	if _, err := qrankFile.Seek(0, os.SEEK_SET); err != nil {
		return "", err
	}
	qrankReader, err = gzip.NewReader(qrankFile)
	if err != nil {
		return "", err
	}

	samplingDistanceSq := 4.0 * 4.0
	var stats Stats
	stats.Samples = make([]Sample, 0, numSamples)
	var id string
	var rank, value int64
	var lastX, lastY, scaleY float64
	scaleX := float64(numSamples) / float64(numRanks)
	scanner := bufio.NewScanner(qrankReader)
	scanner.Scan() // Skip CSV header.
	for scanner.Scan() {
		rank += 1
		cols := strings.Split(scanner.Text(), ",")
		if len(cols) < 2 {
			return "", fmt.Errorf("%s:%d: less than 2 columns", qrankPath, rank-1)
		}

		id = cols[0]
		value, err = strconv.ParseInt(cols[1], 10, 64)
		if err != nil {
			return "", err
		}

		if rank == 1 { // first item in file, this is the maximum value
			scaleY = float64(numSamples) / math.Log10(float64(value))
		}

		x, y := float64(rank)*scaleX, math.Log10(float64(value))*scaleY
		distance := (x-lastX)*(x-lastX) + (y-lastY)*(y-lastY)
		near := distance < samplingDistanceSq
		if rank == medianRank {
			// If the median item is near the last, drop it.
			// Unless the last is among the top N whose inclusion was requested by the caller.
			// In production, the median of ~30M items is never going to be among the top 50,
			// but in unit tests this does happen, and let’s be correct in all cases.
			if near && len(stats.Samples) > topN {
				stats.Samples = stats.Samples[:len(stats.Samples)-1]
			}
			stats.Median = len(stats.Samples)
		}

		if !near || rank <= int64(topN) || rank == medianRank {
			stats.Samples = append(stats.Samples, Sample{id, rank, value})
			lastX, lastY = x, y
		}
	}

	// Make sure the last sample is the minimum value.
	stats.Samples[len(stats.Samples)-1] = Sample{id, rank, value}

	if err := scanner.Err(); err != nil {
		return "", err
	}

	statsPath := filepath.Join(
		outDir,
		fmt.Sprintf("qrank-stats-%04d%02d%02d.json", date.Year(), date.Month(), date.Day()))
	tmpStatsPath := statsPath + ".tmp"

	j, err := json.Marshal(stats)
	if err != nil {
		return "", err
	}
	statsFile, err := os.Create(tmpStatsPath)
	if err != nil {
		return "", err
	}
	defer statsFile.Close()
	if _, err := statsFile.Write(j); err != nil {
		return "", err
	}
	if err := statsFile.Sync(); err != nil {
		return "", err
	}
	if err := statsFile.Close(); err != nil {
		return "", err
	}
	if err := os.Rename(tmpStatsPath, statsPath); err != nil {
		return "", err
	}

	return statsPath, nil
}

// CountLines counts the number of lines in its input.
func countLines(r io.Reader) (int64, error) {
	var count int64
	buf := make([]byte, 64*1024)
	for {
		bufSize, err := r.Read(buf)
		if err != nil && err != io.EOF {
			return 0, err
		}
		var pos int
		for {
			i := bytes.IndexByte(buf[pos:], '\n')
			if i == -1 || pos == bufSize {
				break
			}
			pos += i + 1
			count += 1
		}
		if err == io.EOF {
			break
		}
	}
	return count, nil
}
