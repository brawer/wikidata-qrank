// SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

type Stats struct {
	QRankFilename string `json:"qrank-filename"`
	QRankSha256   string `json:"qrank-sha256"`
}

func buildStats(date time.Time, qrank string, outDir string) (string, error) {
	statsPath := filepath.Join(
		outDir,
		fmt.Sprintf("stats-%04d%02d%02d.json", date.Year(), date.Month(), date.Day()))
	tmpStatsPath := statsPath + ".tmp"

	var stats Stats
	stats.QRankFilename = filepath.Base(qrank)
	h, err := getSha256(qrank)
	if err != nil {
		return "", err
	}
	stats.QRankSha256 = h

	j, err := json.MarshalIndent(stats, "", "    ")
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

func getSha256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}

	return fmt.Sprintf("%x", h.Sum(nil)), nil
}
