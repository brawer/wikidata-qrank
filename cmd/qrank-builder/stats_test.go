// SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestBuildStats(t *testing.T) {
	qrank := filepath.Join(t.TempDir(), "TestStats-qrank.gz")
	writeGzipFile(qrank, "Q4\t77\nQ2\t42\nQ5\t42\nQ1\t1\nQ3\t1\n")
	statsPath, err := buildStats(time.Now(), qrank, t.TempDir())
	if err != nil {
		t.Error(err)
		return
	}

	statsFile, err := os.Open(statsPath)
	if err != nil {
		t.Error(err)
		return
	}
	defer statsFile.Close()

	buf, err := io.ReadAll(statsFile)
	if err != nil {
		t.Error(err)
		return
	}

	var stats Stats
	if err := json.Unmarshal(buf, &stats); err != nil {
		t.Error(err)
		return
	}

	expectedSha256 := "e5e9abaaeda2ccf879ef08c01756b0dae17f1ea66069d34756c1459df3e4b077"
	if expectedSha256 != stats.QRankSha256 {
		t.Errorf("expected %q, got %q", expectedSha256, stats.QRankSha256)
	}
}
