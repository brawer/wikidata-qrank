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

/* TODO: Remove this once we’re sure the code works.

func TestBuildStats222222(t *testing.T) {
	statsPath, err := buildStats(time.Now(), "/Users/sascha/src/wikidata-qrank/cache/qrank-20211220.gz", 50, 1000, "/Users/sascha/src/wikidata-qrank/testout")
	if err != nil {
	   t.Fatal(err)
	}
	fmt.Println("******************", statsPath)
}
*/

func TestBuildStats(t *testing.T) {
	qrank := filepath.Join(t.TempDir(), "TestStats-qrank.gz")
	writeGzipFile(qrank,
		`Entity,QRank
Q1,4721864130
Q2,107330319
Q3,69160330
Q4,5111172
Q5,51123
Q6,156
Q7,1
Q8,1
Q9,1
`)
	statsPath, err := buildStats(time.Now(), qrank, 2, 5, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	statsFile, err := os.Open(statsPath)
	if err != nil {
		t.Fatal(err)
	}
	defer statsFile.Close()

	buf, err := io.ReadAll(statsFile)
	if err != nil {
		t.Fatal(err)
	}

	var stats Stats
	if err := json.Unmarshal(buf, &stats); err != nil {
		t.Fatal(err)
	}

	got := string(buf)
	want := `{"Median":2,"Samples":[["Q1",1,4721864130],["Q2",2,107330319],["Q5",5,51123],["Q9",9,1]]}`
	if got != want {
		t.Errorf("got %s, want %s", got, want)
	}
}
