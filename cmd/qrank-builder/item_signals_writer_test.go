// SPDX-FileCopyrightText: 2024 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
)

// ItemSignalsHeader is the header line that itemSignalsToLines emits,
// and itemSignalsFromLines expects. It lists the Parquet column names.
const itemSignalsHeader = "item,pageviews_52w,wikitext_bytes,statement_count,identifier_count,sitelink_count,osmviews_score"

// ItemSignalsToLines converts a Parquet file of item signals to CSV lines,
// for comparing against expectations in tests.
func itemSignalsToLines(data []byte) ([]string, error) {
	lines := []string{itemSignalsHeader}
	err := readItemSignalsFile(bytes.NewReader(data), func(row itemSignalsRow) error {
		score := ""
		if row.OSMViewsScore != nil {
			score = strconv.FormatFloat(float64(*row.OSMViewsScore), 'g', -1, 32)
		}
		lines = append(lines, fmt.Sprintf("%d,%d,%d,%d,%d,%d,%s",
			row.Item, row.Pageviews52w, row.WikitextBytes,
			row.StatementCount, row.IdentifierCount, row.SitelinkCount, score))
		return nil
	})
	return lines, err
}

// ItemSignalsFromLines converts CSV lines, as emitted by itemSignalsToLines,
// to a Parquet file of item signals, for setting up test inputs.
func itemSignalsFromLines(lines []string) ([]byte, error) {
	var signals []ItemSignals
	var scores bytes.Buffer
	for _, line := range lines {
		if line == itemSignalsHeader {
			continue
		}
		var v [6]int64
		cols := strings.Split(line, ",")
		if len(cols) != len(v)+1 {
			return nil, fmt.Errorf("bad item signals line: %q", line)
		}
		for i, c := range cols[:len(v)] {
			n, err := strconv.ParseInt(c, 10, 64)
			if err != nil {
				return nil, err
			}
			v[i] = n
		}
		signals = append(signals, ItemSignals{v[0], v[1], v[2], v[3], v[4], v[5]})
		if c := cols[len(v)]; c != "" {
			score, err := strconv.ParseFloat(c, 32)
			if err != nil {
				return nil, err
			}
			var rec [12]byte
			binary.LittleEndian.PutUint64(rec[0:8], uint64(v[0]))
			binary.LittleEndian.PutUint32(rec[8:12], math.Float32bits(float32(score)))
			scores.Write(rec[:])
		}
	}

	var buf bytes.Buffer
	w := NewItemSignalsWriter(NopWriteCloser(&buf))
	w.SetOSMViewsScores(newOSMViewsScores(&scores))
	for _, s := range signals {
		if err := w.Write(s); err != nil {
			return nil, err
		}
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func TestItemSignalsWriter(t *testing.T) {
	var buf bytes.Buffer
	writer := TestingWriteCloser(&buf)
	w := NewItemSignalsWriter(writer)
	for _, s := range []ItemSignals{
		ItemSignals{72, 1, 2, 3, 4, 5},
		ItemSignals{72, 3, 3, 3, 3, 3},
		ItemSignals{99, 9, 8, 7, 6, 5},
	} {
		if err := w.Write(s); err != nil {
			t.Error(err)
		}
	}
	if writer.closed {
		t.Error("ItemSignalsWriter has prematurely closed its output writer")
	}
	if err := w.Close(); err != nil {
		t.Error(err)
	}
	if !writer.closed {
		t.Error("ItemSignalsWriter.Close() should close its output writer")
	}

	got, err := itemSignalsToLines(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		itemSignalsHeader,
		"72,4,5,6,7,8,",
		"99,9,8,7,6,5,",
	}
	if !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

// The column names and types are part of the published format,
// documented in doc/downloads.md and the data package.
func TestItemSignalsWriter_Schema(t *testing.T) {
	var buf bytes.Buffer
	w := NewItemSignalsWriter(NopWriteCloser(&buf))
	if err := w.Write(ItemSignals{72, 1, 2, 3, 4, 5}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, field := range f.Schema().Fields() {
		got = append(got, fmt.Sprintf("%s:%s", field.Name(), field.Type()))
	}
	want := []string{
		"item:INT(64,true)",
		"pageviews_52w:INT(64,true)",
		"wikitext_bytes:INT(64,true)",
		"statement_count:INT(64,true)",
		"identifier_count:INT(64,true)",
		"sitelink_count:INT(64,true)",
		"osmviews_score:FLOAT",
	}
	if !slices.Equal(got, want) {
		t.Errorf("got schema %v, want %v", got, want)
	}

	md := f.Metadata()
	if len(md.RowGroups) != 1 || len(md.RowGroups[0].SortingColumns) != 1 {
		t.Fatalf("got row groups %+v, want one sorted by item", md.RowGroups)
	}
	if sc := md.RowGroups[0].SortingColumns[0]; sc.ColumnIdx != 0 || sc.Descending {
		t.Errorf("got sorting column %+v, want ascending item", sc)
	}
}

func TestItemSignalsWriter_Empty(t *testing.T) {
	var buf bytes.Buffer
	w := NewItemSignalsWriter(NopWriteCloser(&buf))
	if err := w.Close(); err != nil {
		t.Error(err)
	}
	got, err := itemSignalsToLines(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{itemSignalsHeader}; !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestItemSignalsWriter_ZeroItem(t *testing.T) {
	var buf bytes.Buffer
	w := NewItemSignalsWriter(NopWriteCloser(&buf))
	if err := w.Write(ItemSignals{0, 1, 2, 3, 4, 5}); err == nil {
		t.Error("expected error, got nil")
	}
}
