// SPDX-FileCopyrightText: 2024 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"slices"
	"strings"
	"testing"
)

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

	got := strings.Split(strings.TrimSuffix(string(buf.Bytes()), "\n"), "\n")
	want := []string{
		"item,pageviews_52w,wikitext_bytes,claims,identifiers,sitelinks",
		"Q72,4,5,6,7,8",
		"Q99,9,8,7,6,5",
	}
	if !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestItemSignalsWriter_Empty(t *testing.T) {
	var buf bytes.Buffer
	w := NewItemSignalsWriter(NopWriteCloser(&buf))
	if err := w.Close(); err != nil {
		t.Error(err)
	}
	got := string(buf.Bytes())
	if got != "" {
		t.Errorf(`got "%s", want ""`, got)
	}
}

func TestItemSignalsWriter_ZeroItem(t *testing.T) {
	var buf bytes.Buffer
	w := NewItemSignalsWriter(NopWriteCloser(&buf))
	if err := w.Write(ItemSignals{0, 1, 2, 3, 4, 5}); err == nil {
		t.Error("expected error, got nil")
	}
}
