// SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"io"
	"testing"
	"testing/iotest"

	"github.com/klauspost/compress/zstd"
)

// TestZstdDecoder makes sure that the decoder for the zstandard compression
// can handle io.Readers that return results in smaller chunks than requested,
// and that return some data together with the final EOF error.
//
// While trying to find the root cause for a nasty bug related to EOF handling,
// we speculated (wrongly) that the zstd decoder might be buggy in its handling
// of such readers. Howerver, that theory turned out to be wrong.
//
// https://github.com/brawer/wikidata-qrank/issues/40
func TestZstdDecoder(t *testing.T) {
	var buf bytes.Buffer
	zstdLevel := zstd.WithEncoderLevel(zstd.SpeedFastest)
	writer, err := zstd.NewWriter(&buf, zstdLevel)
	if err != nil {
		t.Fatal(err)
	}
	writer.Write([]byte("hello world"))
	writer.Close()

	r := iotest.DataErrReader(iotest.OneByteReader(&buf))
	decoder, err := zstd.NewReader(r, zstd.WithDecoderConcurrency(0))
	if err != nil {
		t.Fatal(err)
	}

	data, err := io.ReadAll(decoder)
	if err != nil {
		t.Fatal(err)
	}

	got, want := string(data), "hello world"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}
