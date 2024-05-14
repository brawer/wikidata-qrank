// SPDX-FileCopyrightText: 2024 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"
)

type ItemSignalsWriter struct {
	signals     ItemSignals
	out         io.WriteCloser
	wroteHeader bool
}

func NewItemSignalsWriter(w io.WriteCloser) *ItemSignalsWriter {
	return &ItemSignalsWriter{out: w, wroteHeader: false}
}

func (w *ItemSignalsWriter) Write(s ItemSignals) error {
	if s.item == 0 {
		return fmt.Errorf("cannot write ItemSignals for item 0: %v", s)
	}

	if s.item != w.signals.item {
		if err := w.flush(); err != nil {
			return err
		}
	}

	w.signals.item = s.item
	w.signals.Add(s)
	return nil
}

func (w *ItemSignalsWriter) Close() error {
	if err := w.flush(); err != nil {
		return err
	}
	return w.out.Close()
}

func (w *ItemSignalsWriter) flush() error {
	if w.signals.item == 0 {
		return nil
	}

	if !w.wroteHeader {
		header := strings.Join([]string{
			"item",
			"pageviews_52w",
			"wikitext_bytes",
			"claims",
			"identifiers",
			"sitelinks",
		}, ",")
		var hbuf bytes.Buffer
		hbuf.WriteString(header)
		hbuf.WriteByte('\n')
		if _, err := w.out.Write(hbuf.Bytes()); err != nil {
			return err
		}
		w.wroteHeader = true
	}

	var buf bytes.Buffer
	buf.WriteByte('Q')
	buf.WriteString(strconv.FormatInt(w.signals.item, 10))
	buf.WriteByte(',')
	buf.WriteString(strconv.FormatInt(w.signals.pageviews, 10))
	buf.WriteByte(',')
	buf.WriteString(strconv.FormatInt(w.signals.wikitextBytes, 10))
	buf.WriteByte(',')
	buf.WriteString(strconv.FormatInt(w.signals.claims, 10))
	buf.WriteByte(',')
	buf.WriteString(strconv.FormatInt(w.signals.identifiers, 10))
	buf.WriteByte(',')
	buf.WriteString(strconv.FormatInt(w.signals.sitelinks, 10))
	buf.WriteByte('\n')

	w.signals.Clear()
	_, err := w.out.Write(buf.Bytes())
	return err
}
