// SPDX-FileCopyrightText: 2024 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"fmt"
	"io"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"

	"github.com/brawer/wikidata-qrank/v2/internal/version"
)

// ItemSignalsRow is one row of the public item signals file,
// public/item_signals-YYYYMMDD.parquet. The column names are part
// of the published format; see doc/downloads.md.
type itemSignalsRow struct {
	Item            int64 `parquet:"item,delta"`
	Pageviews52w    int64 `parquet:"pageviews_52w,delta"`
	WikitextBytes   int64 `parquet:"wikitext_bytes,delta"`
	StatementCount  int64 `parquet:"statement_count,delta"`
	IdentifierCount int64 `parquet:"identifier_count,delta"`
	SitelinkCount   int64 `parquet:"sitelink_count,delta"`

	// OSMViewsScore is null for items without coordinates on Earth.
	OSMViewsScore *float32 `parquet:"osmviews_score,optional"`
}

// ItemSignalsRowGroupSize is the number of rows per Parquet row group.
// The writer buffers one row group in memory, so this bounds its memory
// use; it also lets readers skip ranges of items.
const itemSignalsRowGroupSize = 1 << 20

// ItemSignalsBatchSize is the number of rows passed to the Parquet
// writer at once.
const itemSignalsBatchSize = 4096

// ItemSignalsWriter writes item signals, sorted by item ID, as Parquet.
// Consecutive signals for the same item get summed up into one row.
type ItemSignalsWriter struct {
	signals   ItemSignals
	out       io.WriteCloser
	writer    *parquet.GenericWriter[itemSignalsRow]
	batch     []itemSignalsRow
	scores    *osmviewsScores
	numScored int64
}

func NewItemSignalsWriter(w io.WriteCloser) *ItemSignalsWriter {
	// Parquet's created_by should read "<app> version <version> (build <build>)",
	// but parquet-go omits the space before "(build"; hence the trailing space.
	revision, _ := version.Revision()
	builder := parseBuilderVersion(SoftwareVersion).Version()
	writer := parquet.NewGenericWriter[itemSignalsRow](w,
		parquet.Compression(&zstd.Codec{Level: zstd.SpeedBetterCompression}),
		parquet.MaxRowsPerRowGroup(itemSignalsRowGroupSize),
		parquet.SortingWriterConfig(parquet.SortingColumns(parquet.Ascending("item"))),
		parquet.CreatedBy("qrank-builder", builder+" ", revision),
	)
	return &ItemSignalsWriter{
		out:    w,
		writer: writer,
		batch:  make([]itemSignalsRow, 0, itemSignalsBatchSize),
	}
}

// SetOSMViewsScores makes the writer fill the osmviews_score column
// from `scores`. Without it, the column is null for every item.
func (w *ItemSignalsWriter) SetOSMViewsScores(scores *osmviewsScores) {
	w.scores = scores
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
	if err := w.writeBatch(); err != nil {
		return err
	}
	if err := w.writer.Close(); err != nil {
		return err
	}
	if w.scores != nil {
		logger.Printf("ItemSignalsWriter: %d items have an osmviews_score", w.numScored)
	}
	return w.out.Close()
}

func (w *ItemSignalsWriter) flush() error {
	if w.signals.item == 0 {
		return nil
	}

	row := itemSignalsRow{
		Item:            w.signals.item,
		Pageviews52w:    w.signals.pageviews,
		WikitextBytes:   w.signals.wikitextBytes,
		StatementCount:  w.signals.claims,
		IdentifierCount: w.signals.identifiers,
		SitelinkCount:   w.signals.sitelinks,
	}
	if w.scores != nil {
		score, ok, err := w.scores.Lookup(row.Item)
		if err != nil {
			return err
		}
		if ok {
			row.OSMViewsScore = &score
			w.numScored++
		}
	}
	w.batch = append(w.batch, row)
	w.signals = ItemSignals{}
	if len(w.batch) == cap(w.batch) {
		return w.writeBatch()
	}
	return nil
}

func (w *ItemSignalsWriter) writeBatch() error {
	if len(w.batch) == 0 {
		return nil
	}
	if _, err := w.writer.Write(w.batch); err != nil {
		return err
	}
	w.batch = w.batch[:0]
	return nil
}

// ReadItemSignalsFile reads a Parquet file of item signals, calling
// `fn` for each row in file order.
func readItemSignalsFile(r io.ReaderAt, fn func(row itemSignalsRow) error) error {
	reader := parquet.NewGenericReader[itemSignalsRow](r)
	defer reader.Close()
	rows := make([]itemSignalsRow, itemSignalsBatchSize)
	for {
		n, err := reader.Read(rows)
		for _, row := range rows[:n] {
			if err := fn(row); err != nil {
				return err
			}
		}
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}
	}
}
