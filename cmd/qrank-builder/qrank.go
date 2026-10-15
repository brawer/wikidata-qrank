// SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/lanrat/extsort"
	"github.com/minio/minio-go/v7"

	"github.com/brawer/wikidata-qrank/v2/internal/version"
)

type QRank struct {
	Entity int64
	Rank   int64
}

func (qr QRank) ToBytes() []byte {
	buf := make([]byte, binary.MaxVarintLen64*2)
	entitySize := binary.PutVarint(buf, qr.Entity)
	rankSize := binary.PutVarint(buf[entitySize:], qr.Rank)
	return buf[0 : entitySize+rankSize]
}

func QRankFromBytes(b []byte) extsort.SortType {
	entity, entitySize := binary.Varint(b)
	rank, _ := binary.Varint(b[entitySize:])
	return QRank{Entity: entity, Rank: rank}
}

func QRankLess(a, b extsort.SortType) bool {
	// Sort by decreasing rank, or (as secondary key) increasing entity ID.
	x, y := a.(QRank), b.(QRank)
	if x.Rank != y.Rank {
		return x.Rank > y.Rank
	} else {
		return x.Entity < y.Entity
	}
}

// WriteQRankFile sorts the QRank values sent by `read` by decreasing rank,
// and writes them as gzip-compressed CSV to `qrankPath`.
func writeQRankFile(ctx context.Context, qrankPath string, read func(ctx context.Context, ch chan<- extsort.SortType) error) error {
	tmpQRankPath := qrankPath + ".tmp"
	tmpQRankFile, err := os.Create(tmpQRankPath)
	if err != nil {
		return err
	}
	defer tmpQRankFile.Close()

	qrankWriter, err := gzip.NewWriterLevel(tmpQRankFile, 9)
	if err != nil {
		return err
	}
	defer qrankWriter.Close()

	ch := make(chan extsort.SortType, 50000)
	g, subCtx := errgroup.WithContext(context.Background())
	config := extsort.DefaultConfig()
	config.NumWorkers = runtime.NumCPU()
	sorter, outChan, errChan := extsort.New(ch, QRankFromBytes, QRankLess, config)
	g.Go(func() error {
		return read(subCtx, ch)
	})
	g.Go(func() error {
		sorter.Sort(ctx) // not subCtx, as per extsort docs
		return nil
	})
	if err := g.Wait(); err != nil {
		return err
	}

	header := "Entity,QRank\n"
	if _, err := qrankWriter.Write([]byte(header)); err != nil {
		return err
	}

	for data := range outChan {
		qr := data.(QRank)
		var buf bytes.Buffer
		buf.WriteByte('Q')
		buf.WriteString(strconv.FormatInt(qr.Entity, 10))
		buf.WriteByte(',')
		buf.WriteString(strconv.FormatInt(qr.Rank, 10))
		buf.WriteByte('\n')
		if _, err := qrankWriter.Write(buf.Bytes()); err != nil {
			return err
		}
	}

	if err := <-errChan; err != nil {
		return err
	}

	if err := qrankWriter.Close(); err != nil {
		return err
	}

	if err := tmpQRankFile.Sync(); err != nil {
		return err
	}

	if err := tmpQRankFile.Close(); err != nil {
		return err
	}

	return os.Rename(tmpQRankPath, qrankPath)
}

// BuildQRankFiles computes the QRank ranking from the item signals
// of the given date, and puts `public/qrank-YYYYMMDD.cdx.json`,
// `public/qrank-stats-YYYYMMDD.json`, `public/qrank-YYYYMMDD.csv.gz`
// and finally `public/datapackage.json` into storage, in this order.
// A client that sees a version in datapackage.json can therefore
// fetch all of its files. If all files are already stored, and
// datapackage.json names this version, nothing gets re-built.
func buildQRankFiles(ctx context.Context, date time.Time, s3 S3) error {
	ymd := date.Format("20060102")
	signalsPath := itemSignalsPath(date)
	qrankDest := fmt.Sprintf("public/qrank-%s.csv.gz", ymd)
	statsDest := fmt.Sprintf("public/qrank-stats-%s.json", ymd)
	bomDest := fmt.Sprintf("public/qrank-%s.cdx.json", ymd)

	stored := make(map[string]bool, 10)
	for obj := range s3.ListObjects(ctx, "qrank", minio.ListObjectsOptions{Prefix: "public/"}) {
		if obj.Err != nil {
			return obj.Err
		}
		stored[obj.Key] = true
	}
	if stored[qrankDest] && stored[statsDest] && stored[bomDest] && stored[datapackagePath] {
		dp, err := readStoredDatapackage(ctx, s3)
		if err != nil {
			return err
		}
		// Data packages of earlier releases did not list the item signals.
		if dp.Version == date.Format(time.DateOnly) && dp.lists(filepath.Base(signalsPath)) {
			logger.Printf("ranking in storage is still fresh: %s", qrankDest)
			return nil
		}
	}

	prov, err := readItemSignalsProvenance(ctx, date, s3)
	if err != nil {
		return fmt.Errorf("provenance of %s: %w", signalsPath, err)
	}

	logger.Printf("building %s from %s", qrankDest, signalsPath)
	start := time.Now()
	tempDir, err := os.MkdirTemp("", "qrank-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tempDir)

	signalsFile := filepath.Join(tempDir, filepath.Base(signalsPath))
	if err := s3.FGetObject(ctx, "qrank", signalsPath, signalsFile, minio.GetObjectOptions{}); err != nil {
		return err
	}
	signals, err := os.Open(signalsFile)
	if err != nil {
		return err
	}
	defer signals.Close()

	qrankPath := filepath.Join(tempDir, filepath.Base(qrankDest))
	read := func(ctx context.Context, ch chan<- extsort.SortType) error {
		return readItemSignalsPageviews(ctx, signals, ch)
	}
	if err := writeQRankFile(ctx, qrankPath, read); err != nil {
		return err
	}

	statsPath, err := buildStats(date, qrankPath, 50, 1000, tempDir)
	if err != nil {
		return err
	}

	bomPath := filepath.Join(tempDir, filepath.Base(bomDest))
	dpPath := filepath.Join(tempDir, filepath.Base(datapackagePath))
	if err := writeBOMAndDatapackage(date, prov, qrankPath, signalsFile, statsPath, bomPath, dpPath); err != nil {
		return err
	}

	uploads := []struct{ src, dest, contentType string }{
		{bomPath, bomDest, "application/vnd.cyclonedx+json"},
		{statsPath, statsDest, "application/json"},
		{qrankPath, qrankDest, "text/csv"},
		{dpPath, datapackagePath, "application/json"},
	}
	for _, u := range uploads {
		if err := PutInStorage(ctx, u.src, s3, "qrank", u.dest, u.contentType); err != nil {
			return err
		}
	}
	logger.Printf("built %s, %s and %s in %.1fs", qrankDest, statsDest, bomDest, time.Since(start).Seconds())

	// BOMs are small and kept forever, so that the provenance of
	// any published QRank file can still be looked up later.
	for _, re := range []string{`^public/qrank-\d{8}\.csv\.gz$`, `^public/qrank-stats-\d{8}\.json$`} {
		if err := prunePublicFiles(ctx, regexp.MustCompile(re), 3, s3); err != nil {
			return err
		}
	}

	return nil
}

// WriteBOMAndDatapackage writes the CycloneDX BOM for the QRank file at
// `qrankPath` and the item signals at `signalsPath` to `bomPath`, and
// then a data package listing the QRank file, the item signals, the
// stats file and the BOM to `dpPath`.
func writeBOMAndDatapackage(date time.Time, prov *ItemSignalsProvenance, qrankPath, signalsPath, statsPath, bomPath, dpPath string) error {
	qrankSHA256, qrankSHA512, qrankBytes, err := hashFile(qrankPath)
	if err != nil {
		return err
	}
	signalsSHA256, signalsSHA512, signalsBytes, err := hashFile(signalsPath)
	if err != nil {
		return err
	}
	revision, modified := version.Revision()
	bom := bomInputs{
		Date:          date,
		SHA256:        qrankSHA256,
		SHA512:        qrankSHA512,
		SignalsSHA256: signalsSHA256,
		SignalsSHA512: signalsSHA512,
		Provenance:    prov,
		Software:      SoftwareVersion,
		Revision:      revision,
		Modified:      modified,
	}
	if err := writeBOM(bomPath, bom); err != nil {
		return err
	}

	statsSHA256, _, statsBytes, err := hashFile(statsPath)
	if err != nil {
		return err
	}
	bomSHA256, _, bomBytes, err := hashFile(bomPath)
	if err != nil {
		return err
	}
	dp := datapackageInputs{
		Date:    date,
		QRank:   datapackageFile{Path: filepath.Base(qrankPath), Bytes: qrankBytes, SHA256: qrankSHA256},
		Signals: datapackageFile{Path: filepath.Base(signalsPath), Bytes: signalsBytes, SHA256: signalsSHA256},
		Stats:   datapackageFile{Path: filepath.Base(statsPath), Bytes: statsBytes, SHA256: statsSHA256},
		BOM:     datapackageFile{Path: filepath.Base(bomPath), Bytes: bomBytes, SHA256: bomSHA256},
	}
	return writeDatapackage(dpPath, dp)
}

// ReadItemSignalsPageviews reads an item_signals Parquet file and sends
// the 52-week pageviews of each item as QRank to a channel, before
// closing that channel. Items without any pageviews are skipped,
// as in the rankings computed before item signals existed.
func readItemSignalsPageviews(ctx context.Context, r io.ReaderAt, ch chan<- extsort.SortType) error {
	defer close(ch)
	return readItemSignalsFile(r, func(row itemSignalsRow) error {
		if row.Pageviews52w <= 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ch <- QRank{row.Item, row.Pageviews52w}:
			return nil
		}
	})
}
