// SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"bufio"
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
	"strings"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/andybalholm/brotli"
	"github.com/klauspost/compress/zstd"
	"github.com/lanrat/extsort"
	"github.com/minio/minio-go/v7"
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

func buildQRank(date time.Time, qviews string, outDir string, ctx context.Context) (string, error) {
	qrankPath := filepath.Join(
		outDir,
		fmt.Sprintf("qrank-%04d%02d%02d.gz", date.Year(), date.Month(), date.Day()))
	_, err := os.Stat(qrankPath)
	if err == nil {
		return qrankPath, nil // use pre-existing file
	}
	if !os.IsNotExist(err) {
		return "", err
	}

	if logger != nil {
		logger.Printf("building %s", qrankPath)
	}
	start := time.Now()

	qviewsFile, err := os.Open(qviews)
	if err != nil {
		return "", err
	}
	defer qviewsFile.Close()

	read := func(ctx context.Context, ch chan<- extsort.SortType) error {
		return readQViews(brotli.NewReader(qviewsFile), ch, ctx)
	}
	if err := writeQRankFile(ctx, qrankPath, read); err != nil {
		return "", err
	}

	if logger != nil {
		logger.Printf("built %s in %.1fs", qrankPath, time.Since(start).Seconds())
	}

	return qrankPath, nil
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
// of the given date, and puts `public/qrank-YYYYMMDD.csv.gz` and
// `public/qrank-stats-YYYYMMDD.json` into storage. If both files
// are already stored, they do not get re-built.
func buildQRankFiles(ctx context.Context, date time.Time, s3 S3) error {
	ymd := date.Format("20060102")
	signalsPath := fmt.Sprintf("public/item_signals-%s.csv.zst", ymd)
	qrankDest := fmt.Sprintf("public/qrank-%s.csv.gz", ymd)
	statsDest := fmt.Sprintf("public/qrank-stats-%s.json", ymd)

	stored := make(map[string]bool, 10)
	for obj := range s3.ListObjects(ctx, "qrank", minio.ListObjectsOptions{Prefix: "public/"}) {
		if obj.Err != nil {
			return obj.Err
		}
		stored[obj.Key] = true
	}
	if stored[qrankDest] && stored[statsDest] {
		logger.Printf("ranking in storage is still fresh: %s", qrankDest)
		return nil
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
	decompressor, err := zstd.NewReader(signals)
	if err != nil {
		return err
	}
	defer decompressor.Close()

	qrankPath := filepath.Join(tempDir, filepath.Base(qrankDest))
	read := func(ctx context.Context, ch chan<- extsort.SortType) error {
		return readItemSignalsPageviews(ctx, decompressor, ch)
	}
	if err := writeQRankFile(ctx, qrankPath, read); err != nil {
		return err
	}

	statsPath, err := buildStats(date, qrankPath, 50, 1000, tempDir)
	if err != nil {
		return err
	}

	if err := PutInStorage(ctx, qrankPath, s3, "qrank", qrankDest, "text/csv"); err != nil {
		return err
	}
	if err := PutInStorage(ctx, statsPath, s3, "qrank", statsDest, "application/json"); err != nil {
		return err
	}
	logger.Printf("built %s and %s in %.1fs", qrankDest, statsDest, time.Since(start).Seconds())

	for _, re := range []string{`^public/qrank-\d{8}\.csv\.gz$`, `^public/qrank-stats-\d{8}\.json$`} {
		if err := prunePublicFiles(ctx, regexp.MustCompile(re), 3, s3); err != nil {
			return err
		}
	}

	return nil
}

// ReadItemSignalsPageviews reads an item_signals CSV file and sends
// the 52-week pageviews of each item as QRank to a channel, before
// closing that channel. Items without any pageviews are skipped,
// as in the rankings computed before item signals existed.
func readItemSignalsPageviews(ctx context.Context, r io.Reader, ch chan<- extsort.SortType) error {
	defer close(ch)
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "item,") {
			continue // CSV header
		}
		cols := strings.SplitN(line, ",", 3)
		if len(cols) < 2 || len(cols[0]) < 2 || cols[0][0] != 'Q' {
			return fmt.Errorf("expected Q...,pageviews,..., got %q", line)
		}
		entity, err := strconv.ParseInt(cols[0][1:], 10, 64)
		if err != nil {
			return err
		}
		pageviews, err := strconv.ParseInt(cols[1], 10, 64)
		if err != nil {
			return err
		}
		if pageviews <= 0 {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ch <- QRank{entity, pageviews}:
		}
	}
	return scanner.Err()
}

func readQViews(r io.Reader, ch chan<- extsort.SortType, ctx context.Context) error {
	defer close(ch)
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)
		if len(fields) != 2 {
			return fmt.Errorf("expected 2 columns, got %q", line)
		}
		entity := fields[0]
		if len(entity) < 2 || entity[0] != 'Q' {
			return fmt.Errorf("expected Q..., got %q", line)
		}
		e, err := strconv.ParseInt(entity[1:], 10, 64)
		if err != nil {
			return err
		}
		c, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ch <- QRank{e, c}:
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}
