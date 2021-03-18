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
	"runtime"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/andybalholm/brotli"
	"github.com/lanrat/extsort"
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
	tmpQRankPath := qrankPath + ".tmp"
	tmpQRankFile, err := os.Create(tmpQRankPath)
	if err != nil {
		return "", err
	}
	defer tmpQRankFile.Close()

	qrankWriter, err := gzip.NewWriterLevel(tmpQRankFile, 9)
	if err != nil {
		return "", err
	}
	defer qrankWriter.Close()

	qviewsFile, err := os.Open(qviews)
	if err != nil {
		return "", err
	}
	defer qviewsFile.Close()

	ch := make(chan extsort.SortType, 50000)
	g, subCtx := errgroup.WithContext(context.Background())
	config := extsort.DefaultConfig()
	config.NumWorkers = runtime.NumCPU()
	sorter, outChan, errChan := extsort.New(ch, QRankFromBytes, QRankLess, config)
	g.Go(func() error {
		return readQViews(brotli.NewReader(qviewsFile), ch, subCtx)
	})
	g.Go(func() error {
		sorter.Sort(ctx) // not subCtx, as per extsort docs
		return nil
	})
	if err := g.Wait(); err != nil {
		return "", err
	}

	header := "Entity,QRank\n"
	if _, err := qrankWriter.Write([]byte(header)); err != nil {
		return "", err
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
			return "", err
		}
	}

	if err := <-errChan; err != nil {
		return "", err
	}

	if err := qrankWriter.Close(); err != nil {
	}

	if err := tmpQRankFile.Sync(); err != nil {
	}

	if err := tmpQRankFile.Close(); err != nil {
	}

	if err := os.Rename(tmpQRankPath, qrankPath); err != nil {
		return "", err
	}

	if logger != nil {
		logger.Printf("built %s in %.1fs", qrankPath, time.Since(start).Seconds())
	}

	return qrankPath, nil
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
