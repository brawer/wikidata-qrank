// SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"bufio"
	"bytes"
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

type QViewCount struct {
	entity int64
	count  int64
}

func (qv QViewCount) ToBytes() []byte {
	buf := make([]byte, binary.MaxVarintLen64*2)
	entitySize := binary.PutVarint(buf, qv.entity)
	countSize := binary.PutVarint(buf[entitySize:], qv.count)
	return buf[0 : entitySize+countSize]
}

func QViewCountFromBytes(b []byte) extsort.SortType {
	entity, entitySize := binary.Varint(b)
	count, _ := binary.Varint(b[entitySize:])
	return QViewCount{entity: entity, count: count}
}

func QViewCountLess(a, b extsort.SortType) bool {
	return a.(QViewCount).entity < b.(QViewCount).entity
}

func buildQViews(testRun bool, date time.Time, sitelinks string, pageviews []string, outDir string, ctx context.Context) (string, error) {
	qviewsPath := filepath.Join(
		outDir,
		fmt.Sprintf("qviews-%04d%02d%02d.br", date.Year(), date.Month(), date.Day()))
	_, err := os.Stat(qviewsPath)
	if err == nil {
		return qviewsPath, nil // use pre-existing file
	}
	if !os.IsNotExist(err) {
		return "", err
	}

	if logger != nil {
		logger.Printf("building %s", qviewsPath)
	}
	start := time.Now()
	tmpQViewsPath := qviewsPath + ".tmp"
	tmpQViewsFile, err := os.Create(tmpQViewsPath)
	if err != nil {
		return "", err
	}
	defer tmpQViewsFile.Close()

	qviewsWriter := brotli.NewWriterLevel(tmpQViewsFile, 9)
	defer qviewsWriter.Close()

	sitelinksFile, err := os.Open(sitelinks)
	if err != nil {
		return "", err
	}
	defer sitelinksFile.Close()

	qfiles := make([]io.Reader, 1, len(pageviews)+1)
	qfiles[0] = brotli.NewReader(sitelinksFile)
	for _, pv := range pageviews {
		pvFile, err := os.Open(pv)
		if err != nil {
			return "", err
		}
		defer pvFile.Close()
		qfiles = append(qfiles, brotli.NewReader(pvFile))
	}

	ch := make(chan extsort.SortType, 10000)
	g, subCtx := errgroup.WithContext(context.Background())
	config := extsort.DefaultConfig()
	config.NumWorkers = runtime.NumCPU()
	sorter, outChan, errChan := extsort.New(ch, QViewCountFromBytes, QViewCountLess, config)
	g.Go(func() error {
		return readQViewInputs(testRun, qfiles, ch, subCtx)
	})
	g.Go(func() error {
		sorter.Sort(ctx) // not subCtx, as per extsort docs
		return nil
	})
	if err := g.Wait(); err != nil {
		return "", err
	}
	var entity, count int64
	for data := range outChan {
		c := data.(QViewCount)
		if c.entity != entity {
			if err := writeQViewCount(qviewsWriter, entity, count); err != nil {
				return "", err
			}
			entity = c.entity
			count = 0
		}
		count += c.count
	}
	if err := writeQViewCount(qviewsWriter, entity, count); err != nil {
		return "", err
	}

	if err := <-errChan; err != nil {
		return "", err
	}

	if err := qviewsWriter.Close(); err != nil {
	}

	if err := tmpQViewsFile.Sync(); err != nil {
	}

	if err := tmpQViewsFile.Close(); err != nil {
	}

	if err := os.Rename(tmpQViewsPath, qviewsPath); err != nil {
		return "", err
	}

	if logger != nil {
		logger.Printf("built %s in %.1fs", qviewsPath, time.Since(start).Seconds())
	}

	return qviewsPath, nil
}

func writeQViewCount(w io.Writer, entity int64, count int64) error {
	if entity <= 0 || count <= 0 {
		return nil
	}
	var buf bytes.Buffer
	buf.WriteByte('Q')
	buf.WriteString(strconv.FormatInt(entity, 10))
	buf.WriteByte(' ')
	buf.WriteString(strconv.FormatInt(count, 10))
	buf.WriteByte('\n')
	_, err := w.Write(buf.Bytes())
	return err
}

func readQViewInputs(testRun bool, inputs []io.Reader, ch chan<- extsort.SortType, ctx context.Context) error {
	defer close(ch)
	scanners := make([]LineScanner, 0, len(inputs))
	for _, input := range inputs {
		scanners = append(scanners, bufio.NewScanner(input))
	}
	merger := NewLineMerger(scanners)
	var lastKey string
	var entity, numViews, numLinesRead int64
	for merger.Advance() {
		if testRun {
			numLinesRead++
			if numLinesRead > 10000 {
				break
			}
		}

		cols := strings.Fields(merger.Line())
		if len(cols) != 2 {
			continue
		}
		key, value := cols[0], cols[1]
		if key != lastKey {
			if entity > 0 && numViews > 0 {
				ch <- QViewCount{entity, numViews}
			}
			lastKey = key
			numViews = 0
			entity = 0
		}
		if value[0] == 'Q' {
			e, err := strconv.ParseInt(value[1:], 10, 64)
			if err != nil {
				return err
			}
			entity = e
		} else {
			c, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return err
			}
			numViews += c
		}
	}

	if err := merger.Err(); err != nil {
		return err
	}

	return nil
}
