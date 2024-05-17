// SPDX-FileCopyrightText: 2024 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/klauspost/compress/zstd"
	"github.com/lanrat/extsort"
	"github.com/minio/minio-go/v7"
)

// ItemSignals contains ranking signals for Wikidata items.
type ItemSignals struct {
	item          int64 // eg 72 for Q72
	pageviews     int64
	wikitextBytes int64
	claims        int64
	identifiers   int64
	sitelinks     int64
}

// If we ever want to rank signals for Wikidata lexemes, it would
// probably make sense to use a separate struct (written to a different
// output file) because it's likely not the same set of signals.
// For example, lexemes don't have pageviews, pagerank or wikitextBytes.
// https://github.com/brawer/wikidata-qrank/issues/37
// type LexemeSignals struct {}

func (sig *ItemSignals) Clear() {
	sig.item = 0
	sig.pageviews = 0
	sig.wikitextBytes = 0
	sig.claims = 0
	sig.identifiers = 0
	sig.sitelinks = 0
}

func (sig *ItemSignals) Add(other ItemSignals) {
	if sig.item != 0 && sig.item != other.item {
		panic(fmt.Sprintf("cannot add signals for %v and %v", *sig, other))
	}
	sig.pageviews += other.pageviews
	sig.wikitextBytes += other.wikitextBytes
	sig.claims += other.claims
	sig.identifiers += other.identifiers
	sig.sitelinks += other.sitelinks
}

func (s ItemSignals) ToBytes() []byte {
	buf := make([]byte, binary.MaxVarintLen64*6)
	p := binary.PutVarint(buf, s.item)
	p += binary.PutVarint(buf[p:], s.pageviews)
	p += binary.PutVarint(buf[p:], s.wikitextBytes)
	p += binary.PutVarint(buf[p:], s.claims)
	p += binary.PutVarint(buf[p:], s.identifiers)
	p += binary.PutVarint(buf[p:], s.sitelinks)
	return buf[0:p]
}

func ItemSignalsFromBytes(b []byte) extsort.SortType {
	item, pos := binary.Varint(b)
	pageviews, n := binary.Varint(b[pos:])
	pos += n
	wikitextBytes, n := binary.Varint(b[pos:])
	pos += n
	claims, n := binary.Varint(b[pos:])
	pos += n
	identifiers, n := binary.Varint(b[pos:])
	pos += n
	sitelinks, n := binary.Varint(b[pos:])
	return ItemSignals{
		item:          item,
		pageviews:     pageviews,
		wikitextBytes: wikitextBytes,
		claims:        claims,
		identifiers:   identifiers,
		sitelinks:     sitelinks,
	}
}

func ItemSignalsLess(a, b extsort.SortType) bool {
	aa, bb := a.(ItemSignals), b.(ItemSignals)

	if aa.item < bb.item {
		return true
	} else if aa.item > bb.item {
		return false
	}

	if aa.pageviews < bb.pageviews {
		return true
	} else if aa.pageviews > bb.pageviews {
		return false
	}

	if aa.wikitextBytes < bb.wikitextBytes {
		return true
	} else if aa.wikitextBytes > bb.wikitextBytes {
		return false
	}

	if aa.claims < bb.claims {
		return true
	} else if aa.claims > bb.claims {
		return false
	}

	if aa.identifiers < bb.identifiers {
		return true
	} else if aa.identifiers > bb.identifiers {
		return false
	}

	if aa.sitelinks < bb.sitelinks {
		return true
	} else if aa.sitelinks > bb.sitelinks {
		return false
	}

	return false
}

// BuildItemSignals builds per-item signals and puts them in storage.
// If the signals file is already in storage, it does not get re-built.
func buildItemSignals(ctx context.Context, pageviews []string, sites *map[string]WikiSite, s3 S3) (time.Time, error) {
	stored, err := StoredItemSignalsVersion(ctx, s3)
	if err != nil {
		return time.Time{}, err
	}

	newest := ItemSignalsVersion(pageviews, sites)
	if !newest.After(stored) {
		s := stored.Format(time.DateOnly)
		n := newest.Format(time.DateOnly)
		logger.Printf("signals in storage are still fresh: stored=%s, newest=%s", s, n)
		return stored, nil
	}

	newestYMD := newest.Format("20060102")
	destPath := fmt.Sprintf("public/item_signals-%s.csv.zst", newestYMD)
	logger.Printf("building %s", destPath)
	outFile, err := os.CreateTemp("", "*-item_signals.csv.zst")
	if err != nil {
		return time.Time{}, err
	}
	defer outFile.Close()
	defer os.Remove(outFile.Name())

	zstdLevel := zstd.WithEncoderLevel(zstd.SpeedBestCompression)
	compressor, err := zstd.NewWriter(outFile, zstdLevel)
	if err != nil {
		return time.Time{}, err
	}
	defer compressor.Close()

	writer := NewItemSignalsWriter(compressor)
	scanners := make([]LineScanner, 0, len(pageviews)+1)
	scannerNames := make([]string, 0, len(pageviews)+1)
	scanners = append(scanners, NewPageSignalsScanner(sites, s3))
	scannerNames = append(scannerNames, "page_signals")
	for _, pv := range pageviews {
		reader, err := NewS3Reader(ctx, "qrank", pv, s3)
		if err != nil {
			return time.Time{}, err
		}
		decompressor, err := zstd.NewReader(reader)
		if err != nil {
			return time.Time{}, err
		}
		scanners = append(scanners, bufio.NewScanner(decompressor))
		scannerNames = append(scannerNames, pv)
	}

	// TODO: This is just hack to investigate a bug. Remove it.
	// https://github.com/brawer/wikidata-qrank/issues/40
	if true {
		merg := NewLineMerger(scanners, scannerNames)
		logger.Printf("BuildItemSignals(): start testing LineMerger")
		var lastLine string
		var numOrderErrors int64
		var numLines int64
		for merg.Advance() {
			numLines += 1
			line := merg.Line()
			if lastLine >= line {
				numOrderErrors += 1
				if numOrderErrors < 10 {
					logger.Printf(`LineMerger broken: "%s" after "%s"`, line, lastLine)
				}
			}
			lastLine = line
		}
		if err := merg.Err(); err != nil {
			logger.Printf("LineMerger failed: %v", err)
			return time.Time{}, err
		}
		logger.Printf("BuildItemSignals(): finished testing LineMerger, returned %d lines, %d of which were mis-ordered", numLines, numOrderErrors)
		return time.Time{}, nil
	}

	// Produce a stream of ItemSignals, sorted by Wikidata item ID.
	sigChan := make(chan extsort.SortType, 10000)
	config := extsort.DefaultConfig()
	config.ChunkSize = 8 * 1024 * 1024 / 64 // 8 MiB, 64 Bytes/line avg
	config.NumWorkers = runtime.NumCPU()
	sorter, outChan, errChan := extsort.New(sigChan, ItemSignalsFromBytes, ItemSignalsLess, config)
	merger := NewLineMerger(scanners, scannerNames)
	logger.Printf("BuildItemSignals(): merging signals from %d files; #0=PageSignalsScanner; rest=pageviews", len(scanners))
	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error {
		joiner := itemSignalsJoiner{out: sigChan}
		var linesMerged int64
		for merger.Advance() {
			line := merger.Line()
			if err := joiner.Process(line); err != nil {
				joiner.Close()
				logger.Printf(`ItemSignalsJoiner.Process("%s") failed: %v`, line, err)
				return err
			}
			linesMerged += 1
		}
		joiner.Close()
		logger.Printf("ItemSignalsJoiner: read %d lines", linesMerged)
		if err := merger.Err(); err != nil {
			logger.Printf("LineMerger failed: %v", err)
			return err
		}
		return nil
	})
	group.Go(func() error {
		sorter.Sort(groupCtx)
		logger.Printf("BuildItemSignals(): start sorting")
		for {
			select {
			case <-groupCtx.Done():
				err := groupCtx.Err()
				logger.Printf("BuildItemSignals(): sorting canceled, groupCtx.Err()=%v", err)
				return err

			case s, more := <-outChan:
				if !more {
					err := writer.Close()
					if err != nil {
						logger.Printf("ItemSignalsWriter.Close() failed: %v", err)
					}
					return err
				}
				if err := writer.Write(s.(ItemSignals)); err != nil {
					logger.Printf("ItemSignalsWriter.Write() failed: %v", err)
					return err
				}
			}
		}
	})
	if err := group.Wait(); err != nil {
		logger.Printf("BuildItemSignals(): group.Wait() failed, err==%v", err)
		return time.Time{}, err
	}
	if err := <-errChan; err != nil {
		logger.Printf("BuildItemSignals: sorting failed, err=%v", err)
		return time.Time{}, err
	}
	logger.Printf("BuildItemSignals(): finished sorting")
	for _, s := range scanners {
		if closer, ok := s.(io.Closer); ok {
			if err := closer.Close(); err != nil {
				return time.Time{}, err
			}
		}
	}

	if err := PutInStorage(ctx, outFile.Name(), s3, "qrank", destPath, "application/zstd"); err != nil {
		return time.Time{}, err
	}

	if err := os.Remove(outFile.Name()); err != nil {
		return time.Time{}, err
	}

	return newest, nil
}

type itemSignalsJoiner struct {
	out                                                                  chan<- extsort.SortType
	domain                                                               string
	page, item, pageviews, wikitextBytes, claims, identifiers, sitelinks int64
}

func (j *itemSignalsJoiner) Process(line string) error {
	cols := strings.Split(line, ",")
	if len(cols) < 3 {
		return fmt.Errorf(`bad line: "%s"`, line)
	}
	page, err := strconv.ParseInt(cols[1], 10, 64)
	if err != nil {
		return fmt.Errorf(`bad page: "%s"`, line)
	}
	if cols[0] != j.domain || page != j.page {
		j.flush()
		j.domain, j.page = cols[0], page
	}

	c := cols[2]
	if c[0] != 'Q' {
		if n, err := strconv.ParseInt(c, 10, 64); err == nil {
			j.pageviews += n
		} else {
			return err
		}
		if len(cols) != 3 {
			return fmt.Errorf(`expected domain,page,pageviews: "%s"`, line)
		}
		return nil
	}

	item, err := strconv.ParseInt(c[1:len(c)], 10, 64)
	if err != nil {
		return fmt.Errorf(`expected domain,page,item,...: "%s"`, line)
	}
	j.item = item

	if len(cols) > 3 && len(cols[3]) > 0 {
		n, err := strconv.ParseInt(cols[3], 10, 64)
		if err != nil {
			return fmt.Errorf(`cannot parse wikitextBytes: "%s"`, line)
		}
		j.wikitextBytes += n
	}

	if len(cols) > 4 && len(cols[4]) > 0 {
		n, err := strconv.ParseInt(cols[4], 10, 64)
		if err != nil {
			return fmt.Errorf(`cannot parse claims: "%s"`, line)
		}
		j.claims += n
	}

	if len(cols) > 5 && len(cols[5]) > 0 {
		n, err := strconv.ParseInt(cols[5], 10, 64)
		if err != nil {
			return fmt.Errorf(`cannot parse identifiers: "%s"`, line)
		}
		j.identifiers += n
	}

	if len(cols) > 6 && len(cols[6]) > 0 {
		n, err := strconv.ParseInt(cols[6], 10, 64)
		if err != nil {
			return fmt.Errorf(`cannot parse sitelinks: "%s"`, line)
		}
		j.sitelinks += n
	}

	return nil
}

func (j *itemSignalsJoiner) Close() {
	j.flush()
	close(j.out)
}

func (j *itemSignalsJoiner) flush() {
	if j.item != 0 {
		j.out <- ItemSignals{
			item:          j.item,
			pageviews:     j.pageviews,
			wikitextBytes: j.wikitextBytes,
			claims:        j.claims,
			identifiers:   j.identifiers,
			sitelinks:     j.sitelinks,
		}
	}
	j.domain = ""
	j.page = 0
	j.item = 0
	j.pageviews = 0
	j.wikitextBytes = 0
	j.claims = 0
	j.identifiers = 0
	j.sitelinks = 0
}

func ItemSignalsVersion(pageviews []string, sites *map[string]WikiSite) time.Time {
	var date time.Time
	re := regexp.MustCompile(`^pageviews/pageviews-(\d{4}-W\d{2}).zst$`)
	for _, pv := range pageviews {
		if match := re.FindStringSubmatch(pv); match != nil {
			if year, week, err := ParseISOWeek(match[1]); err == nil {
				weekStart := ISOWeekStart(year, week)
				weekEnd := weekStart.AddDate(0, 0, 6) // weekStart + 6 days
				if weekEnd.After(date) {
					date = weekEnd
				}
			}
		}
	}

	for _, site := range *sites {
		if site.LastDumped.After(date) {
			date = site.LastDumped
		}
	}

	return date
}

// StoredItemSignalsVersion returns the version of the signals file in storage.
// If there is no such file, the result is the zero time.Time without error.
func StoredItemSignalsVersion(ctx context.Context, s3 S3) (time.Time, error) {
	re := regexp.MustCompile(`^public/item_signals-(\d{8}).csv.zst$`)
	var result time.Time
	opts := minio.ListObjectsOptions{Prefix: "public/"}
	for obj := range s3.ListObjects(ctx, "qrank", opts) {
		if obj.Err != nil {
			return time.Time{}, obj.Err
		}
		if match := re.FindStringSubmatch(obj.Key); match != nil {
			if t, err := time.Parse(match[1], "20060201"); err == nil {
				if t.After(result) {
					result = t
				}
			}
		}
	}

	return result, nil
}
