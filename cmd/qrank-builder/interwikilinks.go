// SPDX-FileCopyrightText: 2024 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"

	"golang.org/x/sync/errgroup"

	"github.com/lanrat/extsort"
)

// BuildInterwikiLinks builds the interwiki_links file for a WikiSite and puts it in S3 storage.
func buildInterwikiLinks(site *WikiSite, ctx context.Context, dumps string, s3 S3) error {
	ymd := site.LastDumped.Format("20060102")
	destPath := fmt.Sprintf("interwiki_links/%s-%s-interwiki_links.zst", site.Key, ymd)
	logger.Printf("building %s", destPath)

	outFile, err := os.CreateTemp("", "*-interwiki_links.zst")
	if err != nil {
		return err
	}
	defer os.Remove(outFile.Name())

	linesChan := make(chan string, 10000)
	config := extsort.DefaultConfig()
	config.ChunkSize = 8 * 1024 * 1024 / 64 // 8 MiB, 64 Bytes/line avg
	config.NumWorkers = runtime.NumCPU()
	sorter, outChan, errChan := extsort.Strings(linesChan, config)

	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error {
		defer close(linesChan)
		if err := ReadPageItems(groupCtx, site, "A", s3, linesChan); err != nil {
			return err
		}
		if err := processInterwikiLinks(groupCtx, site, "B", dumps, linesChan); err != nil {
			return err
		}
		return nil
	})
	group.Go(func() error {
		sorter.Sort(groupCtx)
		joiner := NewInterwikiLinksJoiner(site, outFile)
		for {
			select {
			case <-groupCtx.Done():
				return groupCtx.Err()

			case line, more := <-outChan:
				if !more {
					return joiner.Close()
				}
				err := joiner.Process(line)
				if err != nil {
					return err
				}
			}
		}
	})
	if err := group.Wait(); err != nil {
		return err
	}
	if err := <-errChan; err != nil {
		return err
	}
	sorted, err := SortLines(ctx, outFile.Name())
	if err != nil {
		return err
	}
	defer os.Remove(sorted)

	if err := PutInStorage(ctx, sorted, s3, "qrank", destPath, "application/zstd"); err != nil {
		return err
	}

	return nil
}

// ProcessInterwikiLinks reads the iwlinks SQL dump for a wiki site
// and emits lines of the form `<SourcePageID>,<property>,<TargetDomain>,<TargetTitle>`
// to an output channel.
func processInterwikiLinks(ctx context.Context, site *WikiSite, property string, dumps string, out chan<- string) error {
	ymd := site.LastDumped.Format("20060102")
	propsFileName := fmt.Sprintf("%s-%s-iwlinks.sql.gz", site.Key, ymd)
	propsPath := filepath.Join(dumps, site.Key, ymd, propsFileName)
	propsFile, err := os.Open(propsPath)
	if err != nil {
		return err
	}
	defer propsFile.Close()

	gz, err := gzip.NewReader(propsFile)
	if err != nil {
		return err
	}
	defer gz.Close()

	reader, err := NewSQLReader(gz)
	if err != nil {
		return err
	}

	columns := reader.Columns()
	fromCol := slices.Index(columns, "iwl_from")
	prefixCol := slices.Index(columns, "iwl_prefix")
	titleCol := slices.Index(columns, "iwl_title")

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			row, err := reader.Read()
			if err != nil {
				return err
			}
			if row == nil {
				return nil
			}

			title := row[titleCol]
			var buf strings.Builder
			buf.WriteString(row[fromCol])
			buf.WriteRune('\t')
			buf.WriteString(property)
			buf.WriteRune('\t')
			buf.WriteString(row[prefixCol])
			buf.WriteRune('\t')
			buf.WriteString(title)

			if !strings.ContainsRune(title, '\t') {
				out <- buf.String()
			}
		}
	}
}

type interwikiLinksJoiner struct {
	site     *WikiSite
	writer   io.WriteCloser
	page     int64
	item     string
	inLines  int64
	outLines int64
}

func NewInterwikiLinksJoiner(site *WikiSite, w io.WriteCloser) *interwikiLinksJoiner {
	return &interwikiLinksJoiner{site: site, writer: w, page: 0, item: ""}
}

func (j *interwikiLinksJoiner) Process(line string) error {
	j.inLines += 1
	cols := strings.Split(line, "\t")
	stream := cols[1]

	page, err := strconv.ParseInt(cols[0], 10, 64)
	if err != nil {
		return err
	}

	if stream == "A" {
		j.page = page
		j.item = cols[2]
		return nil
	}

	if stream == "B" && page == j.page {
		if site := j.site.ResolveInterwikiPrefix(cols[2]); site != nil {
			title := cols[3]

			// Resolve interwiki prefixes in titles such as "it:m:Foobar".
			for {
				pos := strings.IndexRune(title, ':')
				if pos <= 0 {
					break
				}
				target := site.ResolveInterwikiPrefix(title[0:pos])
				if target == nil {
					break
				}
				site = target
				title = title[pos+1 : len(title)]
			}

			var buf bytes.Buffer
			buf.WriteString(site.Domain)
			buf.WriteRune('\t')
			buf.WriteString(title)
			buf.WriteRune('\t')
			buf.WriteString(j.item)
			buf.WriteRune('\n')
			_, err := j.writer.Write(buf.Bytes())
			return err
		}
	}

	return nil
}

func (j *interwikiLinksJoiner) Close() error {
	return j.writer.Close()
}
