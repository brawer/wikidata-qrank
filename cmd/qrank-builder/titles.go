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

// BuildTitles builds the titles file for a WikiSite and puts it in S3 storage.
// The titles file contains a mapping from page titles to Wikidata item IDs,
// such as:
//
// Category:Foo Q123
// Zürich Q72
func buildTitles(site *WikiSite, ctx context.Context, dumps string, s3 S3) error {
	ymd := site.LastDumped.Format("20060102")
	destPath := fmt.Sprintf("titles/%s-%s-titles.zst", site.Key, ymd)
	logger.Printf("building %s", destPath)

	unsorted, err := os.CreateTemp("", "*-titles-unsorted")
	if err != nil {
		return err
	}
	defer unsorted.Close()
	defer os.Remove(unsorted.Name())

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
		if err := readTitles(groupCtx, site, "B", dumps, linesChan); err != nil {
			return err
		}
		return nil
	})
	group.Go(func() error {
		sorter.Sort(groupCtx)
		joiner := NewTitleJoiner(site, unsorted)
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

	path, err := SortLines(ctx, unsorted.Name())
	if err != nil {
		return err
	}

	if err := PutInStorage(ctx, path, s3, "qrank", destPath, "application/zstd"); err != nil {
		return err
	}

	if err := os.Remove(path); err != nil {
		return err
	}

	return nil
}

func readTitles(ctx context.Context, site *WikiSite, property string, dumps string, out chan<- string) error {
	ymd := site.LastDumped.Format("20060102")
	pageFileName := fmt.Sprintf("%s-%s-page.sql.gz", site.Key, ymd)
	pagePath := filepath.Join(dumps, site.Key, ymd, pageFileName)
	pageFile, err := os.Open(pagePath)
	if err != nil {
		return err
	}
	defer pageFile.Close()

	gz, err := gzip.NewReader(pageFile)
	if err != nil {
		return err
	}
	defer gz.Close()

	reader, err := NewSQLReader(gz)
	if err != nil {
		return err
	}

	columns := reader.Columns()
	pageCol := slices.Index(columns, "page_id")
	namespaceCol := slices.Index(columns, "page_namespace")
	titleCol := slices.Index(columns, "page_title")

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		row, err := reader.Read()
		if err != nil {
			return err
		}
		if row == nil {
			return nil
		}

		page := row[pageCol]
		namespace := row[namespaceCol]
		title := row[titleCol]

		var nsPrefix string
		if namespace != "0" {
			if ns, found := site.Namespaces[row[namespaceCol]]; found && ns.Localized != "" {
				nsPrefix = ns.Localized + ":"
			}
		}

		out <- fmt.Sprintf("%s\t%s\t%s%s", page, property, nsPrefix, title)
	}
}

type titleJoiner struct {
	site     *WikiSite
	writer   io.WriteCloser
	page     int64
	title    string
	item     string
	inLines  int64
	outLines int64
}

func NewTitleJoiner(site *WikiSite, w io.WriteCloser) *titleJoiner {
	return &titleJoiner{site: site, writer: w}
}

func (j *titleJoiner) Process(line string) error {
	j.inLines += 1
	cols := strings.Split(line, "\t")
	page, err := strconv.ParseInt(cols[0], 10, 64)
	if err != nil {
		return err
	}

	stream := cols[1]
	if stream == "A" {
		j.page = page
		j.item = cols[2]
		return nil
	}

	if stream == "B" && page == j.page {
		title := cols[2]
		var buf bytes.Buffer
		buf.WriteString(title)
		buf.WriteByte('\t')
		buf.WriteString(j.item)
		buf.WriteByte('\n')
		if _, err := buf.WriteTo(j.writer); err != nil {
			return err
		}
		return nil
	}

	return nil
}

func (j *titleJoiner) Close() error {
	return j.writer.Close()
}
