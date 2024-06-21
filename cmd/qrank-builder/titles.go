// SPDX-FileCopyrightText: 2024 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"bufio"
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

	"github.com/klauspost/compress/zstd"
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
	destRedirectsPath := fmt.Sprintf("redirects/%s-%s-redirects.zst", site.Key, ymd)
	logger.Printf("building %s and %s", destPath, destRedirectsPath)

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
		if err := ReadPageItemsOld(groupCtx, site, "A", s3, linesChan); err != nil {
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

	titleItemsPath, err := SortLines(ctx, unsorted.Name())
	if err != nil {
		return err
	}
	defer os.Remove(titleItemsPath)

	redirectTitlesPath, err := buildRedirectTitles(ctx, site, dumps)
	if err != nil {
		return err
	}
	defer os.Remove(redirectTitlesPath)

	redirectsPath, err := buildRedirects(ctx, site, titleItemsPath, redirectTitlesPath)
	if err != nil {
		return err
	}
	defer os.Remove(redirectsPath)

	if err := PutInStorage(ctx, titleItemsPath, s3, "qrank", destPath, "application/zstd"); err != nil {
		return err
	}

	if err := PutInStorage(ctx, redirectsPath, s3, "qrank", destRedirectsPath, "application/zstd"); err != nil {
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

func buildRedirectTitles(ctx context.Context, site *WikiSite, dumps string) (string, error) {
	file, err := os.CreateTemp("", "redirect-titles-*")
	if err != nil {
		return "", err
	}
	defer file.Close()
	defer os.Remove(file.Name())

	linesChan := make(chan string, 10000)
	config := extsort.DefaultConfig()
	config.ChunkSize = 8 * 1024 * 1024 / 64 // 8 MiB, 64 Bytes/line avg
	config.NumWorkers = runtime.NumCPU()
	sorter, outChan, errChan := extsort.Strings(linesChan, config)

	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error {
		defer close(linesChan)
		if err := readRedirects(groupCtx, site, "A", dumps, linesChan); err != nil {
			return err
		}
		if err := readTitles(groupCtx, site, "B", dumps, linesChan); err != nil {
			return err
		}
		return nil
	})
	group.Go(func() error {
		sorter.Sort(groupCtx)
		joiner := NewRedirectTitleJoiner(site, file)
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
		return "", err
	}
	if err := <-errChan; err != nil {
		return "", err
	}

	path, err := SortLines(ctx, file.Name())
	if err != nil {
		return "", err
	}

	return path, nil
}

func readRedirects(ctx context.Context, site *WikiSite, property string, dumps string, out chan<- string) error {
	ymd := site.LastDumped.Format("20060102")
	filename := fmt.Sprintf("%s-%s-redirect.sql.gz", site.Key, ymd)
	path := filepath.Join(dumps, site.Key, ymd, filename)
	file, err := os.Open(path)
	if os.IsNotExist(err) {
		// Intentionally not failing when a wiki has no redirects file.
		return nil
	} else if err != nil {
		return err
	}
	defer file.Close()

	gz, err := gzip.NewReader(file)
	if err != nil {
		return err
	}
	defer gz.Close()

	reader, err := NewSQLReader(gz)
	if err != nil {
		return err
	}

	columns := reader.Columns()
	fromCol := slices.Index(columns, "rd_from")
	namespaceCol := slices.Index(columns, "rd_namespace")
	titleCol := slices.Index(columns, "rd_title")
	interwikiCol := slices.Index(columns, "rd_interwiki")

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

		from := row[fromCol]
		title := row[titleCol]
		namespace, _ := site.Namespaces[row[namespaceCol]]
		interwiki := row[interwikiCol]

		var namespacePrefix string
		if namespace != nil && len(namespace.Localized) > 0 {
			namespacePrefix = namespace.Localized + ":"
		}

		// TODO: Maybe handle interwiki redirects at some point in time.
		// They are quite rare, so it's probably fine if we ignore them
		// for the purpose of computing PageRank for Wikidata.
		if interwiki == "" {
			out <- fmt.Sprintf("%s\t%s\t%s%s", from, property, namespacePrefix, title)
		}
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

type redirectTitleJoiner struct {
	site     *WikiSite
	writer   io.WriteCloser
	page     int64
	title    string
	target   string
	inLines  int64
	outLines int64
}

func NewRedirectTitleJoiner(site *WikiSite, w io.WriteCloser) *redirectTitleJoiner {
	return &redirectTitleJoiner{site: site, writer: w}
}

func (j *redirectTitleJoiner) Process(line string) error {
	j.inLines += 1
	cols := strings.Split(line, "\t")
	page, err := strconv.ParseInt(cols[0], 10, 64)
	if err != nil {
		return err
	}

	stream := cols[1]
	if stream == "A" {
		j.page = page
		j.target = cols[2]
		return nil
	}

	if stream == "B" && page == j.page {
		title := cols[2]
		var buf bytes.Buffer
		buf.WriteString(j.target)
		buf.WriteByte('\t')
		buf.WriteString(title)
		buf.WriteByte('\n')
		if _, err := buf.WriteTo(j.writer); err != nil {
			return err
		}
		return nil
	}

	return nil
}

func (j *redirectTitleJoiner) Close() error {
	return j.writer.Close()
}

// buildRedirects mixes regular page titles with redirects to build the final `redirects` file.
// Both input files contain tab-separated values in zstandard compression; their lines
// must be sorted.
//
// For example, a titleItem "Zürich Q72" gets merged with a redirectTitle "Zürich Zurigo"
// into the two output lines "Zurigo Q72" and "Zürich Q72". The lines in the output file
// are in strong sort order; its file path (in a temporary directoy) is returned as a result.
func buildRedirects(ctx context.Context, site *WikiSite, titleItemsPath string, redirectTitlesPath string) (string, error) {
	scanners := make([]LineScanner, 0, 2)
	scannerNames := make([]string, 0, 2)
	for _, path := range []string{titleItemsPath, redirectTitlesPath} {
		file, err := os.Open(path)
		if err != nil {
			return "", err
		}
		defer file.Close()
		reader, err := zstd.NewReader(file)
		if err != nil {
			return "", err
		}
		scanners = append(scanners, bufio.NewScanner(reader))
		scannerNames = append(scannerNames, path)
	}

	redirectItems, err := os.CreateTemp("", "redirect_items-*.tsv")
	if err != nil {
		return "", err
	}
	defer os.Remove(redirectItems.Name())

	redirectItemsPath := redirectItems.Name()
	writer := bufio.NewWriterSize(redirectItems, 128*1024)

	merger := NewLineMerger(scanners, scannerNames)
	var title string
	var item string
	aliases := make([]string, 0, 10)
	write := func() error {
		if len(item) > 0 && len(aliases) > 0 {
			for _, alias := range aliases {
				if _, err := writer.WriteString(alias); err != nil {
					return err
				}
				if _, err := writer.WriteString("\t"); err != nil {
					return err
				}
				if _, err := writer.WriteString(item); err != nil {
					return err
				}
				if _, err := writer.WriteString("\n"); err != nil {
					return err
				}
			}
		}
		title = ""
		item = ""
		aliases = aliases[:0]
		return nil
	}
	for merger.Advance() {
		cols := strings.Split(merger.Line(), "\t")
		if cols[0] != title {
			if err := write(); err != nil {
				return "", err
			}
			title = cols[0]
		}
		if merger.Name() == titleItemsPath {
			item = cols[1]
		} else {
			aliases = append(aliases, cols[1])
		}
	}
	if err := write(); err != nil {
		return "", err
	}
	if err := writer.Flush(); err != nil {
		return "", err
	}
	if err := redirectItems.Close(); err != nil {
		return "", err
	}

	sortedRedirects, err := SortLines(ctx, redirectItemsPath)
	if err != nil {
		return "", err
	}

	return sortedRedirects, nil
}
