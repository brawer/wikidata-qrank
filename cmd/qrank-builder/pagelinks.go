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

// BuildLinks builds the `links` file for a WikiSite and puts it in S3 storage.
// This includes any links between items of the same wiki. Interwiki links
// are handled elsewhere, see BuildInterwikiLinks().
func buildLinks(site *WikiSite, ctx context.Context, dumps string, s3 S3) error {
	destPath := site.S3Path("links")
	logger.Printf("building %s", destPath)

	unsorted, err := os.CreateTemp("", "*-links.zst")
	if err != nil {
		return err
	}
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
		if err := readPageLinks(groupCtx, site, "B", dumps, linesChan); err != nil {
			return err
		}
		return nil
	})
	group.Go(func() error {
		sorter.Sort(groupCtx)
		joiner := NewPagelinksJoiner(site, unsorted)
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

	sorted, err := SortLines(ctx, unsorted.Name())
	if err != nil {
		return err
	}
	defer os.Remove(sorted)

	links, err := joinPagelinksByTitle(ctx, site, sorted, s3)
	if err != nil {
		return err
	}
	defer os.Remove(links)

	if err := PutInStorage(ctx, links, s3, "qrank", destPath, "application/zstd"); err != nil {
		return err
	}

	return nil
}

func readPageLinks(ctx context.Context, site *WikiSite, property string, dumps string, out chan<- string) error {
	ymd := site.LastDumped.Format("20060102")
	pageLinksFileName := fmt.Sprintf("%s-%s-pagelinks.sql.gz", site.Key, ymd)
	pageLinksPath := filepath.Join(dumps, site.Key, ymd, pageLinksFileName)
	pageLinksFile, err := os.Open(pageLinksPath)
	if err != nil {
		return err
	}
	defer pageLinksFile.Close()

	gz, err := gzip.NewReader(pageLinksFile)
	if err != nil {
		return err
	}
	defer gz.Close()

	reader, err := NewSQLReader(gz)
	if err != nil {
		return err
	}

	columns := reader.Columns()
	fromPageCol := slices.Index(columns, "pl_from")
	namespaceCol := slices.Index(columns, "pl_namespace")
	titleCol := slices.Index(columns, "pl_title")

	if namespaceCol < 0 || titleCol < 0 {
		return joinLinkTargets(ctx, site, property, dumps, out)
	}

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

		fromPage := row[fromPageCol]
		title := row[titleCol]
		namespace := row[namespaceCol]

		var nsPrefix string
		if namespace != "0" {
			if ns, found := site.Namespaces[namespace]; found && ns.Localized != "" {
				nsPrefix = ns.Localized + ":"
			}
		}

		out <- fmt.Sprintf("%s\t%s\t%s%s", fromPage, property, nsPrefix, title)
	}
}

type pagelinksJoiner struct {
	site     *WikiSite
	writer   io.WriteCloser
	page     int64
	title    string
	item     string
	inLines  int64
	outLines int64
}

func NewPagelinksJoiner(site *WikiSite, w io.WriteCloser) *pagelinksJoiner {
	return &pagelinksJoiner{site: site, writer: w}
}

func (j *pagelinksJoiner) Process(line string) error {
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

func (j *pagelinksJoiner) Close() error {
	return j.writer.Close()
}

func joinPagelinksByTitle(ctx context.Context, site *WikiSite, pagelinks string, s3 S3) (string, error) {
	scanners := make([]LineScanner, 0, 3)
	scannerNames := make([]string, 0, 3)
	pagelinksFile, err := os.Open(pagelinks)
	if err != nil {
		return "", err
	}
	defer pagelinksFile.Close()
	pagelinksDecompressor, err := zstd.NewReader(pagelinksFile)
	if err != nil {
		return "", err
	}
	scanners = append(scanners, bufio.NewScanner(pagelinksDecompressor))
	scannerNames = append(scannerNames, "pagelinks")
	for _, filename := range []string{"titles", "redirects"} {
		s3Path := site.S3Path(filename)
		reader, err := NewS3Reader(ctx, "qrank", s3Path, s3)
		if err != nil {
			logger.Printf("cannot read %s, err=%v", s3Path, err)
			return "", err
		}
		decompressor, err := zstd.NewReader(reader)
		if err != nil {
			return "", err
		}
		scanners = append(scanners, bufio.NewScanner(decompressor))
		scannerNames = append(scannerNames, filename)
	}

	dest, err := os.CreateTemp("", "links*")
	if err != nil {
		return "", err
	}
	defer dest.Close()

	zstdLevel := zstd.WithEncoderLevel(zstd.SpeedBestCompression)
	compressor, err := zstd.NewWriter(dest, zstdLevel)
	if err != nil {
		return "", err
	}
	defer compressor.Close()
	writer := NewLinkWriter(compressor)

	ch := make(chan extsort.SortType, 50000)
	group, groupCtx := errgroup.WithContext(ctx)
	config := extsort.DefaultConfig()
	config.NumWorkers = runtime.NumCPU()
	sorter, outChan, errChan := extsort.New(ch, LinkFromBytes, LinkLess, config)
	group.Go(func() error {
		defer close(ch)
		merger := NewLineMerger(scanners, scannerNames)
		ptj := pagelinksTitleJoiner{out: ch}
		for merger.Advance() {
			cols := strings.Split(merger.Line(), "\t")
			if merger.Name() == "pagelinks" {
				if err := ptj.ProcessSource(cols[0], cols[1]); err != nil {
					return err
				}
			} else {
				if err := ptj.ProcessTarget(cols[0], cols[1]); err != nil {
					return err
				}
			}
		}
		if err := ptj.Flush(); err != nil {
			return err
		}
		return nil
	})
	group.Go(func() error {
		sorter.Sort(ctx) // not groupCtx, as per extsort docs
		for {
			select {
			case <-groupCtx.Done():
				return groupCtx.Err()
			case linkItem, more := <-outChan:
				if !more {
					return writer.Flush()
				}
				link := linkItem.(Link)
				if err := writer.Write(link); err != nil {
					return err
				}
			}
		}
	})
	if err := group.Wait(); err != nil {
		os.Remove(dest.Name())
		return "", err
	}
	if err := <-errChan; err != nil {
		os.Remove(dest.Name())
		return "", err
	}

	if err := compressor.Close(); err != nil {
		os.Remove(dest.Name())
		return "", err
	}

	return dest.Name(), nil
}

type pagelinksTitleJoiner struct {
	out    chan<- extsort.SortType
	title  string
	source int64
	target int64
}

func (p *pagelinksTitleJoiner) ProcessSource(title string, source string) error {
	if p.title != title {
		if err := p.Flush(); err != nil {
			return err
		}
	}
	if len(source) < 2 || source[0] != 'Q' {
		return nil
	}

	i, err := strconv.ParseInt(source[1:len(source)], 10, 64)
	if err != nil {
		return nil
	}

	p.title = title
	p.source = i
	return nil
}

func (p *pagelinksTitleJoiner) ProcessTarget(title string, target string) error {
	if p.title != title {
		if err := p.Flush(); err != nil {
			return err
		}
	}
	if len(target) < 2 || target[0] != 'Q' {
		return nil
	}

	i, err := strconv.ParseInt(target[1:len(target)], 10, 64)
	if err != nil {
		return nil
	}

	p.title = title
	p.target = i
	return nil
}

func (p *pagelinksTitleJoiner) Flush() error {
	if len(p.title) > 0 && p.source != 0 && p.target != 0 && p.source != p.target {
		p.out <- Link{Source: p.source, Target: p.target}
	}
	p.title = ""
	p.source = 0
	p.target = 0
	return nil
}
