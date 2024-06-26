// SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
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
	"regexp"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"

	"golang.org/x/sync/errgroup"

	"github.com/klauspost/compress/zstd"
	"github.com/lanrat/extsort"
)

// BuildPageSignals builds the page_signals file for a WikiSite and puts it in S3 storage.
func buildPageSignals(site *WikiSite, ctx context.Context, dumps string, s3 S3) error {
	destPath := site.S3Path("page_signals")
	logger.Printf("building %s", destPath)

	outFile, err := os.CreateTemp("", "*-page_signals.zst")
	if err != nil {
		return err
	}
	zstdLevel := zstd.WithEncoderLevel(zstd.SpeedBestCompression)
	writer, err := zstd.NewWriter(outFile, zstdLevel)
	if err != nil {
		return err
	}

	linesChan := make(chan string, 10000)
	config := extsort.DefaultConfig()
	config.ChunkSize = 8 * 1024 * 1024 / 64 // 8 MiB, 64 Bytes/line avg
	config.NumWorkers = runtime.NumCPU()
	sorter, outChan, errChan := extsort.Strings(linesChan, config)

	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error {
		defer close(linesChan)
		if err := processPagePropsTable(groupCtx, dumps, site, linesChan); err != nil {
			return err
		}
		if err := processPageTable(groupCtx, dumps, site, linesChan); err != nil {
			return err
		}
		return nil
	})
	group.Go(func() error {
		sorter.Sort(groupCtx)
		merger := NewPageSignalMerger(writer)
		for {
			select {
			case <-groupCtx.Done():
				logger.Printf("BuildSitePageSignals(): canceled, groupCtx.Err()=%v", groupCtx.Err())
				return groupCtx.Err()

			case line, more := <-outChan:
				if !more {
					return merger.Close()
				}
				err := merger.Process(line)
				if err != nil {
					logger.Printf(`BuildSitePageSignals(): merger.Process("%s") failed, err=%v`, line, err)
					return err
				}
			}
		}
	})
	if err := group.Wait(); err != nil {
		logger.Printf(`BuildSitePageSignals(): group.Wait() failed, err=%v`, err)
		return err
	}
	if err := <-errChan; err != nil {
		return err
	}
	if err := outFile.Close(); err != nil {
		return err
	}

	if err := PutInStorage(ctx, outFile.Name(), s3, "qrank", destPath, "application/zstd"); err != nil {
		return err
	}

	if err := os.Remove(outFile.Name()); err != nil {
		return err
	}

	return nil
}

// ProcessPagePropsTable processes a dump of the `page_props` table for a Wikimedia site.
// Called by function buildSitePageSignals().
func processPagePropsTable(ctx context.Context, dumps string, site *WikiSite, out chan<- string) error {
	ymd := site.LastDumped.Format("20060102")
	propsFileName := fmt.Sprintf("%s-%s-page_props.sql.gz", site.Key, ymd)
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
	pageCol := slices.Index(columns, "pp_page")
	nameCol := slices.Index(columns, "pp_propname")
	valueCol := slices.Index(columns, "pp_value")

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
		value := row[valueCol]
		switch row[nameCol] {
		case "wikibase_item":
			out <- fmt.Sprintf("%s,%s", page, value)
		case "wb-claims":
			out <- fmt.Sprintf("%s,c=%s", page, value)
		case "wb-identifiers":
			out <- fmt.Sprintf("%s,i=%s", page, value)
		case "wb-sitelinks":
			out <- fmt.Sprintf("%s,l=%s", page, value)
		}
	}
}

// This regexp does not match the page titles of Wikidata lexemes.
// For now this is intentional, but at some later time we might want
// to support lexemes, too.
// https://github.com/brawer/wikidata-qrank/issues/37
var wikidataTitleRe = regexp.MustCompile(`^Q\d+$`)

// ProcessPageTable processes a dump of the `page` table for a Wikimedia site.
// Called by function buildSitePageSignals().
func processPageTable(ctx context.Context, dumps string, site *WikiSite, out chan<- string) error {
	isWikidata := site.Key == "wikidatawiki"
	ymd := site.LastDumped.Format("20060102")
	propsFileName := fmt.Sprintf("%s-%s-page.sql.gz", site.Key, ymd)
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
	pageCol := slices.Index(columns, "page_id")
	namespaceCol := slices.Index(columns, "page_namespace")
	titleCol := slices.Index(columns, "page_title")
	contentModelCol := slices.Index(columns, "page_content_model")
	lenCol := slices.Index(columns, "page_len")

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

		// Other than other wiki projects, wikidatawiki.page_props only contains
		// Wikidata IDs for internal maintenance pages such as templates. To find
		// the mapping from page-id to wikidata-id for the actually interesting
		// entities, we need to look at page titles.
		// https://github.com/brawer/wikidata-qrank/issues/35
		if isWikidata && row[namespaceCol] == "0" {
			title := row[titleCol]
			if wikidataTitleRe.MatchString(title) {
				out <- fmt.Sprintf("%s,%s", row[pageCol], title)
			}
		}

		// Collect page sizes.
		// https://github.com/brawer/wikidata-qrank/issues/38
		if row[contentModelCol] == "wikitext" {
			out <- fmt.Sprintf("%s,s=%s", row[pageCol], row[lenCol])
		}
	}
}

type pageSignalsScanner struct {
	err          error
	paths        []string
	domains      []string
	curDomain    int
	storage      S3
	reader       io.ReadCloser
	decompressor *zstd.Decoder
	scanner      *bufio.Scanner
	curLine      bytes.Buffer
}

// NewPageSignalsScanner returns an object similar to bufio.Scanner
// that sequentially scans pageid-to-qid mapping files for all WikiSites.
// Lines are returned in the exact same order and format as pageviews files.
func NewPageSignalsScanner(sites *WikiSites, s3 S3) *pageSignalsScanner {
	sorted := make([]*WikiSite, 0, len(sites.Sites))
	for _, site := range sites.Sites {
		sorted = append(sorted, site)
	}
	sort.Slice(sorted, func(i, j int) bool {
		a := strings.TrimSuffix(sorted[i].Domain, ".org")
		b := strings.TrimSuffix(sorted[j].Domain, ".org")
		return a < b
	})
	paths := make([]string, 0, len(sorted))
	domains := make([]string, 0, len(sorted))
	for _, site := range sorted {
		ymd := site.LastDumped.Format("20060102")
		path := fmt.Sprintf("page_signals/%s-%s-page_signals.zst", site.Key, ymd)
		paths = append(paths, path)
		domains = append(domains, strings.TrimSuffix(site.Domain, ".org"))
	}

	return &pageSignalsScanner{
		err:          nil,
		paths:        paths,
		domains:      domains,
		curDomain:    -1,
		storage:      s3,
		reader:       nil,
		decompressor: nil,
		scanner:      nil,
	}
}

func (s *pageSignalsScanner) Scan() bool {
	s.curLine.Truncate(0)
	if s.err != nil {
		logger.Printf("PageSignalsScanner.Scan(): early exit due to err=%v", s.err)
		return false
	}
	for s.curDomain < len(s.domains) {
		if s.scanner != nil {
			if s.scanner.Scan() {
				s.curLine.WriteString(s.domains[s.curDomain])
				s.curLine.WriteByte(',')
				s.curLine.Write(s.scanner.Bytes())
				return true
			}
			s.err = s.scanner.Err()
			if s.err != nil {
				logger.Printf("PageSignalsScanner.Scan(): failed, domain=%s, err=%v", s.domains[s.curDomain], s.err)
				break
			}
		}
		s.curDomain += 1
		if s.curDomain == len(s.domains) {
			logger.Println("PageSignalsScanner.Scan(): finished last domain")
			break
		}

		path := s.paths[s.curDomain]
		s.reader, s.err = NewS3Reader(context.Background(), "qrank", path, s.storage)
		if s.err != nil {
			logger.Printf(`PageSignalsScanner.Scan(): cannot open s3://qrank/%s, err=%v`, path, s.err)
			break
		}

		if s.decompressor == nil {
			s.decompressor, s.err = zstd.NewReader(nil)
			if s.err != nil {
				logger.Printf(`failed to create zstd decompressor, err=%v`, s.err)
				break
			}
		}
		s.err = s.decompressor.Reset(s.reader)
		if s.err != nil {
			logger.Printf(`failed to reset zstd decompressor, err=%v`, s.err)
			break
		}
		s.scanner = bufio.NewScanner(s.decompressor)
	}

	logger.Printf("PageSignalsScanner.Scan(): cleaning up")

	if s.decompressor != nil {
		s.decompressor.Close()
		s.decompressor = nil
	}

	if s.reader != nil {
		s.reader.Close()
		s.reader = nil
	}

	s.scanner = nil
	return false
}

func (s *pageSignalsScanner) Bytes() []byte {
	return s.curLine.Bytes()
}

func (s *pageSignalsScanner) Text() string {
	return s.curLine.String()
}

func (s *pageSignalsScanner) Err() error {
	return s.err
}

// PageSignalMerger aggregates per-page signals from different sources
// into a single output line. Input and output is keyed by page id.
type pageSignalMerger struct {
	writer         io.WriteCloser
	page           string
	entity         string
	pageSize       int64
	numClaims      int64
	numIdentifiers int64
	numSiteLinks   int64

	// Stats for logging.
	inputRecords  int64
	outputRecords int64
}

func NewPageSignalMerger(w io.WriteCloser) *pageSignalMerger {
	return &pageSignalMerger{writer: w}
}

// Process handles one line of input.
// Input must be grouped by page (such as by sorting lines).
// Recognized line formats:
//
//	  "200,Q72": wikipage 200 is for Wikidata entity Q72
//		 "200,c=8": wikipage 200 has 8 claims in wikidatawiki
//		 "200,i=17": wikipage 200 has 17 identifiers in wikidatawiki
//		 "200,l=23": wikipage 200 has 23 sitelinks in wikidatawiki
//	  "200,s=830167": wikipage 200 has 830167 bytes in wikitext format
func (m *pageSignalMerger) Process(line string) error {
	m.inputRecords += 1
	pos := strings.IndexByte(line, ',')
	page := line[0:pos]
	if page != m.page {
		if err := m.write(); err != nil {
			return err
		}
		m.page = page
	}

	var value int64 = 0
	if line[pos+2] == '=' {
		n, err := strconv.ParseInt(line[pos+3:len(line)], 10, 64)
		if err != nil {
			return err
		}
		value = n
	}

	switch line[pos+1] {
	case 'Q':
		m.entity = line[pos+1 : len(line)]
	case 'c':
		m.numClaims += value
	case 'i':
		m.numIdentifiers += value
	case 'l':
		m.numSiteLinks += value
	case 's':
		m.pageSize += value
	}

	return nil
}

func (m *pageSignalMerger) Close() error {
	if err := m.write(); err != nil {
		return err
	}

	if err := m.writer.Close(); err != nil {
		return err
	}

	logger.Printf("PageSignalMerger: processed %d → %d records", m.inputRecords, m.outputRecords)
	return nil
}

func (m *pageSignalMerger) write() error {
	var err error
	if m.page != "" && m.entity != "" {
		var buf bytes.Buffer
		buf.WriteString(m.page)
		buf.WriteByte(',')
		buf.WriteString(m.entity)
		buf.WriteByte(',')
		if m.pageSize > 0 {
			buf.WriteString(strconv.FormatInt(m.pageSize, 10))
		}
		if m.numClaims > 0 || m.numIdentifiers > 0 || m.numSiteLinks > 0 {
			buf.WriteByte(',')
			if m.numClaims > 0 {
				buf.WriteString(strconv.FormatInt(m.numClaims, 10))
			}
			buf.WriteByte(',')
			if m.numIdentifiers > 0 {
				buf.WriteString(strconv.FormatInt(m.numIdentifiers, 10))
			}
			buf.WriteByte(',')
			if m.numSiteLinks > 0 {
				buf.WriteString(strconv.FormatInt(m.numSiteLinks, 10))
			}
		}
		buf.WriteByte('\n')
		_, err = m.writer.Write(buf.Bytes())
		m.outputRecords += 1
	}

	m.page = ""
	m.entity = ""
	m.numClaims = 0
	m.numIdentifiers = 0
	m.numSiteLinks = 0
	m.pageSize = 0

	return err
}

// ReadPageItemsOld reads our page_signals file and emits lines of the form
// `<PageID>,<property>,<WikidataItemID>` to an output channel.
// TODO: Remove this method after refactoring clients to call ReadPageItems().
func ReadPageItemsOld(ctx context.Context, site *WikiSite, property string, s3 S3, out chan<- string) error {
	ymd := site.LastDumped.Format("20060102")
	path := fmt.Sprintf("page_signals/%s-%s-page_signals.zst", site.Key, ymd)
	reader, err := NewS3Reader(ctx, "qrank", path, s3)
	if err != nil {
		return err
	}
	defer reader.Close()

	decompressor, err := zstd.NewReader(reader)
	if err != nil {
		return err
	}
	defer decompressor.Close()

	scanner := bufio.NewScanner(decompressor)
	for scanner.Scan() {
		cols := strings.Split(scanner.Text(), ",")
		if len(cols) >= 2 {
			var buf bytes.Buffer
			buf.WriteString(cols[0])
			buf.WriteByte('\t')
			buf.WriteString(property)
			buf.WriteByte('\t')
			buf.WriteString(cols[1])
			out <- buf.String()
		}
	}

	decompressor.Close()
	if err := reader.Close(); err != nil {
		return err
	}

	return nil
}
