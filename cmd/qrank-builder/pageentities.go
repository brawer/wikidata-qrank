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
	"github.com/minio/minio-go/v7"
)

// BuildPageEntities builds pageid-to-qid mappings and puts them in storage.
// If a mapping file is already stored for the last dumped version of a site,
// it is not getting re-built.
func buildPageEntities(ctx context.Context, dumps string, sites *map[string]WikiSite, s3 S3) error {
	stored, err := storedPageEntities(ctx, s3)
	if err != nil {
		return err
	}
	tasks := make(chan WikiSite, len(*sites))
	group, groupCtx := errgroup.WithContext(ctx)
	for i := 0; i < runtime.NumCPU(); i++ {
		group.Go(func() error {
			for {
				select {
				case <-groupCtx.Done():
					return groupCtx.Err()

				case t, more := <-tasks:
					if !more {
						return nil
					}
					if err := buildSitePageEntities(t, ctx, dumps, s3); err != nil {
						return err
					}
				}
			}
		})
	}

	built := make(map[string]string, len(*sites))
	for _, site := range *sites {
		ymd := site.LastDumped.Format("20060102")
		if arr, ok := stored[site.Key]; !ok || !slices.Contains(arr, ymd) {
			tasks <- site
			built[site.Key] = ymd
		}
	}
	close(tasks)

	if err := group.Wait(); err != nil {
		return err
	}

	// Clean up old files. We only touch those wikis for which we built a new file.
	for site, ymd := range built {
		versions := append(stored[site], ymd)
		sort.Strings(versions)
		pos := slices.Index(versions, ymd)
		for i := 0; i < pos-2; i += 1 {
			path := fmt.Sprintf("page_entities/%s-%s-page_entities.zst", site, versions[i])
			opts := minio.RemoveObjectOptions{}
			if err := s3.RemoveObject(ctx, "qrank", path, opts); err != nil {
				return err
			}
		}
	}

	return nil
}

// BuildSitePageEntities builds the page_entities file for one WikiSite.
func buildSitePageEntities(site WikiSite, ctx context.Context, dumps string, s3 S3) error {
	ymd := site.LastDumped.Format("20060102")
	destPath := fmt.Sprintf("page_entities/%s-%s-page_entities.zst", site.Key, ymd)
	logger.Printf("building %s", destPath)

	outFile, err := os.CreateTemp("", "*-page_entities.zst")
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
		if err := processPagePropsTable(groupCtx, dumps, &site, linesChan); err != nil {
			return err
		}
		if err := processPageTable(groupCtx, dumps, &site, linesChan); err != nil {
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
				return groupCtx.Err()
			case line, more := <-outChan:
				if !more {
					return merger.Close()
				}
				err := merger.Process(line)
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
// Called by function buildSitePageEntities().
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
		if row[nameCol] == "wikibase_item" {
			out <- fmt.Sprintf("%s,%s", row[pageCol], row[valueCol])
		}
	}
}

// This regexp does not match the page titles of Wikidata lexemes.
// For now this is intentional, but at some later time we might want
// to support lexemes, too.
// https://github.com/brawer/wikidata-qrank/issues/37
var wikidataTitleRe = regexp.MustCompile(`^Q\d+$`)

// ProcessPageTable processes a dump of the `page` table for a Wikimedia site.
// Called by function buildSitePageEntities().
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

// StoredPageEntitites returns what entity files are available in storage.
func storedPageEntities(ctx context.Context, s3 S3) (map[string][]string, error) {
	re := regexp.MustCompile(`^page_entities/([a-z0-9_\-]+)-(\d{8})-page_entities.zst$`)
	result := make(map[string][]string, 1000)
	opts := minio.ListObjectsOptions{Prefix: "page_entities/"}
	for obj := range s3.ListObjects(ctx, "qrank", opts) {
		if obj.Err != nil {
			return nil, obj.Err
		}
		if match := re.FindStringSubmatch(obj.Key); match != nil {
			arr, ok := result[match[1]]
			if !ok {
				arr = make([]string, 0, 3)
			}
			result[match[1]] = append(arr, match[2])
		}
	}
	for _, val := range result {
		sort.Strings(val)
	}
	return result, nil
}

type pageEntitiesScanner struct {
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

// NewPageEntitiesScanner returns an object similar to bufio.Scanner
// that sequentially scans pageid-to-qid mapping files for all WikiSites.
// Lines are returned in the exact same order and format as pageviews files.
func NewPageEntitiesScanner(sites *map[string]WikiSite, s3 S3) *pageEntitiesScanner {
	sorted := make([]WikiSite, 0, len(*sites))
	for _, site := range *sites {
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
		path := fmt.Sprintf("page_entities/%s-%s-page_entities.zst", site.Key, ymd)
		paths = append(paths, path)
		domains = append(domains, strings.TrimSuffix(site.Domain, ".org"))
	}

	return &pageEntitiesScanner{
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

func (s *pageEntitiesScanner) Scan() bool {
	s.curLine.Truncate(0)
	if s.err != nil {
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
				break
			}
		}
		s.curDomain += 1
		if s.curDomain == len(s.domains) {
			break
		}

		s.reader, s.err = NewS3Reader(context.Background(), "qrank", s.paths[s.curDomain], s.storage)
		if s.err != nil {
			break
		}

		if s.decompressor == nil {
			s.decompressor, s.err = zstd.NewReader(nil)
			if s.err != nil {
				break
			}
		}
		s.err = s.decompressor.Reset(s.reader)
		if s.err != nil {
			break
		}
		s.scanner = bufio.NewScanner(s.decompressor)
	}

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

func (s *pageEntitiesScanner) Bytes() []byte {
	return s.curLine.Bytes()
}

func (s *pageEntitiesScanner) Text() string {
	return s.curLine.String()
}

func (s *pageEntitiesScanner) Err() error {
	return s.err
}

// PageSignalMerger aggregates per-page signals from different sources
// into a single output line. Input and output is keyed by page id.
type pageSignalMerger struct {
	writer   io.WriteCloser
	page     string
	entity   string
	pageSize int64
}

func NewPageSignalMerger(w io.WriteCloser) *pageSignalMerger {
	return &pageSignalMerger{writer: w}
}

// Process handles one line of input.
// Input must be grouped by page (such as by sorting lines).
// Recognized line formats:
//
//	"200,Q72": wikipage #200 is for Wikidata entity Q72
//	"200,s=830167": wikipage #200 is 830167 bytes in size
func (m *pageSignalMerger) Process(line string) error {
	pos := strings.IndexByte(line, ',')
	page := line[0:pos]
	if page != m.page {
		if err := m.write(); err != nil {
			return err
		}
		m.page = page
	}

	switch line[pos+1] {
	case 'Q':
		m.entity = line[pos+1 : len(line)]
	case 's':
		if line[pos+2] == '=' {
			if n, err := strconv.ParseInt(line[pos+3:len(line)], 10, 64); err == nil {
				m.pageSize += n
			}
		}
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
		buf.WriteString(strconv.FormatInt(m.pageSize, 10))
		buf.WriteByte('\n')
		_, err = m.writer.Write(buf.Bytes())
	}

	m.page = ""
	m.entity = ""
	m.pageSize = 0

	return err
}
