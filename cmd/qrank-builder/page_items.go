// SPDX-FileCopyrightText: 2024 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"fmt"
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

type PageItem struct {
	Page uint64
	Item Item
}

func (pi PageItem) ToBytes() []byte {
	buf := make([]byte, binary.MaxVarintLen64*2)
	p := binary.PutUvarint(buf, pi.Page)
	p += binary.PutUvarint(buf[p:], uint64(pi.Item))
	return buf[0:p]
}

func PageItemFromBytes(b []byte) extsort.SortType {
	page, pos := binary.Uvarint(b)
	item, _ := binary.Uvarint(b[pos:])
	return PageItem{Page: page, Item: Item(item)}
}

func PageItemLess(a, b extsort.SortType) bool {
	aa, bb := a.(PageItem), b.(PageItem)
	if aa.Page < bb.Page {
		return true
	} else if aa.Page > bb.Page {
		return false
	}

	if aa.Item < bb.Item {
		return true
	} else if aa.Item > bb.Item {
		return false
	}

	return false
}

// ReadPageItems streams the contents of a page_items file to a channel.
// The streamed PageItems are sorted by page id.
func ReadPageItems(ctx context.Context, path string, out chan<- PageItem) error {
	defer close(out)
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	decompressor, err := zstd.NewReader(file)
	if err != nil {
		return err
	}

	scanner := bufio.NewScanner(decompressor)
	for scanner.Scan() {
		cols := strings.Split(scanner.Text(), "\t")
		if len(cols) != 2 {
			return fmt.Errorf("%s has unexpected format", path)
		}
		page, err := strconv.ParseUint(cols[0], 10, 64)
		if page <= 0 || err != nil {
			return fmt.Errorf("%s has bad page: %q", path, cols[0])
		}
		item := ParseItem(cols[1])
		if item == NoItem {
			return fmt.Errorf("%s has bad item: %q", path, cols[1])
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- PageItem{Page: page, Item: item}:
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}

	return nil
}

func buildPageItems(ctx context.Context, site *WikiSite, dumps string) (string, error) {
	file, err := os.CreateTemp("", "pageitems-*.zst")
	if err != nil {
		return "", err
	}
	defer file.Close()

	// Somewhat surprisingly, the Wikimedia dump of the page_props
	// table is mostly sorted by page id, but there's occasional
	// exceptions.  For example, the file dewiki-20240601-page_props.sql.gz
	// contains entries in non-sorted order.  Therefore, we need to re-sort
	// the page_items ourselves.
	items := make(chan extsort.SortType, 10000)
	config := extsort.DefaultConfig()
	config.ChunkSize = 8 * 1024 * 1024
	config.NumWorkers = runtime.NumCPU()
	sorter, sortedChan, errChan := extsort.New(items, PageItemFromBytes, PageItemLess, config)

	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error {
		defer close(items)
		if err := readPageItemsFromPageProps(groupCtx, site, dumps, items); err != nil {
			return err
		}
		if err := readPageItemsFromPage(groupCtx, site, dumps, items); err != nil {
			return err
		}
		return nil
	})
	group.Go(func() error {
		sorter.Sort(groupCtx)
		zstdLevel := zstd.WithEncoderLevel(zstd.SpeedFastest)
		compressor, err := zstd.NewWriter(file, zstdLevel)
		if err != nil {
			return err
		}
		for s := range sortedChan {
			pi := s.(PageItem)
			var buf bytes.Buffer
			buf.WriteString(strconv.FormatUint(pi.Page, 10))
			buf.WriteByte('\t')
			buf.WriteString(pi.Item.String())
			buf.WriteByte('\n')
			if _, err := compressor.Write(buf.Bytes()); err != nil {
				return err
			}
		}
		if err := compressor.Close(); err != nil {
			return err
		}
		return nil
	})
	if err := group.Wait(); err != nil {
		os.Remove(file.Name())
		return "", err
	}
	if err := <-errChan; err != nil {
		return "", err
	}

	return file.Name(), nil
}

// ReadPageItemsFromPageProps reads a stream of PageItems (which page
// corresponds to what Wikidata item) from a site’s `page_props` table.
// The results are streamed in order of increasing page ID.
func readPageItemsFromPageProps(ctx context.Context, site *WikiSite, dumps string, out chan<- extsort.SortType) error {
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
		row, err := reader.Read()
		if err != nil {
			return err
		}
		if row == nil {
			return nil
		}

		page, err := strconv.ParseUint(row[pageCol], 10, 64)
		if err != nil || page == 0 {
			continue
		}

		value := row[valueCol]
		if row[nameCol] == "wikibase_item" {
			item := ParseItem(value)
			if item != NoItem {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case out <- PageItem{Page: page, Item: item}:
				}
			}
		}
	}
}

// ReadPageItemsFromPageProps reads a stream of PageItems (which page
// corresponds to what Wikidata item) from a site’s `page` table.
// The results are streamed in order of increasing page ID.
func readPageItemsFromPage(ctx context.Context, site *WikiSite, dumps string, out chan<- extsort.SortType) error {
	// Other than other wiki projects, wikidatawiki.page_props only contains
	// Wikidata IDs for internal maintenance pages such as templates. To find
	// the mapping from page-id to wikidata-id for the actually interesting
	// entities, we need to look at page titles. But for other wikis, this
	// is not needed so we return early.
	//
	// https://github.com/brawer/wikidata-qrank/issues/35
	if site.Key != "wikidatawiki" {
		return nil
	}

	ymd := site.LastDumped.Format("20060102")
	fileName := fmt.Sprintf("%s-%s-page.sql.gz", site.Key, ymd)
	filePath := filepath.Join(dumps, site.Key, ymd, fileName)
	file, err := os.Open(filePath)
	if err != nil {
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

		page, err := strconv.ParseUint(row[pageCol], 10, 64)
		if err != nil || page == 0 {
			continue
		}

		if row[namespaceCol] != "0" {
			continue
		}

		if item := ParseItem(row[titleCol]); item != NoItem {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case out <- PageItem{Page: page, Item: item}:
			}
		}
	}
}
