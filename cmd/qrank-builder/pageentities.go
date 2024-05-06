// SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"sort"

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
	tasks := make(chan WikiSite, 1000)
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

	for _, site := range *sites {
		ymd := site.LastDumped.Format("20060102")
		if arr, ok := stored[ymd]; !ok || !slices.Contains(arr, ymd) {
			tasks <- site
		}
	}
	close(tasks)

	if err := group.Wait(); err != nil {
		return err
	}
	return nil
}

func buildSitePageEntities(site WikiSite, ctx context.Context, dumps string, s3 S3) error {
	ymd := site.LastDumped.Format("20060102")
	destPath := fmt.Sprintf("page_entities/%s-%s-page_entities.zst", site.Key, ymd)
	logger.Printf("building %s", destPath)

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
		for {
			select {
			case <-groupCtx.Done():
				return groupCtx.Err()
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
				linesChan <- fmt.Sprintf("%s,%s", row[pageCol], row[valueCol])
			}
		}
	})
	group.Go(func() error {
		sorter.Sort(groupCtx)
		for {
			select {
			case <-groupCtx.Done():
				return groupCtx.Err()
			case line, more := <-outChan:
				if !more {
					return nil
				}
				var buf bytes.Buffer
				if _, err := buf.WriteString(line); err != nil {
					return err
				}
				if err := buf.WriteByte('\n'); err != nil {
					return err
				}
				if _, err := buf.WriteTo(writer); err != nil {
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

	if err := writer.Close(); err != nil {
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

// StoredPageEntitites returns what entity files are available in storage.
func storedPageEntities(ctx context.Context, s3 S3) (map[string][]string, error) {
	re := regexp.MustCompile(`^page_entities/([a-z0-9_\-]+)-(\d{8})-page_entities.zst$`)
	result := make(map[string][]string, 1000)
	opts := minio.ListObjectsOptions{Prefix: "pageviews/"}
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
