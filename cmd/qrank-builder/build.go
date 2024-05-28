// SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"fmt"
	"net/http"
	"runtime"
	"slices"
	"sort"

	"github.com/minio/minio-go/v7"
	"golang.org/x/sync/errgroup"
)

// Build runs the entire QRank pipeline.
func Build(client *http.Client, dumps string, numWeeks int, s3 S3) error {
	ctx := context.Background()

	pageviews, err := buildPageviews(ctx, dumps, numWeeks, s3)
	if err != nil {
		return err
	}

	sites, err := ReadWikiSites(client, dumps)
	if err != nil {
		return err
	}
	logger.Printf("found wikimedia dumps for %d sites", len(sites.Sites))

	if err := buildSiteFiles(ctx, "page_signals", buildPageSignals, dumps, sites, s3); err != nil {
		return err
	}

	if err := buildSiteFiles(ctx, "interwiki_links", buildInterwikiLinks, dumps, sites, s3); err != nil {
		return err
	}

	if err := buildSiteFiles(ctx, "titles", buildTitles, dumps, sites, s3); err != nil {
		return err
	}

	_, err = buildItemSignals(ctx, pageviews, sites, s3)
	if err != nil {
		return err
	}

	return nil
}

type SiteFileBuilder func(site *WikiSite, ctx context.Context, dumps string, s3 S3) error

func buildSiteFiles(ctx context.Context, filename string, builder SiteFileBuilder, dumps string, sites *WikiSites, s3 S3) error {
	stored, err := ListStoredFiles(ctx, filename, s3)
	if err != nil {
		return err
	}
	tasks := make(chan WikiSite, len(sites.Sites))
	group, groupCtx := errgroup.WithContext(ctx)
	for i := 0; i < runtime.NumCPU(); i++ {
		group.Go(func() error {
			for {
				select {
				case <-groupCtx.Done():
					logger.Printf("BuildSiteFile(): canceled, filename=%s, groupCtx.Err()=%v", filename, groupCtx.Err())
					return groupCtx.Err()

				case t, more := <-tasks:
					if !more {
						return nil
					}
					if err := builder(&t, ctx, dumps, s3); err != nil {
						return err
					}
				}
			}
		})
	}

	built := make(map[string]string, len(sites.Sites))
	for _, site := range sites.Sites {
		ymd := site.LastDumped.Format("20060102")
		if arr, ok := stored[site.Key]; !ok || !slices.Contains(arr, ymd) {
			tasks <- *site
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
			path := fmt.Sprintf("%s/%s-%s-%s.zst", filename, site, versions[i], filename)
			opts := minio.RemoveObjectOptions{}
			if err := s3.RemoveObject(ctx, "qrank", path, opts); err != nil {
				return err
			}
		}
	}

	return nil
}
