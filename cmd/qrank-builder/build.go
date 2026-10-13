// SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
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

	// buildTitles also writes redirects, which buildSiteFiles does not clean up.
	if err := PruneStoredFiles(ctx, "redirects", 3, s3); err != nil {
		return err
	}

	if err := buildSiteFiles(ctx, "page_items", buildSite, dumps, sites, s3); err != nil {
		return err
	}

	signalsDate, err := buildItemSignals(ctx, pageviews, sites, s3)
	if err != nil {
		return err
	}

	if err := buildQRankFiles(ctx, signalsDate, s3); err != nil {
		return err
	}

	return nil
}

// StampedSiteFiles are the kinds of site-specific files that must record
// which software produced them, because they feed into the item signals,
// whose provenance gets published in the CycloneDX bill of materials.
var stampedSiteFiles = map[string]bool{"page_signals": true}

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

	// Files that feed into the item signals must record which software
	// produced them. Re-build any that were stored by older releases.
	unstamped := make(map[string]bool)
	if stampedSiteFiles[filename] {
		paths := make([]string, 0, len(sites.Sites))
		for _, site := range sites.Sites {
			ymd := site.LastDumped.Format("20060102")
			if slices.Contains(stored[site.Key], ymd) {
				paths = append(paths, site.S3Path(filename))
			}
		}
		unstamped, err = findUnstampedFiles(ctx, paths, s3)
		if err != nil {
			return err
		}
		if len(unstamped) > 0 {
			logger.Printf("re-building %d %s files without software metadata", len(unstamped), filename)
		}
	}

	built := make(map[string]string, len(sites.Sites))
	for _, site := range sites.Sites {
		ymd := site.LastDumped.Format("20060102")
		if arr, ok := stored[site.Key]; !ok || !slices.Contains(arr, ymd) || unstamped[site.S3Path(filename)] {
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

func buildSite(site *WikiSite, ctx context.Context, dumps string, s3 S3) error {
	dest := site.S3Path("page_items") // TODO: change to "links" once implemented
	logger.Printf("building %s", dest)

	pageItems, err := buildPageItems(ctx, site, dumps)
	if err != nil {
		return err
	}
	defer os.Remove(pageItems)

	// TODO: Move generation of titles and redirects to here.
	// Note that the sort order has changed; the ordering by page ID
	// will allow us to replace the sort by a (much faster) merge.

	// TODO: Ultimately, we want the resolved links, not any intermediate files.
	if err := PutInStorage(ctx, pageItems, s3, "qrank", dest, "application/zstd"); err != nil {
		return err
	}

	return nil
}
