// SPDX-FileCopyrightText: 2026 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"

	"github.com/klauspost/compress/zstd"
	"github.com/lanrat/extsort"
	"golang.org/x/sync/errgroup"
)

// The item_coordinates file lists the geographic coordinates of Wikidata
// items, as lines of the form "item,lat,lon" such as "72,47.37,8.54".
// It is built once per Wikidata dump from the `geo_tags` table, which
// the GeoData extension fills with the P625 coordinates of every item,
// and stored as item_coordinates/wikidatawiki-YYYYMMDD-item_coordinates.zst.
// Items with several coordinates have several lines. Only coordinates
// on Earth are included.
//
// The lines are sorted in spatial order, by the zoom-10 Web Mercator tile
// of the coordinate, so that looking them up in the OSMViews GeoTIFF
// touches each raster tile only once. Looking up the same coordinates
// in item order would be about 50 times slower, because the lookups
// would keep evicting raster tiles from the cache.

// CoordinatesTileZoom is the zoom level of the Web Mercator tiles
// whose order the item_coordinates file follows. Tiles at any finer
// zoom nest inside them, so the order is spatially coherent for any
// raster resolution.
const coordinatesTileZoom = 10

// MaxMercatorLatitude is the latitude beyond which Web Mercator,
// and hence OSMViews, has no data.
const maxMercatorLatitude = 85.05112877980659

// CoordinatesTileKey returns a sort key that orders coordinates by their
// Web Mercator tile at coordinatesTileZoom, row by row. Coordinates
// outside the Web Mercator range get sorted last.
func coordinatesTileKey(lat, lon float64) string {
	if math.IsNaN(lat) || math.IsNaN(lon) || math.Abs(lat) >= maxMercatorLatitude {
		return "9999999"
	}
	n := float64(int(1) << coordinatesTileZoom)
	x := math.Mod(math.Mod(lon+180, 360)+360, 360) / 360 * n
	y := (1 - math.Asinh(math.Tan(lat*math.Pi/180))/math.Pi) / 2 * n
	tx := int(math.Min(math.Max(math.Floor(x), 0), n-1))
	ty := int(math.Min(math.Max(math.Floor(y), 0), n-1))
	return fmt.Sprintf("%07d", ty<<coordinatesTileZoom|tx)
}

// BuildItemCoordinates builds the item_coordinates file for the most
// recent Wikidata dump, unless it is already in storage. It must run
// after the page signals, because it takes the mapping from Wikidata
// page IDs to item IDs from the page_signals file of wikidatawiki.
func buildItemCoordinates(ctx context.Context, dumps string, sites *WikiSites, s3 S3) error {
	site, ok := sites.Sites["wikidatawiki"]
	if !ok {
		logger.Printf("no Wikidata dump, so no item coordinates")
		return nil
	}

	stored, err := ListStoredFiles(ctx, "item_coordinates", s3)
	if err != nil {
		return err
	}
	ymd := site.LastDumped.Format("20060102")
	if !slices.Contains(stored["wikidatawiki"], ymd) {
		if err := buildSiteItemCoordinates(ctx, dumps, site, s3); err != nil {
			return err
		}
	}

	return PruneStoredFiles(ctx, "item_coordinates", 3, s3)
}

// StoredItemCoordinates returns the storage path of the item_coordinates
// file for the current Wikidata dump, or "" if there is none.
func storedItemCoordinates(ctx context.Context, sites *WikiSites, s3 S3) (string, error) {
	site, ok := sites.Sites["wikidatawiki"]
	if !ok {
		return "", nil
	}
	stored, err := ListStoredFiles(ctx, "item_coordinates", s3)
	if err != nil {
		return "", err
	}
	if !slices.Contains(stored["wikidatawiki"], site.LastDumped.Format("20060102")) {
		return "", nil
	}
	return site.S3Path("item_coordinates"), nil
}

func buildSiteItemCoordinates(ctx context.Context, dumps string, site *WikiSite, s3 S3) error {
	destPath := site.S3Path("item_coordinates")
	logger.Printf("building %s", destPath)

	outFile, err := os.CreateTemp("", "*-item_coordinates.zst")
	if err != nil {
		return err
	}
	defer os.Remove(outFile.Name())
	defer outFile.Close()
	writer, err := zstd.NewWriter(outFile, zstd.WithEncoderLevel(zstd.SpeedBetterCompression))
	if err != nil {
		return err
	}

	config := extsort.DefaultConfig()
	config.ChunkSize = 8 * 1024 * 1024 / 32 // 8 MiB, 32 Bytes/line avg
	config.NumWorkers = runtime.NumCPU()

	// Sort the coordinates by page ID, in the same string order
	// as the page_signals file, so that we can join them.
	geoChan := make(chan string, 10000)
	geoSorter, geoOut, geoErr := extsort.Strings(geoChan, config)

	// Sort the joined coordinates by tile.
	tileChan := make(chan string, 10000)
	tileSorter, tileOut, tileErr := extsort.Strings(tileChan, config)

	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error {
		defer close(geoChan)
		return processGeoTagsTable(groupCtx, dumps, site, geoChan)
	})
	group.Go(func() error {
		defer close(tileChan)
		geoSorter.Sort(groupCtx)
		reader, err := NewS3Reader(groupCtx, "qrank", site.S3Path("page_signals"), s3)
		if err != nil {
			return err
		}
		defer reader.Close()
		decompressor, err := zstd.NewReader(reader)
		if err != nil {
			return err
		}
		defer decompressor.Close()
		return joinCoordinates(groupCtx, bufio.NewScanner(decompressor), geoOut, tileChan)
	})
	group.Go(func() error {
		tileSorter.Sort(groupCtx)
		for line := range tileOut {
			// Strip the tile key, which is only needed for sorting.
			_, rest, _ := strings.Cut(line, ",")
			if _, err := fmt.Fprintln(writer, rest); err != nil {
				return err
			}
		}
		return writer.Close()
	})
	if err := group.Wait(); err != nil {
		return err
	}
	if err := <-geoErr; err != nil {
		return err
	}
	if err := <-tileErr; err != nil {
		return err
	}
	if err := outFile.Close(); err != nil {
		return err
	}

	return PutInStorage(ctx, outFile.Name(), s3, "qrank", destPath, "application/zstd")
}

// ProcessGeoTagsTable reads the `geo_tags` table of a site, and emits
// lines of the form "page,lat,lon" for every coordinate on Earth.
func processGeoTagsTable(ctx context.Context, dumps string, site *WikiSite, out chan<- string) error {
	ymd := site.LastDumped.Format("20060102")
	fileName := fmt.Sprintf("%s-%s-geo_tags.sql.gz", site.Key, ymd)
	file, err := os.Open(filepath.Join(dumps, site.Key, ymd, fileName))
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
	pageCol := slices.Index(columns, "gt_page_id")
	globeCol := slices.Index(columns, "gt_globe")
	latCol := slices.Index(columns, "gt_lat")
	lonCol := slices.Index(columns, "gt_lon")
	if pageCol < 0 || globeCol < 0 || latCol < 0 || lonCol < 0 {
		return fmt.Errorf("%s: missing columns, got %v", fileName, columns)
	}

	for {
		row, err := reader.Read()
		if err != nil {
			return err
		}
		if row == nil {
			return nil
		}
		if row[globeCol] != "earth" {
			continue
		}
		lat, err := strconv.ParseFloat(row[latCol], 64)
		if err != nil {
			continue
		}
		lon, err := strconv.ParseFloat(row[lonCol], 64)
		if err != nil {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- fmt.Sprintf("%s,%s,%s", row[pageCol],
			strconv.FormatFloat(lat, 'f', -1, 64), strconv.FormatFloat(lon, 'f', -1, 64)):
		}
	}
}

// JoinCoordinates joins the lines of a page_signals file, such as
// "200,Q72,...", with coordinate lines of the form "200,47.37,8.54".
// Both inputs must be sorted in string order. For every coordinate
// of a page with a Wikidata item, it emits "tilekey,item,lat,lon".
func joinCoordinates(ctx context.Context, pages *bufio.Scanner, coords <-chan string, out chan<- string) error {
	var page, item string
	for line := range coords {
		cpage, latlon, ok := strings.Cut(line, ",")
		if !ok {
			return fmt.Errorf("bad coordinates line: %q", line)
		}
		for page+"," < cpage+"," {
			if !pages.Scan() {
				page, item = "\xff", ""
				break
			}
			cols := strings.SplitN(pages.Text(), ",", 3)
			if len(cols) < 2 {
				return fmt.Errorf("bad page signals line: %q", pages.Text())
			}
			page, item = cols[0], strings.TrimPrefix(cols[1], "Q")
		}
		if page != cpage || item == "" {
			continue
		}
		slat, slon, _ := strings.Cut(latlon, ",")
		lat, err := strconv.ParseFloat(slat, 64)
		if err != nil {
			return fmt.Errorf("bad coordinates line: %q", line)
		}
		lon, err := strconv.ParseFloat(slon, 64)
		if err != nil {
			return fmt.Errorf("bad coordinates line: %q", line)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- fmt.Sprintf("%s,%s,%s", coordinatesTileKey(lat, lon), item, latlon):
		}
	}
	return pages.Err()
}
