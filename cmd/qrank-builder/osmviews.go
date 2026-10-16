// SPDX-FileCopyrightText: 2026 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/brawer/osmviews-go"
	"github.com/klauspost/compress/zstd"
	"github.com/lanrat/extsort"
	"github.com/minio/minio-go/v7"
	"golang.org/x/sync/errgroup"
)

// The osmviews_score column of the item signals tells how much people
// look at an item's location on OpenStreetMap-based maps, from 0 to 1.
// It comes from the OSMViews GeoTIFF (https://osmviews.brawer.ch),
// which gets rebuilt weekly. We look up every coordinate of an item
// and take the maximum; items without coordinates get null.
//
// The GeoTIFF is about 600 MB. We keep a copy in our own storage under
// osmviews/osmviews-YYYYMMDD.tiff, next to its bill of materials,
// so that a build does not download it again while the upstream version
// is unchanged, and so that a build can still proceed with the last
// known version if osmviews.brawer.ch is unreachable.

// OSMViewsDatapackageURL is the Frictionless data package that
// names the current OSMViews GeoTIFF and its SHA-256 hash.
var osmviewsDatapackageURL = "https://osmviews.brawer.ch/data/datapackage.json"

// OSMViewsProvenance describes the OSMViews GeoTIFF that went into
// a build of the item signals. The PURL and hashes are copied from the
// GeoTIFF's own CycloneDX bill of materials, which is kept indefinitely
// at BOMURL.
type OSMViewsProvenance struct {
	Version   string `json:"version"` // YYYY-MM-DD
	URL       string `json:"url"`
	PURL      string `json:"purl"`
	SHA256    string `json:"sha256"`
	SHA512    string `json:"sha512,omitempty"`
	BOMURL    string `json:"bomUrl"`
	BOMSHA256 string `json:"bomSha256"`
}

var osmviewsCachePathRegexp = regexp.MustCompile(`^osmviews/osmviews-(\d{8})\.cdx\.json$`)

func osmviewsCachePath(ymd, ext string) string {
	return fmt.Sprintf("osmviews/osmviews-%s.%s", ymd, ext)
}

// FetchOSMViews makes the current OSMViews GeoTIFF available on local disk.
// The caller must remove the returned file when done. If the upstream
// server cannot be reached, it falls back to the newest cached version.
func fetchOSMViews(ctx context.Context, client *http.Client, s3 S3) (string, *OSMViewsProvenance, error) {
	path, prov, err := fetchCurrentOSMViews(ctx, client, s3)
	if err == nil {
		return path, prov, pruneOSMViewsCache(ctx, 2, s3)
	}

	logger.Printf("cannot fetch current OSMViews, trying cache: %v", err)
	path, prov, cacheErr := fetchCachedOSMViews(ctx, "", s3)
	if cacheErr != nil {
		return "", nil, errors.Join(err, cacheErr)
	}
	logger.Printf("using cached OSMViews %s", prov.Version)
	return path, prov, nil
}

func fetchCurrentOSMViews(ctx context.Context, client *http.Client, s3 S3) (string, *OSMViewsProvenance, error) {
	data, err := httpGet(ctx, client, osmviewsDatapackageURL)
	if err != nil {
		return "", nil, err
	}
	var pkg struct {
		Version   string `json:"version"`
		Resources []struct {
			Name  string `json:"name"`
			Path  string `json:"path"`
			Bytes int64  `json:"bytes"`
			Hash  string `json:"hash"`
		} `json:"resources"`
	}
	if err := json.Unmarshal(data, &pkg); err != nil {
		return "", nil, fmt.Errorf("%s: %w", osmviewsDatapackageURL, err)
	}
	version, err := time.Parse(time.DateOnly, pkg.Version)
	if err != nil {
		return "", nil, fmt.Errorf("%s: bad version %q", osmviewsDatapackageURL, pkg.Version)
	}
	ymd := version.Format("20060102")

	var tiffPath, tiffHash, bomPath, bomHash string
	var tiffBytes int64
	for _, r := range pkg.Resources {
		switch r.Name {
		case "osmviews":
			tiffPath, tiffHash, tiffBytes = r.Path, r.Hash, r.Bytes
		case "bom":
			bomPath, bomHash = r.Path, r.Hash
		}
	}
	if tiffPath == "" || bomPath == "" {
		return "", nil, fmt.Errorf("%s: missing osmviews or bom resource", osmviewsDatapackageURL)
	}

	// If we already have this version in our cache, there is
	// no need to download it again.
	if path, prov, err := fetchCachedOSMViews(ctx, ymd, s3); err == nil {
		return path, prov, nil
	}

	tiffURL, err := resolveURL(osmviewsDatapackageURL, tiffPath)
	if err != nil {
		return "", nil, err
	}
	bomURL, err := resolveURL(osmviewsDatapackageURL, bomPath)
	if err != nil {
		return "", nil, err
	}
	bom, err := httpGet(ctx, client, bomURL)
	if err != nil {
		return "", nil, err
	}
	if err := checkSHA256(bom, bomHash); err != nil {
		return "", nil, fmt.Errorf("%s: %w", bomURL, err)
	}
	prov, err := parseOSMViewsBOM(bom, pkg.Version, tiffURL, bomURL)
	if err != nil {
		return "", nil, err
	}
	if "sha256:"+prov.SHA256 != tiffHash {
		return "", nil, fmt.Errorf("%s: hash %s does not match data package %s", bomURL, prov.SHA256, tiffHash)
	}

	logger.Printf("downloading %s", tiffURL)
	tiff, err := downloadFile(ctx, client, tiffURL)
	if err != nil {
		return "", nil, err
	}
	if err := checkFileSHA256(tiff, tiffHash, tiffBytes); err != nil {
		os.Remove(tiff)
		return "", nil, fmt.Errorf("%s: %w", tiffURL, err)
	}

	// Store the GeoTIFF before its BOM, so that the BOM marks a
	// complete copy in the cache.
	if err := PutInStorage(ctx, tiff, s3, "qrank", osmviewsCachePath(ymd, "tiff"), "image/tiff"); err != nil {
		os.Remove(tiff)
		return "", nil, err
	}
	if err := putBytesInStorage(ctx, bom, s3, osmviewsCachePath(ymd, "cdx.json"), "application/vnd.cyclonedx+json"); err != nil {
		os.Remove(tiff)
		return "", nil, err
	}
	return tiff, prov, nil
}

// FetchCachedOSMViews retrieves a GeoTIFF from our own storage.
// If ymd is empty, it takes the newest cached version.
func fetchCachedOSMViews(ctx context.Context, ymd string, s3 S3) (string, *OSMViewsProvenance, error) {
	if ymd == "" {
		versions, err := cachedOSMViewsVersions(ctx, s3)
		if err != nil {
			return "", nil, err
		}
		if len(versions) == 0 {
			return "", nil, fmt.Errorf("no OSMViews GeoTIFF in cache")
		}
		ymd = versions[len(versions)-1]
	}

	r, err := NewS3Reader(ctx, "qrank", osmviewsCachePath(ymd, "cdx.json"), s3)
	if err != nil {
		return "", nil, err
	}
	bom, err := io.ReadAll(r)
	r.Close()
	if err != nil {
		return "", nil, err
	}

	version, err := time.Parse("20060102", ymd)
	if err != nil {
		return "", nil, err
	}
	tiffURL, err := resolveURL(osmviewsDatapackageURL, fmt.Sprintf("osmviews-%s.tiff", ymd))
	if err != nil {
		return "", nil, err
	}
	bomURL, err := resolveURL(osmviewsDatapackageURL, fmt.Sprintf("osmviews-%s.cdx.json", ymd))
	if err != nil {
		return "", nil, err
	}
	prov, err := parseOSMViewsBOM(bom, version.Format(time.DateOnly), tiffURL, bomURL)
	if err != nil {
		return "", nil, err
	}

	tiff, err := os.CreateTemp("", "osmviews-*.tiff")
	if err != nil {
		return "", nil, err
	}
	tiff.Close()
	if err := s3.FGetObject(ctx, "qrank", osmviewsCachePath(ymd, "tiff"), tiff.Name(), minio.GetObjectOptions{}); err != nil {
		os.Remove(tiff.Name())
		return "", nil, err
	}
	if err := checkFileSHA256(tiff.Name(), "sha256:"+prov.SHA256, -1); err != nil {
		os.Remove(tiff.Name())
		return "", nil, err
	}
	return tiff.Name(), prov, nil
}

// CachedOSMViewsVersions returns the versions in our cache, oldest first,
// as YYYYMMDD strings.
func cachedOSMViewsVersions(ctx context.Context, s3 S3) ([]string, error) {
	var versions []string
	for obj := range s3.ListObjects(ctx, "qrank", minio.ListObjectsOptions{Prefix: "osmviews/"}) {
		if obj.Err != nil {
			return nil, obj.Err
		}
		if m := osmviewsCachePathRegexp.FindStringSubmatch(obj.Key); m != nil {
			versions = append(versions, m[1])
		}
	}
	sort.Strings(versions)
	return versions, nil
}

// PruneOSMViewsCache removes old GeoTIFFs from our cache,
// keeping only the newest `keep` versions.
func pruneOSMViewsCache(ctx context.Context, keep int, s3 S3) error {
	versions, err := cachedOSMViewsVersions(ctx, s3)
	if err != nil {
		return err
	}
	for i := 0; i < len(versions)-keep; i++ {
		for _, ext := range []string{"cdx.json", "tiff"} {
			path := osmviewsCachePath(versions[i], ext)
			if err := s3.RemoveObject(ctx, "qrank", path, minio.RemoveObjectOptions{}); err != nil {
				return err
			}
		}
	}
	return nil
}

// ParseOSMViewsBOM extracts the PURL and hashes of the GeoTIFF
// from its CycloneDX bill of materials.
func parseOSMViewsBOM(bom []byte, version, tiffURL, bomURL string) (*OSMViewsProvenance, error) {
	var doc struct {
		Metadata struct {
			Component struct {
				PURL   string `json:"purl"`
				Hashes []struct {
					Alg     string `json:"alg"`
					Content string `json:"content"`
				} `json:"hashes"`
			} `json:"component"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal(bom, &doc); err != nil {
		return nil, fmt.Errorf("%s: %w", bomURL, err)
	}
	sum := sha256.Sum256(bom)
	prov := &OSMViewsProvenance{
		Version:   version,
		URL:       tiffURL,
		PURL:      doc.Metadata.Component.PURL,
		BOMURL:    bomURL,
		BOMSHA256: hex.EncodeToString(sum[:]),
	}
	for _, h := range doc.Metadata.Component.Hashes {
		switch h.Alg {
		case "SHA-256":
			prov.SHA256 = h.Content
		case "SHA-512":
			prov.SHA512 = h.Content
		}
	}
	if prov.SHA256 == "" || prov.PURL == "" {
		return nil, fmt.Errorf("%s: no purl or SHA-256 in metadata.component", bomURL)
	}
	return prov, nil
}

func resolveURL(base, ref string) (string, error) {
	b, err := url.Parse(base)
	if err != nil {
		return "", err
	}
	r, err := url.Parse(ref)
	if err != nil {
		return "", err
	}
	return b.ResolveReference(r).String(), nil
}

func newRequest(ctx context.Context, u string) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "QRankBuilderBot/1.0 (https://github.com/brawer/wikidata-qrank; sascha@brawer.ch)")
	return req, nil
}

// HttpGet fetches a small resource into memory.
func httpGet(ctx context.Context, client *http.Client, u string) ([]byte, error) {
	req, err := newRequest(ctx, u)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("failed to fetch %s; StatusCode=%d", u, resp.StatusCode)
	}
	return io.ReadAll(io.LimitReader(resp.Body, 16<<20))
}

// DownloadFile fetches a resource into a temporary file,
// whose path gets returned.
func downloadFile(ctx context.Context, client *http.Client, u string) (string, error) {
	req, err := newRequest(ctx, u)
	if err != nil {
		return "", err
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("failed to fetch %s; StatusCode=%d", u, resp.StatusCode)
	}
	out, err := os.CreateTemp("", "osmviews-*.tiff")
	if err != nil {
		return "", err
	}
	if _, err := io.Copy(out, resp.Body); err != nil {
		out.Close()
		os.Remove(out.Name())
		return "", err
	}
	if err := out.Close(); err != nil {
		os.Remove(out.Name())
		return "", err
	}
	return out.Name(), nil
}

func checkSHA256(data []byte, want string) error {
	sum := sha256.Sum256(data)
	if got := "sha256:" + hex.EncodeToString(sum[:]); got != want {
		return fmt.Errorf("got %s, want %s", got, want)
	}
	return nil
}

// CheckFileSHA256 verifies the hash of a file, and its size unless
// wantBytes is negative.
func checkFileSHA256(path string, want string, wantBytes int64) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return err
	}
	if wantBytes >= 0 && n != wantBytes {
		return fmt.Errorf("got %d bytes, want %d", n, wantBytes)
	}
	if got := "sha256:" + hex.EncodeToString(h.Sum(nil)); got != want {
		return fmt.Errorf("got %s, want %s", got, want)
	}
	return nil
}

func putBytesInStorage(ctx context.Context, data []byte, s3 S3, dest, contentType string) error {
	tmp, err := os.CreateTemp("", "*-osmviews")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	defer tmp.Close()
	if _, err := tmp.Write(data); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return PutInStorage(ctx, tmp.Name(), s3, "qrank", dest, contentType)
}

// ItemScore is the OSMViews score of one coordinate of an item.
type itemScore struct {
	item  int64
	score float32
}

func (s itemScore) ToBytes() []byte {
	buf := make([]byte, binary.MaxVarintLen64+4)
	n := binary.PutVarint(buf, s.item)
	binary.LittleEndian.PutUint32(buf[n:], math.Float32bits(s.score))
	return buf[:n+4]
}

func itemScoreFromBytes(b []byte) extsort.SortType {
	item, n := binary.Varint(b)
	score := math.Float32frombits(binary.LittleEndian.Uint32(b[n:]))
	return itemScore{item: item, score: score}
}

func itemScoreLess(a, b extsort.SortType) bool {
	aa, bb := a.(itemScore), b.(itemScore)
	if aa.item != bb.item {
		return aa.item < bb.item
	}
	return aa.score > bb.score
}

// ScoreItemCoordinates looks up every coordinate of an item_coordinates
// file in the OSMViews raster. It writes the highest score of each item
// to a local file of fixed-size records sorted by item, whose path gets
// returned; the caller must remove it when done.
func scoreItemCoordinates(ctx context.Context, raster string, coordinates string, s3 S3) (string, error) {
	r, err := osmviews.Open(raster)
	if err != nil {
		return "", err
	}
	defer r.Close()

	reader, err := NewS3Reader(ctx, "qrank", coordinates, s3)
	if err != nil {
		return "", err
	}
	defer reader.Close()
	decompressor, err := zstd.NewReader(reader)
	if err != nil {
		return "", err
	}
	defer decompressor.Close()

	out, err := os.CreateTemp("", "*-osmviews_scores")
	if err != nil {
		return "", err
	}
	defer out.Close()

	scoreChan := make(chan extsort.SortType, 10000)
	config := extsort.DefaultConfig()
	config.ChunkSize = 8 * 1024 * 1024 / 16 // 8 MiB, 16 Bytes/record avg
	config.NumWorkers = runtime.NumCPU()
	sorter, outChan, errChan := extsort.New(scoreChan, itemScoreFromBytes, itemScoreLess, config)

	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error {
		defer close(scoreChan)
		scanner := bufio.NewScanner(decompressor)
		for scanner.Scan() {
			cols := strings.Split(scanner.Text(), ",")
			if len(cols) != 3 {
				return fmt.Errorf("%s: bad line %q", coordinates, scanner.Text())
			}
			item, err := strconv.ParseInt(cols[0], 10, 64)
			if err != nil {
				return err
			}
			lat, err := strconv.ParseFloat(cols[1], 64)
			if err != nil {
				return err
			}
			lon, err := strconv.ParseFloat(cols[2], 64)
			if err != nil {
				return err
			}
			score, err := r.Rank(lon, lat)
			if err != nil {
				return err
			}
			select {
			case <-groupCtx.Done():
				return groupCtx.Err()
			case scoreChan <- itemScore{item: item, score: float32(score)}:
			}
		}
		return scanner.Err()
	})
	group.Go(func() error {
		sorter.Sort(groupCtx)
		w := bufio.NewWriter(out)
		var last int64
		var buf [12]byte
		for s := range outChan {
			s := s.(itemScore)
			if s.item == last {
				continue // already wrote the highest score of this item
			}
			last = s.item
			binary.LittleEndian.PutUint64(buf[0:8], uint64(s.item))
			binary.LittleEndian.PutUint32(buf[8:12], math.Float32bits(s.score))
			if _, err := w.Write(buf[:]); err != nil {
				return err
			}
		}
		return w.Flush()
	})
	if err := group.Wait(); err != nil {
		os.Remove(out.Name())
		return "", err
	}
	if err := <-errChan; err != nil {
		os.Remove(out.Name())
		return "", err
	}
	if err := out.Close(); err != nil {
		os.Remove(out.Name())
		return "", err
	}

	m := r.Metrics()
	logger.Printf("scored %d coordinates with OSMViews %s; tile cache hit rate %.4f",
		m.Queries, r.Date().Format(time.DateOnly), m.HitRate())
	return out.Name(), nil
}

// OSMViewsScores reads a file written by scoreItemCoordinates.
// Lookups must come in increasing item order.
type osmviewsScores struct {
	r     *bufio.Reader
	item  int64
	score float32
	done  bool
}

func newOSMViewsScores(r io.Reader) *osmviewsScores {
	return &osmviewsScores{r: bufio.NewReader(r)}
}

// Lookup returns the score of an item, if it has one.
func (s *osmviewsScores) Lookup(item int64) (float32, bool, error) {
	var buf [12]byte
	for !s.done && s.item < item {
		if _, err := io.ReadFull(s.r, buf[:]); err != nil {
			if err == io.EOF {
				s.done = true
				break
			}
			return 0, false, err
		}
		s.item = int64(binary.LittleEndian.Uint64(buf[0:8]))
		s.score = math.Float32frombits(binary.LittleEndian.Uint32(buf[8:12]))
	}
	if !s.done && s.item == item {
		return s.score, true, nil
	}
	return 0, false, nil
}
