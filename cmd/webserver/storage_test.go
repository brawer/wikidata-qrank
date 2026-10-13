// SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
)

func TestStorage_Reload(t *testing.T) {
	storage := &Storage{
		client:  &fakeStorageClient{},
		workdir: t.TempDir(),
		files:   make(map[string]*localFile, 10),
	}

	old := filepath.Join(storage.workdir, "obsolete")
	if err := os.WriteFile(old, []byte("Old content\n"), 0644); err != nil {
		t.Fatal(err)
	}

	if err := storage.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(old); err == nil {
		t.Errorf("Storage.Reload() should delete old file %s", old)
	}

	if len(storage.files) != 1 {
		t.Errorf("got %d files in %v, expected 1", len(storage.files), storage.files)
	}

	loc := storage.files["hello.txt"]
	if loc.ETag != "Test-ETag" {
		t.Errorf("got ETag=%v, want %v", loc.ETag, "testetag")
	}

	gotLastmod := loc.LastModified.Format(time.RFC3339)
	wantLastmod := "2021-12-29T13:14:15Z"
	if gotLastmod != wantLastmod {
		t.Errorf("got LastMod=%s, want %s", gotLastmod, wantLastmod)
	}

	if loc.ContentType != "text/plain" {
		t.Errorf("got ContentType=%s, want text/plain", loc.ContentType)
	}

	gotContent, err := os.ReadFile(loc.Path)
	if err != nil {
		t.Error(err)
	}
	wantContent := "Hello"
	if string(gotContent) != wantContent {
		t.Errorf("got content=%v, want %v", string(gotContent), wantContent)
	}
}

func TestStorage_Retrieve(t *testing.T) {
	storage := &Storage{
		client:  &fakeStorageClient{},
		workdir: t.TempDir(),
		files:   make(map[string]*localFile, 10),
	}

	path := filepath.Join(storage.workdir, "c.txt")
	if err := os.WriteFile(path, []byte("Content"), 0644); err != nil {
		t.Fatal(err)
	}

	lastmod, _ := time.Parse(time.RFC3339, "2023-11-21T19:20:21Z")
	storage.files["c.txt"] = &localFile{
		Path:         path,
		ContentType:  "text/plain",
		ETag:         "ETag-123",
		LastModified: lastmod,
	}

	c, err := storage.Retrieve("c.txt")
	if err != nil {
		t.Fatal(err)
	}

	if c.ContentType != "text/plain" {
		t.Errorf("got ContentType=%v, want %v", c.ContentType, "text/plain")
	}

	if c.ETag != "ETag-123" {
		t.Errorf("got ETag=%v, want %v", c.ETag, "ETag-123")
	}

	if c.LastModified != lastmod {
		t.Errorf("got LastModified=%v, want %v", c.LastModified, lastmod)
	}

	buf := make([]byte, 2)
	n, err := c.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(buf) {
		t.Errorf("got n=%d, want %d", n, len(buf))
	}
	if string(buf) != "Co" {
		t.Errorf(`got %v, want "Co"`, string(buf))
	}

	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}

type fakeStorageClient struct {
	storageClient
}

func (s *fakeStorageClient) ListObjects(ctx context.Context, bucketName string, opts minio.ListObjectsOptions) <-chan minio.ObjectInfo {
	ch := make(chan minio.ObjectInfo)

	go func() {
		lastmod, _ := time.Parse(time.RFC3339, "2021-12-29T13:14:15Z")
		ch <- minio.ObjectInfo{
			Key:          "public/hello-20211229.txt",
			Size:         5,
			ETag:         "Test-ETag",
			LastModified: lastmod,
		}
		close(ch)
	}()
	return ch
}

func (s *fakeStorageClient) FGetObject(ctx context.Context, bucketName, objectName, filePath string, opts minio.GetObjectOptions) error {
	if bucketName == "qrank" && objectName == "public/hello-20211229.txt" {
		return os.WriteFile(filePath, []byte("Hello"), 0644)
	} else {
		return fmt.Errorf("object not found: %s/%s", bucketName, objectName)
	}
}

func TestStorage_ReloadDated(t *testing.T) {
	client := newMapStorageClient(map[string]string{
		"public/qrank-20260927.csv.gz":         "2026-09-27T10:00:00Z",
		"public/qrank-20261004.csv.gz":         "2026-10-04T10:00:00Z",
		"public/qrank-stats-20261004.json":     "2026-10-04T10:00:00Z",
		"public/qrank-20261004.cdx.json":       "2026-10-04T10:00:00Z",
		"public/item_signals-20260927.csv.zst": "2026-09-27T09:00:00Z",
		"public/item_signals-20261004.csv.zst": "2026-10-04T09:00:00Z",
	})
	storage := &Storage{client: client, workdir: t.TempDir()}
	if err := storage.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Without a data package, aliases point to the newest object.
	for name, want := range map[string]string{
		"qrank.csv.gz":              "public/qrank-20261004.csv.gz",
		"qrank-20260927.csv.gz":     "public/qrank-20260927.csv.gz",
		"qrank-20261004.csv.gz":     "public/qrank-20261004.csv.gz",
		"qrank-stats.json":          "public/qrank-stats-20261004.json",
		"qrank-stats-20261004.json": "public/qrank-stats-20261004.json",
		"qrank.cdx.json":            "public/qrank-20261004.cdx.json",
		"qrank-20261004.cdx.json":   "public/qrank-20261004.cdx.json",
		"item_signals.csv.zst":      "public/item_signals-20261004.csv.zst",
	} {
		checkStorageContent(t, storage, name, want)
	}

	// Item signals are only served under their alias, and the older
	// version is not even downloaded.
	for _, name := range []string{"item_signals-20260927.csv.zst", "item_signals-20261004.csv.zst", "datapackage.json"} {
		if _, err := storage.Retrieve(name); err == nil {
			t.Errorf("Retrieve(%q) should fail", name)
		}
	}
	if client.fetched["public/item_signals-20260927.csv.zst"] {
		t.Error("old item signals should not have been downloaded")
	}

	alias, _ := storage.Retrieve("qrank.csv.gz")
	defer alias.Close()
	if alias.Canonical != "qrank-20261004.csv.gz" || alias.Immutable {
		t.Errorf("alias: got Canonical=%q Immutable=%v", alias.Canonical, alias.Immutable)
	}
	// Item signals have no dated URL, so their alias must not name one.
	signals, _ := storage.Retrieve("item_signals.csv.zst")
	defer signals.Close()
	if signals.Canonical != "" {
		t.Errorf("item_signals.csv.zst: got Canonical=%q, want none", signals.Canonical)
	}
	dated, _ := storage.Retrieve("qrank-20261004.csv.gz")
	defer dated.Close()
	if dated.Canonical != "" || !dated.Immutable {
		t.Errorf("dated: got Canonical=%q Immutable=%v", dated.Canonical, dated.Immutable)
	}
	if dated.ContentType != "application/gzip" {
		t.Errorf("got ContentType=%q, want application/gzip", dated.ContentType)
	}
	bom, _ := storage.Retrieve("qrank-20261004.cdx.json")
	defer bom.Close()
	if bom.ContentType != "application/vnd.cyclonedx+json" {
		t.Errorf("got ContentType=%q, want application/vnd.cyclonedx+json", bom.ContentType)
	}

	// Each object is cached once, even if served under two names.
	entries, err := os.ReadDir(storage.workdir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 5 {
		t.Errorf("got %d files in workdir, want 5", len(entries))
	}
}

func TestStorage_ReloadDatapackage(t *testing.T) {
	client := newMapStorageClient(map[string]string{
		"public/qrank-20260927.csv.gz":     "2026-09-27T10:00:00Z",
		"public/qrank-stats-20260927.json": "2026-09-27T10:00:00Z",
		"public/qrank-20261004.csv.gz":     "2026-10-04T10:00:00Z",
		"public/datapackage.json":          "2026-09-27T10:00:01Z",
	})
	client.content["public/datapackage.json"] = `{"name": "qrank", "version": "2026-09-27"}`
	storage := &Storage{client: client, workdir: t.TempDir()}
	if err := storage.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}

	// The alias follows the data package, not the newest object, so
	// clients never see a CSV that is newer than the data package.
	checkStorageContent(t, storage, "qrank.csv.gz", "public/qrank-20260927.csv.gz")
	checkStorageContent(t, storage, "qrank-stats.json", "public/qrank-stats-20260927.json")
	checkStorageContent(t, storage, "qrank-20261004.csv.gz", "public/qrank-20261004.csv.gz")
	checkStorageContent(t, storage, "datapackage.json", client.content["public/datapackage.json"])
	dp, _ := storage.Retrieve("datapackage.json")
	defer dp.Close()
	if dp.ContentType != "application/json" || dp.Immutable || dp.Canonical != "" {
		t.Errorf("datapackage.json: got %+v", dp)
	}

	// A new version arrives. Once the data package names it, the alias moves.
	client.add("public/qrank-stats-20261004.json", "2026-10-04T10:00:00Z")
	client.add("public/datapackage.json", "2026-10-04T10:00:01Z")
	client.content["public/datapackage.json"] = `{"name": "qrank", "version": "2026-10-04"}`
	if err := storage.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	checkStorageContent(t, storage, "qrank.csv.gz", "public/qrank-20261004.csv.gz")
	checkStorageContent(t, storage, "qrank-stats.json", "public/qrank-stats-20261004.json")
	checkStorageContent(t, storage, "qrank-20260927.csv.gz", "public/qrank-20260927.csv.gz")
}

func TestReadDatapackageVersion(t *testing.T) {
	for _, tc := range []struct{ content, want string }{
		{`{"version": "2026-10-04"}`, "20261004"},
		{`{"version": "1.0"}`, ""},
		{`{}`, ""},
		{`not json`, ""},
	} {
		p := filepath.Join(t.TempDir(), "datapackage.json")
		if err := os.WriteFile(p, []byte(tc.content), 0644); err != nil {
			t.Fatal(err)
		}
		got, err := readDatapackageVersion(p)
		if got != tc.want || (err == nil) != (tc.want != "") {
			t.Errorf("readDatapackageVersion(%s): got %q, %v; want %q", tc.content, got, err, tc.want)
		}
	}
}

func checkStorageContent(t *testing.T, s *Storage, name string, want string) {
	t.Helper()
	c, err := s.Retrieve(name)
	if err != nil {
		t.Errorf("Retrieve(%q) failed: %v", name, err)
		return
	}
	defer c.Close()
	got, err := io.ReadAll(c)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != want {
		t.Errorf("Retrieve(%q): got %q, want %q", name, got, want)
	}
}

// MapStorageClient is a fake storageClient whose objects contain
// their own key, unless the test sets other content.
type mapStorageClient struct {
	objects map[string]minio.ObjectInfo
	content map[string]string
	fetched map[string]bool
}

func newMapStorageClient(objects map[string]string) *mapStorageClient {
	c := &mapStorageClient{
		objects: make(map[string]minio.ObjectInfo, len(objects)),
		content: make(map[string]string, len(objects)),
		fetched: make(map[string]bool, len(objects)),
	}
	for key, lastmod := range objects {
		c.add(key, lastmod)
	}
	return c
}

func (c *mapStorageClient) add(key, lastmod string) {
	t, err := time.Parse(time.RFC3339, lastmod)
	if err != nil {
		panic(err)
	}
	c.objects[key] = minio.ObjectInfo{Key: key, ETag: key + "@" + lastmod, LastModified: t}
	c.content[key] = key
}

func (c *mapStorageClient) ListObjects(ctx context.Context, bucketName string, opts minio.ListObjectsOptions) <-chan minio.ObjectInfo {
	ch := make(chan minio.ObjectInfo, len(c.objects))
	for _, obj := range c.objects {
		ch <- obj
	}
	close(ch)
	return ch
}

func (c *mapStorageClient) FGetObject(ctx context.Context, bucketName, objectName, filePath string, opts minio.GetObjectOptions) error {
	content, ok := c.content[objectName]
	if bucketName != "qrank" || !ok {
		return fmt.Errorf("object not found: %s/%s", bucketName, objectName)
	}
	c.fetched[objectName] = true
	return os.WriteFile(filePath, []byte(content), 0644)
}

func TestStorage_objRegexp(t *testing.T) {
	for _, s := range []string{
		"public/qrank-20220631.csv.gz",
		"public/qrank-stats-20220631.json",
		"public/osmviews-20220631.tiff",
		"public/qrank-20261004.cdx.json",
	} {
		if !objRegexp.MatchString(s) {
			t.Errorf("should match but does not: %v", s)
		}
	}

	for _, s := range []string{
		"internal/osmviews-builder/foobar.tiff",
		"public/qrank.csv.gz",
		"public/datapackage.json",
		"internal/public/qrank-20220631.csv.gz",
	} {
		if objRegexp.MatchString(s) {
			t.Errorf("should not match but does: %v", s)
		}
	}
}
