// SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"fmt"
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
