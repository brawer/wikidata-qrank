// SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

func TestCleanup(t *testing.T) {
	ctx := context.Background()

	localpath := filepath.Join(t.TempDir(), "testcleanup")
	if err := os.WriteFile(localpath, []byte("foo"), 0644); err != nil {
		t.Fatal(err)
	}

	s := NewFakeStorage()
	for _, path := range []string{
		"public/osmviews-20211205.tiff",
		"public/osmviews-20211212.tiff",
		"public/osmviews-20211226.tiff",
		"public/osmviews-20220102.tiff",
		"public/osmviews-20220109.tiff",
		"public/osmviews-not-matching-pattern.txt",
	} {
		if err := s.PutFile(ctx, "qrank", path, localpath, "image/tiff"); err != nil {
			t.Fatal(err)
		}
	}
	if err := Cleanup(s); err != nil {
		t.Fatal(err)
	}

	got := make([]string, 0)
	files, err := s.List(ctx, "qrank", "public/")
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		got = append(got, f.Key)
	}
	sort.Strings(got)

	want := []string{
		"public/osmviews-20211226.tiff",
		"public/osmviews-20220102.tiff",
		"public/osmviews-20220109.tiff",
		"public/osmviews-not-matching-pattern.txt",
	}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Errorf("got %v, want %v", got, want)
	}
}

type FakeStorageObject struct {
	Content []byte
	Info    ObjectInfo
}

type FakeStorage struct {
	Files map[string]*FakeStorageObject
}

func (s *FakeStorage) BucketExists(ctx context.Context, bucket string) (bool, error) {
	return bucket == "qrank", nil
}

func (s *FakeStorage) PutFile(ctx context.Context, bucket string, remotepath string, localpath string, contentType string) error {
	content, err := os.ReadFile(localpath)
	if err != nil {
		return err
	}

	digest := md5.Sum(content)
	etag := base64.RawStdEncoding.EncodeToString(digest[0:len(digest)])
	info := ObjectInfo{
		Key:         remotepath,
		ContentType: contentType,
		ETag:        etag,
	}

	s.Files[remotepath] = &FakeStorageObject{content, info}
	return nil
}

func (s *FakeStorage) Get(ctx context.Context, bucket, path string) (io.Reader, error) {
	f, present := s.Files[path]
	if !present {
		return nil, fmt.Errorf("file not found: %s", path)
	}
	return bytes.NewReader(f.Content), nil
}

func (s *FakeStorage) List(ctx context.Context, bucket, prefix string) ([]ObjectInfo, error) {
	result := make([]ObjectInfo, 0, len(s.Files))
	for _, f := range s.Files {
		result = append(result, f.Info)
	}
	return result, nil
}

func (s *FakeStorage) Remove(ctx context.Context, bucketName, path string) error {
	delete(s.Files, path)
	return nil
}

func (s *FakeStorage) Stat(ctx context.Context, bucket string, path string) (ObjectInfo, error) {
	if f, present := s.Files[path]; present {
		return f.Info, nil
	} else {
		return ObjectInfo{}, fmt.Errorf("no such file: %s", path)
	}
}

func NewFakeStorage() *FakeStorage {
	return &FakeStorage{Files: make(map[string]*FakeStorageObject)}
}
