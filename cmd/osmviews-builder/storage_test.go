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
		"internal/otherproject’s_data_should/not/be/touched.txt",
		"public/osmviews-not-matching-pattern.txt",
		"public/qrank-20210830.csv.gz",
	} {
		if err := s.PutFile(ctx, "qrank", path, localpath, "text/plain"); err != nil {
			t.Fatal(err)
		}
	}
	for _, date := range []string{"20211205", "20211212", "20211226", "20220102", "20220109"} {
		for _, p := range []struct{ pattern, contentType string }{
			{"public/osmviews-%s.tiff", "image/tiff"},
			{"public/osmviews-stats-%s.json", "application/json"},
		} {
			path := fmt.Sprintf(p.pattern, date)
			if err := s.PutFile(ctx, "qrank", path, localpath, p.contentType); err != nil {
				t.Fatal(err)
			}
		}
	}
	for year := 2021; year <= 2022; year++ {
		for week := 1; week <= 52; week++ {
			if year == 2022 && week > 40 {
				break
			}
			path := fmt.Sprintf("internal/osmviews-builder/tilelogs-%d-W%02d.br", year, week)
			if err := s.PutFile(ctx, "qrank", path, localpath, "application/x-brotli"); err != nil {
				t.Fatal(err)
			}
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
		"internal/osmviews-builder/tilelogs-2021-W33.br",
		"internal/osmviews-builder/tilelogs-2021-W34.br",
		"internal/osmviews-builder/tilelogs-2021-W35.br",
		"internal/osmviews-builder/tilelogs-2021-W36.br",
		"internal/osmviews-builder/tilelogs-2021-W37.br",
		"internal/osmviews-builder/tilelogs-2021-W38.br",
		"internal/osmviews-builder/tilelogs-2021-W39.br",
		"internal/osmviews-builder/tilelogs-2021-W40.br",
		"internal/osmviews-builder/tilelogs-2021-W41.br",
		"internal/osmviews-builder/tilelogs-2021-W42.br",
		"internal/osmviews-builder/tilelogs-2021-W43.br",
		"internal/osmviews-builder/tilelogs-2021-W44.br",
		"internal/osmviews-builder/tilelogs-2021-W45.br",
		"internal/osmviews-builder/tilelogs-2021-W46.br",
		"internal/osmviews-builder/tilelogs-2021-W47.br",
		"internal/osmviews-builder/tilelogs-2021-W48.br",
		"internal/osmviews-builder/tilelogs-2021-W49.br",
		"internal/osmviews-builder/tilelogs-2021-W50.br",
		"internal/osmviews-builder/tilelogs-2021-W51.br",
		"internal/osmviews-builder/tilelogs-2021-W52.br",
		"internal/osmviews-builder/tilelogs-2022-W01.br",
		"internal/osmviews-builder/tilelogs-2022-W02.br",
		"internal/osmviews-builder/tilelogs-2022-W03.br",
		"internal/osmviews-builder/tilelogs-2022-W04.br",
		"internal/osmviews-builder/tilelogs-2022-W05.br",
		"internal/osmviews-builder/tilelogs-2022-W06.br",
		"internal/osmviews-builder/tilelogs-2022-W07.br",
		"internal/osmviews-builder/tilelogs-2022-W08.br",
		"internal/osmviews-builder/tilelogs-2022-W09.br",
		"internal/osmviews-builder/tilelogs-2022-W10.br",
		"internal/osmviews-builder/tilelogs-2022-W11.br",
		"internal/osmviews-builder/tilelogs-2022-W12.br",
		"internal/osmviews-builder/tilelogs-2022-W13.br",
		"internal/osmviews-builder/tilelogs-2022-W14.br",
		"internal/osmviews-builder/tilelogs-2022-W15.br",
		"internal/osmviews-builder/tilelogs-2022-W16.br",
		"internal/osmviews-builder/tilelogs-2022-W17.br",
		"internal/osmviews-builder/tilelogs-2022-W18.br",
		"internal/osmviews-builder/tilelogs-2022-W19.br",
		"internal/osmviews-builder/tilelogs-2022-W20.br",
		"internal/osmviews-builder/tilelogs-2022-W21.br",
		"internal/osmviews-builder/tilelogs-2022-W22.br",
		"internal/osmviews-builder/tilelogs-2022-W23.br",
		"internal/osmviews-builder/tilelogs-2022-W24.br",
		"internal/osmviews-builder/tilelogs-2022-W25.br",
		"internal/osmviews-builder/tilelogs-2022-W26.br",
		"internal/osmviews-builder/tilelogs-2022-W27.br",
		"internal/osmviews-builder/tilelogs-2022-W28.br",
		"internal/osmviews-builder/tilelogs-2022-W29.br",
		"internal/osmviews-builder/tilelogs-2022-W30.br",
		"internal/osmviews-builder/tilelogs-2022-W31.br",
		"internal/osmviews-builder/tilelogs-2022-W32.br",
		"internal/osmviews-builder/tilelogs-2022-W33.br",
		"internal/osmviews-builder/tilelogs-2022-W34.br",
		"internal/osmviews-builder/tilelogs-2022-W35.br",
		"internal/osmviews-builder/tilelogs-2022-W36.br",
		"internal/osmviews-builder/tilelogs-2022-W37.br",
		"internal/osmviews-builder/tilelogs-2022-W38.br",
		"internal/osmviews-builder/tilelogs-2022-W39.br",
		"internal/osmviews-builder/tilelogs-2022-W40.br",
		"internal/otherproject’s_data_should/not/be/touched.txt",
		"public/osmviews-20211226.tiff",
		"public/osmviews-20220102.tiff",
		"public/osmviews-20220109.tiff",
		"public/osmviews-not-matching-pattern.txt",
		"public/osmviews-stats-20211226.json",
		"public/osmviews-stats-20220102.json",
		"public/osmviews-stats-20220109.json",
		"public/qrank-20210830.csv.gz",
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
