package main

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/minio/minio-go/v7"
)

func TestCleanup(t *testing.T) {
	ctx := context.Background()
	s := NewFakeStorageClient()
	for _, path := range []string{
		"public/osmviews-20211205.tiff",
		"public/osmviews-20211212.tiff",
		"public/osmviews-20211226.tiff",
		"public/osmviews-20220102.tiff",
		"public/osmviews-20220109.tiff",
		"public/osmviews-not-matching-pattern.txt",
	} {
		s.FPutObject(ctx, "qrank", path, path, minio.PutObjectOptions{})
	}
	if err := Cleanup(s); err != nil {
		t.Fatal(err)
	}

	got := make([]string, 0)
	for f := range s.ListObjects(ctx, "qrank", minio.ListObjectsOptions{}) {
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

type FakeStorageClient struct {
	Files map[string]string
}

func (s *FakeStorageClient) BucketExists(ctx context.Context, bucket string) (bool, error) {
	return bucket == "qrank", nil
}

func (s *FakeStorageClient) FPutObject(ctx context.Context, bucket string, remotepath string, localpath string, opts minio.PutObjectOptions) (minio.UploadInfo, error) {
	s.Files[remotepath] = localpath
	return minio.UploadInfo{}, nil
}

func (s *FakeStorageClient) ListObjects(ctx context.Context, bucketName string, opts minio.ListObjectsOptions) <-chan minio.ObjectInfo {
	ch := make(chan minio.ObjectInfo)
	go func() {
		for path, _ := range s.Files {
			ch <- minio.ObjectInfo{Key: path}
		}
		close(ch)
	}()
	return ch
}

func (s *FakeStorageClient) RemoveObject(ctx context.Context, bucketName, objectName string, opts minio.RemoveObjectOptions) error {
	delete(s.Files, objectName)
	return nil
}

func (s *FakeStorageClient) StatObject(ctx context.Context, bucket string, path string, opts minio.StatObjectOptions) (minio.ObjectInfo, error) {
	if _, present := s.Files[path]; present {
		return minio.ObjectInfo{Key: path}, nil
	} else {
		return minio.ObjectInfo{}, fmt.Errorf("no such file: %s", path)
	}
}

func NewFakeStorageClient() *FakeStorageClient {
	return &FakeStorageClient{Files: make(map[string]string)}
}
