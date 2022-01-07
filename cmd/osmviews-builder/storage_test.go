package main

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/minio/minio-go/v7"
)

func TestCleanup(t *testing.T) {
	ctx := context.Background()

	localpath := filepath.Join(t.TempDir(), "testcleanup")
	if err := os.WriteFile(localpath, []byte("foo"), 0644); err != nil {
		t.Fatal(err)
	}

	s := NewFakeStorageClient()
	for _, path := range []string{
		"public/osmviews-20211205.tiff",
		"public/osmviews-20211212.tiff",
		"public/osmviews-20211226.tiff",
		"public/osmviews-20220102.tiff",
		"public/osmviews-20220109.tiff",
		"public/osmviews-not-matching-pattern.txt",
	} {
		s.FPutObject(ctx, "qrank", path, localpath, minio.PutObjectOptions{})
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

type FakeStorageObject struct {
	Content []byte
	Info    minio.ObjectInfo
}

type FakeStorageClient struct {
	Files map[string]*FakeStorageObject
}

func (s *FakeStorageClient) BucketExists(ctx context.Context, bucket string) (bool, error) {
	return bucket == "qrank", nil
}

func (s *FakeStorageClient) FPutObject(ctx context.Context, bucket string, remotepath string, localpath string, opts minio.PutObjectOptions) (minio.UploadInfo, error) {
	stat, err := os.Stat(localpath)
	if err != nil {
		return minio.UploadInfo{}, err
	}

	content, err := os.ReadFile(localpath)
	if err != nil {
		return minio.UploadInfo{}, err
	}

	digest := md5.Sum(content)
	etag := base64.RawStdEncoding.EncodeToString(digest[0:len(digest)])
	info := minio.ObjectInfo{
		Key:          remotepath,
		ContentType:  opts.ContentType,
		LastModified: stat.ModTime(),
		ETag:         etag,
		Size:         int64(len(content)),
	}

	s.Files[remotepath] = &FakeStorageObject{content, info}
	return minio.UploadInfo{ETag: info.ETag}, nil
}

func (s *FakeStorageClient) GetObject(ctx context.Context, bucketName, objectName string, opts minio.GetObjectOptions) (*minio.Object, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *FakeStorageClient) ListObjects(ctx context.Context, bucketName string, opts minio.ListObjectsOptions) <-chan minio.ObjectInfo {
	ch := make(chan minio.ObjectInfo)
	go func() {
		for _, fakeFile := range s.Files {
			ch <- fakeFile.Info
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
	if f, present := s.Files[path]; present {
		return f.Info, nil
	} else {
		return minio.ObjectInfo{}, fmt.Errorf("no such file: %s", path)
	}
}

func NewFakeStorageClient() *FakeStorageClient {
	return &FakeStorageClient{Files: make(map[string]*FakeStorageObject)}
}
