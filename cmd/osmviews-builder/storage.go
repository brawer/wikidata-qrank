package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type ObjectInfo struct {
	Key         string
	ContentType string
	ETag        string
}

type Storage interface {
	BucketExists(ctx context.Context, bucket string) (bool, error)
	List(ctx context.Context, bucket, prefix string) ([]ObjectInfo, error)
	Stat(ctx context.Context, bucket, path string) (ObjectInfo, error)
	Get(ctx context.Context, bucket, path string) (io.Reader, error)
	PutFile(ctx context.Context, bucket string, remotepath string, localpath string, contentType string) error
	Remove(ctx context.Context, bucketName, path string) error
}

// RemoteStorage is an implementation of interface Storage that talks
// to a remote S3-compatible server. The other implementation is FakeStorage,
// which is used for testing.
type remoteStorage struct {
	client *minio.Client
}

func (s *remoteStorage) BucketExists(ctx context.Context, bucket string) (bool, error) {
	return s.client.BucketExists(ctx, bucket)
}

func (s *remoteStorage) List(ctx context.Context, bucket, prefix string) ([]ObjectInfo, error) {
	opts := minio.ListObjectsOptions{Prefix: prefix, Recursive: true}
	result := make([]ObjectInfo, 0)
	for f := range s.client.ListObjects(ctx, bucket, opts) {
		o := ObjectInfo{Key: f.Key, ContentType: f.ContentType, ETag: f.ETag}
		result = append(result, o)
	}
	return result, nil
}

func (s *remoteStorage) Stat(ctx context.Context, bucket, path string) (ObjectInfo, error) {
	st, err := s.client.StatObject(ctx, bucket, path, minio.StatObjectOptions{})
	if err != nil {
		return ObjectInfo{}, err
	}
	info := ObjectInfo{Key: st.Key, ContentType: st.ContentType, ETag: st.ETag}
	return info, nil
}

func (s *remoteStorage) Get(ctx context.Context, bucket, path string) (io.Reader, error) {
	return s.client.GetObject(ctx, bucket, path, minio.GetObjectOptions{})
}

func (s *remoteStorage) PutFile(ctx context.Context, bucket string, remotepath string, localpath string, contentType string) error {
	opts := minio.PutObjectOptions{ContentType: contentType}
	_, err := s.client.FPutObject(ctx, bucket, remotepath, localpath, opts)
	return err
}

func (s *remoteStorage) Remove(ctx context.Context, bucket, path string) error {
	return s.client.RemoveObject(ctx, bucket, path, minio.RemoveObjectOptions{})
}

// NewStorage sets up a client for accessing S3-compatible object storage.
func NewStorage(keypath string) (Storage, error) {
	data, err := os.ReadFile(keypath)
	if err != nil {
		return nil, err
	}

	var config struct{ Endpoint, Key, Secret string }
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, err
	}

	client, err := minio.New(config.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(config.Key, config.Secret, ""),
		Secure: true,
	})
	if err != nil {
		return nil, err
	}

	client.SetAppInfo("QRankOSMViewsBuilder", "0.1")
	return &remoteStorage{client: client}, nil
}

func Cleanup(s Storage) error {
	ctx := context.Background()
	re := regexp.MustCompile(`public/osmviews-\d{8}\.tiff`)

	osmviews := make([]string, 0, 10)
	files, err := s.List(ctx, "qrank", "public/osmviews-")
	if err != nil {
		return err
	}
	for _, f := range files {
		if re.MatchString(f.Key) {
			osmviews = append(osmviews, f.Key)
		}
	}

	if len(osmviews) > 3 {
		sort.Strings(osmviews)
		for _, path := range osmviews[0 : len(osmviews)-3] {
			msg := fmt.Sprintf("Deleting from storage: qrank/%s", path)
			fmt.Println(msg)
			if logger != nil {
				logger.Println(msg)
			}
			if err := s.Remove(ctx, "qrank", path); err != nil {
				return err
			}
		}
	}

	return nil
}
