package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"sort"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// StorageClient is the subset of minio.Client used in this program.
type StorageClient interface {
	ListObjects(ctx context.Context, bucketName string, opts minio.ListObjectsOptions) <-chan minio.ObjectInfo
	BucketExists(ctx context.Context, bucket string) (bool, error)
	GetObject(ctx context.Context, bucketName, objectName string, opts minio.GetObjectOptions) (*minio.Object, error)
	FPutObject(ctx context.Context, bucket string, remotepath string, localpath string, opts minio.PutObjectOptions) (minio.UploadInfo, error)
	RemoveObject(ctx context.Context, bucketName, objectName string, opts minio.RemoveObjectOptions) error
	StatObject(ctx context.Context, bucket string, path string, opts minio.StatObjectOptions) (minio.ObjectInfo, error)
}

// NewStorageClient sets up a client for accessing S3-compatible object storage.
func NewStorageClient(keypath string) (StorageClient, error) {
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
	return client, nil
}

func Cleanup(s StorageClient) error {
	ctx := context.Background()
	re := regexp.MustCompile(`public/osmviews-\d{8}\.tiff`)

	osmviews := make([]string, 0, 10)
	opts := minio.ListObjectsOptions{Prefix: "public/osmviews-", Recursive: true}
	for f := range s.ListObjects(ctx, "qrank", opts) {
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
			if err := s.RemoveObject(ctx, "qrank", path, minio.RemoveObjectOptions{}); err != nil {
				return err
			}
		}
	}

	return nil
}
