// SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"

	"github.com/minio/minio-go/v7"
	//"github.com/minio/minio-go/v7/pkg/credentials"
)

// S3 is the subset of minio.Client used in this program.
//
// We define our own interface for easier testing, so we only have to fake
// those parts of the (rather big) S3 interface that we actually use.
// A fake implementation for tests is in FakeS3, implemented in s3_test.go.
type S3 interface {
	ListObjects(ctx context.Context, bucketName string, opts minio.ListObjectsOptions) <-chan minio.ObjectInfo
	RemoveObject(ctx context.Context, bucketName string, objectName string, opts minio.RemoveObjectOptions) error
	FGetObject(ctx context.Context, bucketName, objectName, filePath string, opts minio.GetObjectOptions) error
	FPutObject(ctx context.Context, bucketName, objectName, filePath string, opts minio.PutObjectOptions) (minio.UploadInfo, error)
}

type tempFileReader struct {
	file *os.File
}

func (r *tempFileReader) Read(buf []byte) (int, error) {
	if r.file != nil {
		return r.file.Read(buf)
	} else {
		return 0, fmt.Errorf("already closed")
	}
}

func (r *tempFileReader) Close() error {
	if r.file == nil {
		return nil
	}
	err1 := r.file.Close()
	err2 := os.Remove(r.file.Name())
	r.file = nil
	if err1 != nil {
		return err1
	} else {
		return err2
	}
}

// NewS3Reader creates an io.ReadCloser for an S3 blob. To minimize the impact
// of network problems (Wikimedia’s datacenter is sometimes a little flaky),
// the blob is first downloaded to a temporary file on local disk; the temp file
// gets deleted when the caller deletes the returned io.ReadCloser.
func NewS3Reader(ctx context.Context, bucket string, path string, s3 S3) (io.ReadCloser, error) {
	opts := minio.GetObjectOptions{}

	// Initially, we did the following, but Wikimedia’s datacenter
	// seems to be too unreliable for reading a stream over the network
	// for more than a few seconds. Therefore, we now download our S3 blobs
	// to a temporary file. This decoupling of I/O from processing reduces
	// the likelihood of getting hit by a network problem, at the cost of
	// increasing local disk consumption. We don't actually know how Wikimedia’s
	// Kubernetes cluster implements /tmp for Toolforge job workers;
	// in case /tmp was always a RAM-backed tmpfs, this would be quite
	// wasteful. But other than processing input streams over the network,
	// downloading the blobs to /tmp seems to work better in production.
	//
	// See https://github.com/brawer/wikidata-qrank/issues/40 for background.
	//
	// if client, ok := s3.(*minio.Client); ok {
	//     obj, err := client.GetObject(ctx, bucket, path, opts)
	//	   if err != nil {
	//         return nil, err
	//     }
	//     return obj, nil
	// }

	temp, err := os.CreateTemp("", "s3*")
	if err != nil {
		return nil, err
	}
	if err := temp.Close(); err != nil {
		return nil, err
	}
	if err := s3.FGetObject(ctx, bucket, path, temp.Name(), opts); err != nil {
		return nil, err
	}
	tempPath := temp.Name()
	temp, err = os.Open(tempPath)
	if err != nil {
		os.Remove(tempPath)
		return nil, err
	}

	return &tempFileReader{temp}, nil
}

// PutInStorage stores a file in S3 storage.
func PutInStorage(ctx context.Context, file string, s3 S3, bucket string, dest string, contentType string) error {
	options := minio.PutObjectOptions{ContentType: contentType}
	_, err := s3.FPutObject(ctx, bucket, dest, file, options)
	return err
}

// ListStoredFiles returns what files are available in S3 storage.
func ListStoredFiles(ctx context.Context, filename string, s3 S3) (map[string][]string, error) {
	re := regexp.MustCompile(fmt.Sprintf(`^%s/([a-z0-9_\-]+)-(\d{8})-%s.zst$`, filename, filename))
	result := make(map[string][]string, 1000)
	opts := minio.ListObjectsOptions{Prefix: filename + "/"}
	for obj := range s3.ListObjects(ctx, "qrank", opts) {
		if obj.Err != nil {
			return nil, obj.Err
		}
		if match := re.FindStringSubmatch(obj.Key); match != nil {
			arr, ok := result[match[1]]
			if !ok {
				arr = make([]string, 0, 3)
			}
			result[match[1]] = append(arr, match[2])
		}
	}
	for _, val := range result {
		sort.Strings(val)
	}
	return result, nil
}
