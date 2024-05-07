// SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"context"

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
	FGetObject(ctx context.Context, bucketName, objectName, filePath string, opts minio.GetObjectOptions) error
	FPutObject(ctx context.Context, bucketName, objectName, filePath string, opts minio.PutObjectOptions) (minio.UploadInfo, error)
}

// PutInStorage stores a file in S3 storage.
func PutInStorage(ctx context.Context, file string, s3 S3, bucket string, dest string, contentType string) error {
	options := minio.PutObjectOptions{ContentType: contentType}
	_, err := s3.FPutObject(ctx, bucket, dest, file, options)
	return err
}
