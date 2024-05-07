// SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"fmt"
	"os"

	"github.com/minio/minio-go/v7"
)

type FakeS3 struct {
	data map[string][]byte
}

func NewFakeS3() *FakeS3 {
	fake := &FakeS3{
		data: make(map[string][]byte, 10),
	}
	return fake
}

func (s3 *FakeS3) ListObjects(ctx context.Context, bucketName string, opts minio.ListObjectsOptions) <-chan minio.ObjectInfo {
	ch := make(chan minio.ObjectInfo, 2)
	go func() {
		defer close(ch)
		if bucketName == "qrank" {
			for key, _ := range s3.data {
				ch <- minio.ObjectInfo{Key: key}
			}
		}
	}()
	return ch
}

func (s3 *FakeS3) FGetObject(ctx context.Context, bucketName, objectName, filePath string, opts minio.GetObjectOptions) error {
	if bucketName != "qrank" {
		return fmt.Errorf(`unexpected bucket "%s"`, bucketName)
	}
	data, ok := s3.data[objectName]
	if !ok {
		return fmt.Errorf("object not found: %s", objectName)
	}
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}

	if _, err := file.Write(data); err != nil {
		file.Close()
		os.Remove(filePath)
		return err
	}

	if err := file.Close(); err != nil {
		os.Remove(filePath)
		return err
	}

	return nil
}

func (s3 *FakeS3) FPutObject(ctx context.Context, bucketName, objectName, filePath string, opts minio.PutObjectOptions) (minio.UploadInfo, error) {
	info := minio.UploadInfo{}
	if bucketName != "qrank" {
		return info, fmt.Errorf("unexpected bucket %v", bucketName)
	}

	file, err := os.ReadFile(filePath)
	if err != nil {
		return info, err
	}

	s3.data[objectName] = file
	return info, nil
}
