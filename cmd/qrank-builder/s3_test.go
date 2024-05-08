// SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/minio/minio-go/v7"
)

type FakeS3 struct {
	data  map[string][]byte
	mutex sync.RWMutex
}

func NewFakeS3() *FakeS3 {
	fake := &FakeS3{
		data: make(map[string][]byte, 10),
	}
	return fake
}

func (s3 *FakeS3) ListObjects(ctx context.Context, bucketName string, opts minio.ListObjectsOptions) <-chan minio.ObjectInfo {
	s3.mutex.RLock()
	defer s3.mutex.RUnlock()

	ch := make(chan minio.ObjectInfo, 2)
	go func() {
		defer close(ch)
		prefix := opts.Prefix
		if bucketName == "qrank" {
			for key, _ := range s3.data {
				if strings.HasPrefix(key, prefix) {
					ch <- minio.ObjectInfo{Key: key}
				}
			}
		}
	}()
	return ch
}

func (s3 *FakeS3) RemoveObject(ctx context.Context, bucketName string, objectName string, opts minio.RemoveObjectOptions) error {
	s3.mutex.Lock()
	defer s3.mutex.Unlock()

	if bucketName != "qrank" {
		return fmt.Errorf(`unexpected bucket "%s"`, bucketName)
	}
	if _, ok := s3.data[objectName]; !ok {
		return fmt.Errorf(`file not found: %s`, objectName)
	}
	delete(s3.data, objectName)
	return nil
}

func (s3 *FakeS3) FGetObject(ctx context.Context, bucketName, objectName, filePath string, opts minio.GetObjectOptions) error {
	s3.mutex.RLock()
	defer s3.mutex.RUnlock()

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
	s3.mutex.Lock()
	defer s3.mutex.Unlock()

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
