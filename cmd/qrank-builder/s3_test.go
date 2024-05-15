// SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/klauspost/compress/zstd"
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

func (s3 *FakeS3) ReadLines(path string) ([]string, error) {
	s3.mutex.RLock()
	defer s3.mutex.RUnlock()

	data, ok := s3.data[path]
	if !ok {
		return nil, fmt.Errorf("file not found: %s", path)
	}

	var buf bytes.Buffer
	if strings.HasSuffix(path, ".zst") {
		decoder, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(0))
		if err != nil {
			return nil, err
		}
		defer decoder.Close()
		data, err = decoder.DecodeAll(data, nil)
		if err != nil {
			return nil, err
		}
	}
	if _, err := buf.Write(data); err != nil {
		return nil, err
	}

	s := strings.TrimSuffix(string(data), "\n")
	return strings.Split(s, "\n"), nil
}

func (s3 *FakeS3) WriteLines(lines []string, path string) error {
	s3.mutex.Lock()
	defer s3.mutex.Unlock()

	var buf bytes.Buffer
	var writer io.WriteCloser
	var err error
	if strings.HasSuffix(path, ".zst") {
		zstdLevel := zstd.WithEncoderLevel(zstd.SpeedFastest)
		writer, err = zstd.NewWriter(&buf, zstdLevel)
		if err != nil {
			return err
		}
	} else {
		writer = NopWriteCloser(&buf)
	}

	for _, line := range lines {
		if _, err := writer.Write([]byte(line)); err != nil {
			return err
		}
		if _, err := writer.Write([]byte("\n")); err != nil {
			return err
		}
	}

	if err := writer.Close(); err != nil {
		return err
	}

	s3.data[path] = buf.Bytes()
	return nil
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

type testingWriteCloser struct {
	writer io.Writer
	closed bool
}

func TestingWriteCloser(w io.Writer) *testingWriteCloser {
	return &testingWriteCloser{writer: w}
}

func (t *testingWriteCloser) Write(p []byte) (int, error) {
	if t.closed {
		return 0, fmt.Errorf("already closed")
	}
	return t.writer.Write(p)
}

func (t *testingWriteCloser) Close() error {
	t.closed = true
	return nil
}

// NopWriteCloser returns a WriteCloser with a no-op Close method wrapping the
// provided Writer w. Like io.ReadCloser but for writing.
func NopWriteCloser(w io.Writer) io.WriteCloser {
	return &nopWriteCloser{w}
}

type nopWriteCloser struct {
	writer io.Writer
}

func (n *nopWriteCloser) Close() error {
	return nil
}

func (n *nopWriteCloser) Write(p []byte) (int, error) {
	return n.writer.Write(p)
}

func TestReadWriteLinest(t *testing.T) {
	s3 := NewFakeS3()
	lines := []string{"foo", "bar"}
	for _, path := range []string{"f", "f.zst"} {
		if err := s3.WriteLines([]string{"foo", "bar"}, path); err != nil {
			t.Error(err)
		}
		got, err := s3.ReadLines(path)
		if err != nil {
			t.Error(err)
		}
		if !slices.Equal(got, lines) {
			t.Errorf("got %v, want %v", got, lines)
		}
	}
}
