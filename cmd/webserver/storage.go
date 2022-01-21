// SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"encoding/base32"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type Storage struct {
	client  storageClient
	workdir string
	mutex   sync.RWMutex
	files   map[string]*localFile
}

// LocalFile represents a file in the local working directory,
// which is a cached copy of a servable file in remote storage.
type localFile struct {
	Path         string
	ContentType  string
	ETag         string
	LastModified time.Time
}

// StorageClient is the subset of minio.Client used in this program.
// For testing, struct fakeStorageClient provides a fake implementation.
type storageClient interface {
	ListObjects(ctx context.Context, bucketName string, opts minio.ListObjectsOptions) <-chan minio.ObjectInfo
	FGetObject(ctx context.Context, bucketName, objectName, filePath string, opts minio.GetObjectOptions) error
}

// NewStorage sets up a client for accessing S3-compatible object storage.
func NewStorage(keypath, workdir string) (*Storage, error) {
	if err := os.MkdirAll(workdir, 0755); err != nil {
		return nil, err
	}

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

	client.SetAppInfo("QRankWebserver", "0.1")
	return &Storage{
		client:  client,
		workdir: workdir,
		files:   make(map[string]*localFile, 10),
	}, nil
}

var objRegexp = regexp.MustCompile(`public/([a-z\-]+)\-(2[0-9]{7})\.([a-z0-9\.]+)`)

// Reload caches public content from remote object storage to local disk.
// Any old content (which is not live anymore) is deleted from local disk.
func (s *Storage) Reload(ctx context.Context) error {
	// Find the most recent version of each file in storage.
	objects := s.client.ListObjects(ctx, "qrank", minio.ListObjectsOptions{
		Prefix:    "public/",
		Recursive: false,
	})
	inStorage := make(map[string]minio.ObjectInfo, 5)
	for obj := range objects {
		if m := objRegexp.FindStringSubmatch(obj.Key); m != nil {
			filename := fmt.Sprintf("%s.%s", m[1], m[3])
			info := inStorage[filename]
			if obj.LastModified.After(info.LastModified) {
				inStorage[filename] = obj
			}
		}
	}

	files := make(map[string]*localFile, len(inStorage))
	for filename, obj := range inStorage {
		mangled := base32.HexEncoding.EncodeToString([]byte(obj.ETag))
		path, err := filepath.Abs(filepath.Join(
			s.workdir,
			fmt.Sprintf("%s-%s", mangled, filename)))
		if err != nil {
			return err
		}
		if _, err := os.Stat(path); err != nil {
			tmpPath := path + ".tmp"
			if err := s.client.FGetObject(ctx, "qrank", obj.Key, tmpPath, minio.GetObjectOptions{}); err != nil {
				return err
			}
			if err := os.Chtimes(tmpPath, time.Now(), obj.LastModified); err != nil {
				return err
			}
			if err := os.Rename(tmpPath, path); err != nil {
				return err
			}
		}

		loc := &localFile{
			LastModified: obj.LastModified.UTC(),
			ContentType:  "application/octet-stream",
			ETag:         obj.ETag,
			Path:         path,
		}

		switch filepath.Ext(filename) {
		case ".gz":
			loc.ContentType = "application/gzip"
		case ".json":
			loc.ContentType = "application/json"
		case ".tiff":
			loc.ContentType = "image/tiff"
		case ".txt":
			loc.ContentType = "text/plain"
		}

		files[filename] = loc
	}

	live := make(map[string]bool, len(files))
	for _, f := range files {
		live[f.Path] = true
	}

	s.mutex.Lock()
	s.files = files
	s.mutex.Unlock()

	// Clean up workdir so it only contains live files. If we have a new
	// version for a file that is still getting served to an in-flight
	// request, it’s not a problem: In Linux, it is perfectly fine to
	// delete (unlink) a file while there’s still open file handles.
	// The file handle will remain open and can be used for reading;
	// the underlying file only gets deleted once there’s no open handles
	// anymore.
	ff, err := os.ReadDir(s.workdir)
	if err != nil {
		return err
	}
	for _, f := range ff {
		fp, err := filepath.Abs(filepath.Join(s.workdir, f.Name()))
		if err != nil {
			return err
		}
		if !live[fp] {
			msg := fmt.Sprintf("Deleting obsolete local file: %s\n", fp)
			log.Println(msg)
			fmt.Println(msg)
			if err := os.Remove(fp); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *Storage) Watch(ctx context.Context) error {
	ticker := time.NewTicker(30 * time.Second)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := s.Reload(ctx); err != nil {
				if err == ctx.Err() {
					return err
				} else {
					log.Println(err)
				}
			}
		}
	}
}

type Content struct {
	f            *os.File
	ContentType  string
	ETag         string
	LastModified time.Time
}

func (c *Content) Read(p []byte) (int, error) {
	return c.f.Read(p)
}

func (c *Content) Seek(offset int64, whence int) (int64, error) {
	return c.f.Seek(offset, whence)
}

func (c *Content) Close() error {
	return c.f.Close()
}

func (s *Storage) Retrieve(filename string) (*Content, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	loc, found := s.files[filename]
	if !found {
		return nil, fmt.Errorf("not found")
	}

	f, err := os.Open(loc.Path)
	if err != nil {
		return nil, err
	}

	c := &Content{
		f:            f,
		ContentType:  loc.ContentType,
		ETag:         loc.ETag,
		LastModified: loc.LastModified,
	}
	return c, nil
}
