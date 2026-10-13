// SPDX-FileCopyrightText: 2024 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"encoding/base32"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/brawer/wikidata-qrank/v2/internal/version"
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

	// Canonical is the dated name of the file, such as
	// "qrank-20261004.csv.gz", if it is served under an undated alias
	// such as "qrank.csv.gz". Empty for files served under their own name.
	Canonical string

	// Immutable is true for files served under a dated name, whose
	// content never changes.
	Immutable bool
}

// StorageClient is the subset of minio.Client used in this program.
// For testing, struct fakeStorageClient provides a fake implementation.
type storageClient interface {
	ListObjects(ctx context.Context, bucketName string, opts minio.ListObjectsOptions) <-chan minio.ObjectInfo
	FGetObject(ctx context.Context, bucketName, objectName, filePath string, opts minio.GetObjectOptions) error
}

// NewStorage sets up a client for accessing S3-compatible object storage.
func NewStorage(workdir string) (*Storage, error) {
	if err := os.MkdirAll(workdir, 0755); err != nil {
		return nil, err
	}

	var config struct{ Endpoint, Key, Secret string }
	config.Endpoint = os.Getenv("S3_ENDPOINT")
	config.Key = os.Getenv("S3_KEY")
	config.Secret = os.Getenv("S3_SECRET")

	client, err := minio.New(config.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(config.Key, config.Secret, ""),
		Secure: true,
	})
	if err != nil {
		return nil, err
	}

	client.SetAppInfo("QRankWebserver", strings.TrimPrefix(ServerVersion, version.Base+"/"))
	return &Storage{
		client:  client,
		workdir: workdir,
		files:   make(map[string]*localFile, 10),
	}, nil
}

var objRegexp = regexp.MustCompile(`^public/([a-z_\-]+)\-(2[0-9]{7})\.([a-z0-9\.]+)$`)

// DatapackageKey is where the pipeline stores the Frictionless data
// package that describes the current version of the published files.
const datapackageKey = "public/datapackage.json"

// DatedKinds are the files that get served under their dated name, such
// as "qrank-20261004.csv.gz", in addition to their undated alias. Other
// files, such as the (much larger) item signals, are only served under
// their alias, which saves disk space in the working directory.
var datedKinds = map[string]bool{
	"qrank.csv.gz":     true,
	"qrank-stats.json": true,
	"qrank.cdx.json":   true,
}

var isoDateRegexp = regexp.MustCompile(`^(\d{4})-(\d{2})-(\d{2})$`)

// Reload caches public content from remote object storage to local disk.
// Any old content (which is not live anymore) is deleted from local disk.
//
// Each dated object, such as public/qrank-20261004.csv.gz, is served
// under an undated alias (qrank.csv.gz). The alias points to the version
// named in public/datapackage.json, so the alias and the data package
// always agree. If there is no data package, or it does not name a
// version that exists in storage, the alias points to the most recently
// modified object. Some kinds of files are also served under their
// dated name; see datedKinds.
func (s *Storage) Reload(ctx context.Context) error {
	objects := s.client.ListObjects(ctx, "qrank", minio.ListObjectsOptions{
		Prefix:    "public/",
		Recursive: false,
	})

	// Group the dated objects in storage by their undated alias.
	// For example, inStorage["qrank.csv.gz"]["20261004"] is the
	// object for public/qrank-20261004.csv.gz.
	inStorage := make(map[string]map[string]minio.ObjectInfo, 5)
	var datapackage *minio.ObjectInfo
	for obj := range objects {
		if obj.Err != nil {
			return obj.Err
		}
		if obj.Key == datapackageKey {
			datapackage = &obj
			continue
		}
		if m := objRegexp.FindStringSubmatch(obj.Key); m != nil {
			alias := fmt.Sprintf("%s.%s", m[1], m[3])
			if inStorage[alias] == nil {
				inStorage[alias] = make(map[string]minio.ObjectInfo, 3)
			}
			inStorage[alias][m[2]] = obj
		}
	}

	files := make(map[string]*localFile, len(inStorage)*4+1)
	pkgVersion := ""
	if datapackage != nil {
		loc, err := s.fetch(ctx, *datapackage)
		if err != nil {
			return err
		}
		files["datapackage.json"] = loc
		pkgVersion, err = readDatapackageVersion(loc.Path)
		if err != nil {
			log.Printf("ignoring %s: %v", datapackageKey, err)
		}
	}

	for alias, versions := range inStorage {
		current, found := versions[pkgVersion]
		if !found {
			for _, obj := range versions {
				if obj.LastModified.After(current.LastModified) {
					current = obj
				}
			}
		}

		for _, obj := range versions {
			if obj.Key != current.Key && !datedKinds[alias] {
				continue
			}
			loc, err := s.fetch(ctx, obj)
			if err != nil {
				return err
			}
			name := path.Base(obj.Key)
			if datedKinds[alias] {
				dated := *loc
				dated.Immutable = true
				files[name] = &dated
			}
			if obj.Key == current.Key {
				// Only point clients to a dated name that we also serve.
				if datedKinds[alias] {
					loc.Canonical = name
				}
				files[alias] = loc
			}
		}
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

// Fetch makes sure that a remote object is cached in the working directory,
// downloading it if necessary.
func (s *Storage) fetch(ctx context.Context, obj minio.ObjectInfo) (*localFile, error) {
	name := path.Base(obj.Key)
	mangled := base32.HexEncoding.EncodeToString([]byte(obj.ETag))
	p, err := filepath.Abs(filepath.Join(s.workdir, fmt.Sprintf("%s-%s", mangled, name)))
	if err != nil {
		return nil, err
	}
	if _, err := os.Stat(p); err != nil {
		tmpPath := p + ".tmp"
		if err := s.client.FGetObject(ctx, "qrank", obj.Key, tmpPath, minio.GetObjectOptions{}); err != nil {
			return nil, err
		}
		if err := os.Chtimes(tmpPath, time.Now(), obj.LastModified); err != nil {
			return nil, err
		}
		if err := os.Rename(tmpPath, p); err != nil {
			return nil, err
		}
	}

	return &localFile{
		LastModified: obj.LastModified.UTC(),
		ContentType:  contentType(name),
		ETag:         obj.ETag,
		Path:         p,
	}, nil
}

// ContentType returns the media type for a file name.
func contentType(name string) string {
	switch {
	case strings.HasSuffix(name, ".cdx.json"):
		return "application/vnd.cyclonedx+json"
	case strings.HasSuffix(name, ".gz"):
		return "application/gzip"
	case strings.HasSuffix(name, ".json"):
		return "application/json"
	case strings.HasSuffix(name, ".tiff"):
		return "image/tiff"
	case strings.HasSuffix(name, ".txt"):
		return "text/plain"
	case strings.HasSuffix(name, ".zst"):
		return "application/zstd"
	default:
		return "application/octet-stream"
	}
}

// ReadDatapackageVersion returns the version of a Frictionless data package
// as YYYYMMDD, given a file whose "version" is an ISO date (YYYY-MM-DD).
func readDatapackageVersion(p string) (string, error) {
	data, err := os.ReadFile(p)
	if err != nil {
		return "", err
	}
	var dp struct {
		Version string `json:"version"`
	}
	if err := json.Unmarshal(data, &dp); err != nil {
		return "", err
	}
	m := isoDateRegexp.FindStringSubmatch(dp.Version)
	if m == nil {
		return "", fmt.Errorf("version %q is not an ISO date", dp.Version)
	}
	return m[1] + m[2] + m[3], nil
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
	Canonical    string // dated name if served under an alias, else ""
	Immutable    bool   // whether served under a dated name
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
		Canonical:    loc.Canonical,
		Immutable:    loc.Immutable,
	}
	return c, nil
}
