package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type DataLoader struct {
	// Directory being watched. Does not change while server is running.
	Path string

	mutex         sync.Mutex
	statsFilename string
	stats         Stats
}

type Stats struct {
	QRankFilename string `json:"qrank-filename"`
	QRankSha256   string `json:"qrank-sha256"`
}

func NewDataLoader(path string) (*DataLoader, error) {
	dl := &DataLoader{Path: path}
	if err := dl.Reload(); err != nil {
		return nil, err
	}
	return dl, nil
}

func (dl *DataLoader) Get() Stats {
	dl.mutex.Lock()
	defer dl.mutex.Unlock()
	return dl.stats
}

func (dl *DataLoader) Reload() error {
	files, err := os.ReadDir(dl.Path)
	if err != nil {
		return err
	}

	var latest string
	for _, f := range files {
		name := f.Name()
		if strings.HasPrefix(name, "stats-") && strings.HasSuffix(name, ".json") {
			if name > latest {
				latest = name
			}
		}
	}

	if len(latest) == 0 {
		return fmt.Errorf("no stats-YYYYMMDD.json files in %s", dl.Path)
	}

	statFile, err := os.Open(filepath.Join(dl.Path, latest))
	if err != nil {
		return err
	}
	defer statFile.Close()

	statsBuf, err := io.ReadAll(statFile)
	if err != nil {
		return err
	}

	var stats Stats
	if err := json.Unmarshal(statsBuf, &stats); err != nil {
		return err
	}

	dl.mutex.Lock()
	defer dl.mutex.Unlock()
	if dl.statsFilename == latest {
		return nil
	}

	dl.statsFilename = latest
	dl.stats = stats

	return nil
}
