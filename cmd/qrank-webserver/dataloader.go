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
	path          string
	mutex         sync.Mutex
	statsFilename string
	stats         Stats
	qrankFile     *os.File
}

type Stats struct {
	QRankFilename string `json:"qrank-filename"`
	QRankSha256   string `json:"qrank-sha256"`
}

func NewDataLoader(path string) (*DataLoader, error) {
	dl := &DataLoader{path: path}
	if err := dl.Reload(); err != nil {
		return nil, err
	}
	return dl, nil
}

func (dl *DataLoader) Get() (Stats, *os.File) {
	dl.mutex.Lock()
	defer dl.mutex.Unlock()
	return dl.stats, dl.qrankFile
}

func (dl *DataLoader) Reload() error {
	files, err := os.ReadDir(dl.path)
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
		return fmt.Errorf("no stats-YYYYMMDD.json files in %s", dl.path)
	}

	statFile, err := os.Open(filepath.Join(dl.path, latest))
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

	qrankFile, err := os.Open(filepath.Join(dl.path, stats.QRankFilename))
	if err != nil {
		return err
	}
	dl.statsFilename = latest
	dl.stats = stats
	if dl.qrankFile != nil {
		dl.qrankFile.Close()
	}
	dl.qrankFile = qrankFile

	return nil
}
