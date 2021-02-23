package main

import (
	"bufio"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/dsnet/compress/bzip2"
)

func TestFindEntitiesDump(t *testing.T) {
	dumpsDir := t.TempDir()
	dir := filepath.Join(dumpsDir, "wikidatawiki", "entities")
	if err := os.MkdirAll(filepath.Join(dir, "20250215"), 0755); err != nil {
		t.Error(err)
		return
	}

	dumpPath := filepath.Join(dir, "20250215", "wikidata-20250215-all.json.bz2")
	if f, err := os.Create(dumpPath); err == nil {
		f.Close()
	} else {
		t.Error(err)
		return
	}

	err := os.Symlink(filepath.Join("20250215", "wikidata-20250215-all.json.bz2"),
		filepath.Join(dir, "latest-all.json.bz2"))
	if err != nil {
		t.Error(err)
		return
	}

	expectedPath := filepath.Join(dir, "20250215", "wikidata-20250215-all.json.bz2")
	date, path, err := findEntitiesDump(dumpsDir)
	if err != nil {
		t.Error(err)
		return
	}

	if d := date.Format("2006-01-02"); d != "2025-02-15" {
		t.Errorf("expected 2025-02-15, got %s", d)
	}

	got, _ := os.Stat(path)
	expected, _ := os.Stat(expectedPath)
	if !os.SameFile(expected, got) {
		t.Errorf("expected %q, got %q", expectedPath, path)
	}
}

func BenchmarkProcessEntity(b *testing.B) {
	data, err := readTestEntities("testdata/twenty_entities.json.bz2")
	if err != nil {
		b.Error(err)
		return
	}

	ch := make(chan string, 1000)
	defer close(ch)
	go func() {
		for _ = range ch {
		}
	}()

	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		if err := processEntity(data[i%len(data)], ch, ctx); err != nil {
			b.Error(err)
			return
		}
	}
}

func readTestEntities(path string) ([][]byte, error) {
	data := make([][]byte, 0, 20)
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader, err := bzip2.NewReader(file, &bzip2.ReaderConfig{})
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	scanner := bufio.NewScanner(reader)
	maxLineSize := 1 * 1024 * 1024
	scanner.Buffer(make([]byte, maxLineSize), maxLineSize)
	for scanner.Scan() {
		buf := scanner.Bytes()
		if len(buf) > 3 { // leading '[', trailing ']' line in file
			if buf[len(buf)-1] == ',' {
				buf = buf[0 : len(buf)-1]
			}
			data = append(data, buf)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return data, nil
}
