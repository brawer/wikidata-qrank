// SPDX-License-Identifier: MIT

package main

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
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

func TestNewBzip2ReaderAt(t *testing.T) {
	var buf bytes.Buffer
	buf.WriteString("Junk")
	bz, _ := bzip2.NewWriter(&buf, &bzip2.WriterConfig{Level: 9})
	bz.Write([]byte("Hello World!"))
	bz.Close()

	bufReader := bytes.NewReader(buf.Bytes())
	// 8 bytes prefix: "Junk" written above + "BZh9" file header
	r, err := NewBzip2ReaderAt(bufReader, 8, int64(buf.Len()-8))
	if err != nil {
		t.Error(err)
	}

	decompressed, err := io.ReadAll(r)
	if err != nil {
		t.Error(err)
	}

	got := string(decompressed)
	if got != "Hello World!" {
		t.Errorf("expected \"Hello World!\", got %q", got)
	}
}

func TestFindEntitySplit(t *testing.T) {
	f, err := os.Open("testdata/twenty_entities.json.bz2")
	if err != nil {
		t.Error(err)
	}
	defer f.Close()
	data, _ := io.ReadAll(f)

	var buf bytes.Buffer
	buf.Write(data)
	// Write some junk to make sure magic spans chunk boundary.
	// Our junk contains the BZip2 StreamHeader magic, so we
	// test that we silently ignore bzip2 decoding errors
	// (we have to, since the same byte sequence could also
	// appear inside a compression block).
	junk := make([]byte, 32*1024-(len(data)%(32*1024))+192)
	copy(junk[10:16], []byte{0x31, 0x41, 0x59, 0x26, 0x53, 0x59}) // π
	buf.Write(junk)
	buf.Write(data)
	r := bytes.NewReader(buf.Bytes())

	// First bzip2 BlockStart is at offset 4, after StreamHeader.
	off, entity, err := findEntitySplit(r, 0)
	if err != nil {
		t.Error(err)
	}
	if off != 4 || entity != "Q58921" {
		t.Errorf("expected off=4, entity=\"Q58921\"; got (%d, %s)",
			off, entity)
	}

	// Second bzip2 BlockStart.
	expOff := int64(len(data) + len(junk) + 4)
	off, entity, err = findEntitySplit(r, 200)
	if err != nil {
		t.Error(err)
	}
	if off != expOff || entity != "Q58921" {
		t.Errorf("expected off=%d, entity=\"Q58921\"; got (%d, %s)",
			expOff, off, entity)
	}
}

func TestProcessEntity(t *testing.T) {
	data, err := readTestEntities("testdata/twenty_entities.json.bz2")
	if err != nil {
		t.Error(err)
		return
	}

	got, err := callProcessEntity(data[5], "")
	if err != nil {
		t.Error(err)
		return
	}

	expected := strings.Join([]string{
		"ja.wikipedia/西慶州駅 Q58977",
		"ko.wikipedia/서경주역 Q58977",
		"und.commons/category:seogyeongju_station Q58977",
		"zh.wikipedia/西庆州站 Q58977",
	}, "|")
	if expected != got {
		t.Errorf("expected %q, got %q", expected, got)
	}

	_, err = callProcessEntity(data[5], "Q58977")
	if err != limitReached {
		t.Errorf("expected error limitReached, got %q", err)
	}
}

func TestProcessEntitySpecialSitelinks(t *testing.T) {
	e := []byte(
		`{"type":"item","id":"Q132576","sitelinks":{` +
			`"enwiki":{"site":"enwiki","title":"Impala"},` +
			`"alswikiquote":{"site":"alswikiquote","title":"Impala"},` +
			`"be_x_old_wiki":{"site":"be_x_oldwiki","title":"Імпала"},` + // in Q72
			`"commonswiki":{"site":"commonswiki","title":"Aepyceros melampus"},` +
			`"roa_rupwiki":{"site":"roa_rupwiki","title":"Impala"},` +
			`"simplewiki":{"site":"simplewiki","title":"Impala"},` +
			`"simplewikinews":{"site":"simplewikinews","title":"Impala"},` +
			`"specieswiki":{"site":"specieswiki","title":"Aepyceros melampus"},` +
			`"wikidatawiki":{"site":"wikidatawiki","title":"Project_chat"}` +
			`"zh_classicalwikisource":{"site":"zh_classicalwikisource","title":"Impala"}` +
			`"zh_min_nanwiki":{"site":"zh_min_nanwiki","title":"Impala"}` +
			`}}`)
	got, err := callProcessEntity(e, "")
	if err != nil {
		t.Error(err)
		return
	}
	expected := strings.Join([]string{
		"be-tarask.wikipedia/імпала Q132576",
		"en-x-simple.wikinews/impala Q132576",
		"en-x-simple.wikipedia/impala Q132576",
		"en.wikipedia/impala Q132576",
		"gsw.wikiquote/impala Q132576",
		"lzh.wikisource/impala Q132576",
		"nan.wikipedia/impala Q132576",
		"rup.wikipedia/impala Q132576",
		"und.commons/aepyceros_melampus Q132576",
		"und.wikidata/project_chat Q132576",
		"und.wikispecies/aepyceros_melampus Q132576",
	}, "|")
	if expected != got {
		t.Errorf("expected %s, got %s", expected, got)
	}
}

func callProcessEntity(rec []byte, limit string) (string, error) {
	ch := make(chan string, 20)
	if err := processEntity(rec, limit, ch, context.Background()); err != nil {
		return "", err
	}
	close(ch)
	got := make([]string, 0, 20)
	for s := range ch {
		got = append(got, s)
	}
	sort.Strings(got)
	return strings.Join(got, "|"), nil
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
		if err := processEntity(data[i%len(data)], "", ch, ctx); err != nil {
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
