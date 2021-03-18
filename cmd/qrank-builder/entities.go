// SPDX-License-Identifier: MIT

package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/andybalholm/brotli"
	"github.com/dsnet/compress/bzip2"
	"github.com/lanrat/extsort"
)

func findEntitiesDump(dumpsPath string) (time.Time, string, error) {
	path := filepath.Join(dumpsPath, "wikidatawiki", "entities", "latest-all.json.bz2")
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return time.Time{}, "", err
	}

	parts := strings.Split(resolved, string(os.PathSeparator))
	date, err := time.Parse("20060102", parts[len(parts)-2])
	if err != nil {
		return time.Time{}, "", err
	}

	// The symlink can change any time on the file system, such as
	// when Wikimedia generates a new dump right between the call
	// to EvalSymlinks() and our client opening the returned path.
	// To avoid this race condition, we need to return the resolved
	// path here, not the symlink.
	return date, resolved, nil
}

type WikidataSplit struct {
	Start int64  // Position of compression block in bzip2 file.
	Limit string // First entity coming after the current split.
}

func SplitWikidataDump(r io.ReaderAt, size int64, numSplits int) ([]WikidataSplit, error) {
	type SplitPoint struct {
		Start       int64
		FirstEntity string
	}
	splits := make([]SplitPoint, 0, numSplits)
	for i := 0; i < numSplits; i++ {
		off := int64(i) * size / int64(numSplits)
		start, entity, err := findEntitySplit(r, off)
		if err != nil {
			return nil, err
		}
		splits = append(splits, SplitPoint{start, entity})
	}
	result := make([]WikidataSplit, len(splits))
	for i, split := range splits {
		result[i].Start = split.Start
		if i < len(splits)-1 {
			result[i].Limit = splits[i+1].FirstEntity
		} else {
			result[i].Limit = "*"
		}
	}
	return result, nil
}

func findEntitySplit(r io.ReaderAt, off int64) (int64, string, error) {
	// We look for the magic six bytes that indicate the start
	// of a bzip2 compression block. There is no guarantee that these
	// bytes do not appear in the compressed stream, although it is
	// quite unlikely; that's why we decode the stream and extract the
	// first complete line. Another complication stems from our file
	// access in chunks of 32 KiByte; the magic sequence could span
	// a chunk boundary. To handle this corner case, we copy the
	// last six bytes of the chunk to the beginning of the chunk
	// buffer; this allows us to catch chunk-spanning magic sequences.
	chunk := make([]byte, 6+32*1024) // default value is all zeroes
	chunkLen := len(chunk)
	for {
		if _, err := r.ReadAt(chunk[6:chunkLen], off); err != nil {
			return 0, "", err
		}
		magic := []byte{0x31, 0x41, 0x59, 0x26, 0x53, 0x59} // π
		pos := bytes.Index(chunk, magic)
		if pos < 0 {
			copy(chunk[0:6], chunk[chunkLen-6:chunkLen])
			off += int64(chunkLen - 6)
			continue
		}

		// We might have found a *potential* bzip2 block, but
		// we can't be sure because the magic six bytes could
		// also appear in the middle of block. To be sure,
		// let's read something from the block. If we get
		// a bzip2 decompression error, our speculation was
		// wrong; in that case, try again. We advance `off`
		// after the detected potential blockStart, otherwise
		// we don't terminate.
		off = off + int64(pos)
		blockStart := off - 6
		reader, err := NewBzip2ReaderAt(r, blockStart, 1*1024*1024)
		if err != nil {
			continue
		}

		scanner := bufio.NewScanner(reader)
		maxLineSize := 8 * 1024 * 1024
		scanner.Buffer(make([]byte, maxLineSize), maxLineSize)
		scanner.Scan()
		scanner.Scan()
		err = scanner.Err()
		if err != nil && strings.HasPrefix(err.Error(), "bzip2: corrupted input") {
			// Sadly, the github.com/dsnet/compress/bzip2 keeps
			// its error class private, so we cannot check this
			// condition via errors.Is(). At least it is checked
			// in our unit tests.
			continue
		}
		if err != nil {
			return 0, "", err
		}

		line := scanner.Text()
		if strings.HasPrefix(line, `{"type":"item","id":"`) {
			if p := strings.IndexByte(line[21:len(line)], '"'); p > 0 {

				return blockStart, line[21 : 21+p], nil
			}
		}
	}
}

func NewBzip2ReaderAt(r io.ReaderAt, off int64, size int64) (io.Reader, error) {
	header := strings.NewReader("BZh9")
	stream := io.NewSectionReader(r, off, size)
	cat := io.MultiReader(header, stream)
	return bzip2.NewReader(cat, &bzip2.ReaderConfig{})
}

func processEntities(testRun bool, path string, date time.Time, outDir string, ctx context.Context) (string, error) {
	year, month, day := date.Year(), date.Month(), date.Day()
	sitelinksPath := filepath.Join(
		outDir,
		fmt.Sprintf("sitelinks-%04d%02d%02d.br", year, month, day))
	_, err := os.Stat(sitelinksPath)
	if err == nil {
		return sitelinksPath, nil // use pre-existing file
	}
	if !os.IsNotExist(err) {
		return "", err
	}

	logger.Printf("processing entities of %04d-%02d-%d", year, month, day)
	start := time.Now()

	// We write our output into a temp file in the same directory
	// as the final location, and then rename it atomically at the
	// very end. This ensures we don't end up with incomplete data
	// (which would be preserved across runs) in case of crashes.
	tmpSitelinksPath := sitelinksPath + ".tmp"
	tmpSitelinksFile, err := os.Create(tmpSitelinksPath)
	if err != nil {
		return "", err
	}
	defer tmpSitelinksFile.Close()

	sitelinksWriter := brotli.NewWriterLevel(tmpSitelinksFile, 6)
	defer sitelinksWriter.Close()

	ch := make(chan string, 10000)
	config := extsort.DefaultConfig()
	config.ChunkSize = 8 * 1024 * 1024 / 16 // 8 MiB, 16 Bytes/line avg
	config.NumWorkers = runtime.NumCPU()
	sorter, outChan, errChan := extsort.Strings(ch, config)
	g, subCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return readEntities(testRun, path, ch, subCtx)
	})
	g.Go(func() error {
		sorter.Sort(subCtx)
		if err := writeSitelinks(outChan, sitelinksWriter, subCtx); err != nil {
			return err
		}
		return nil
	})
	if err := g.Wait(); err != nil {
		return "", err
	}
	if err := <-errChan; err != nil {
		return "", err
	}

	if err := sitelinksWriter.Close(); err != nil {
	}

	if err := tmpSitelinksFile.Sync(); err != nil {
	}

	if err := tmpSitelinksFile.Close(); err != nil {
	}

	if err := os.Rename(tmpSitelinksPath, sitelinksPath); err != nil {
		return "", err
	}

	logger.Printf("built sitelinks for %04d-%02d-%02d in %.1fs",
		year, month, day, time.Since(start).Seconds())
	return sitelinksPath, nil
}

func readEntities(testRun bool, path string, sitelinks chan<- string, ctx context.Context) error {
	defer close(sitelinks)

	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return err
	}
	fileSize := stat.Size()

	// To keep CPU cores busy while tasks are blocked waiting for input,
	// we use more worker tasks than we have CPUs.
	numSplits := runtime.NumCPU() * 4
	if testRun {
		numSplits = 2
	}
	splits, err := SplitWikidataDump(file, fileSize, numSplits)
	if err != nil {
		return err
	}
	logger.Printf("reading Wikidata dump with %d parallel workers", len(splits))

	work := make(chan WikidataSplit, len(splits))
	for _, split := range splits {
		work <- split
	}
	close(work)

	g, ctx := errgroup.WithContext(ctx)
	for i := 0; i < numSplits; i++ {
		g.Go(func() error {
			for task := range work {
				reader, err := NewBzip2ReaderAt(file, task.Start, fileSize-task.Start)
				if err != nil {
					return err
				}
				if err := readWikidataSplit(reader, testRun, task.Limit, sitelinks, ctx); err != nil {
					return err
				}
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	return nil
}

func readWikidataSplit(reader io.Reader, testRun bool, limit string, sitelinks chan<- string, ctx context.Context) error {
	numLines := 0
	scanner := bufio.NewScanner(reader)
	maxLineSize := 8 * 1024 * 1024
	scanner.Buffer(make([]byte, maxLineSize), maxLineSize)
	for scanner.Scan() {
		numLines += 1
		buf := scanner.Bytes()
		bufLen := len(buf)
		if bufLen == 1 && buf[0] == '[' { // first line in dump
			continue
		}

		// If we reach the last line in the dump, we stop reading.
		// This prevents the bzip2 decoder from checking the stream
		// checksum, which will not match when we split the bzip2
		// stream for parallel processing.
		if bufLen == 1 && buf[0] == ']' {
			break
		}

		if buf[bufLen-1] == ',' {
			buf = buf[0 : bufLen-1]
		}
		if testRun && numLines >= 1000 {
			break
		}
		if err := processEntity(buf, limit, sitelinks, ctx); err != nil {
			if err == limitReached {
				return nil
			}
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}

	return nil
}

var limitReached error = errors.New("limit reached")

func processEntity(data []byte, limitID string, sitelinks chan<- string, ctx context.Context) error {
	// Unoptimized: 228 μs/op [Intel i9-9880H, 2.3GHz]
	// var e struct {
	//	Id        string
	//	Sitelinks map[string]struct{ Title string }
	// }
	// json.Unmarshal(data, &e)

	// Optimized: 21.9 μs/op [Intel i9-9880H, 2.3GHz]
	// The optimized code is really ugly, but it seems to be worth it.
	// An earlier version only needed 3 μs/op, but it had bugs.
	// If you ever need to micro-optimize further, please have a look
	// at the revision history in source control.
	limit := len(data)

	var id string
	idStart := bytes.Index(data, []byte(`,"id":"Q`))
	if idStart > 0 {
		idStart = idStart + 7
		idLen := bytes.IndexByte(data[idStart:limit], '"')
		if idLen >= 2 && idLen < 25 {
			id = string(data[idStart : idStart+idLen])
		}
	}
	if id == limitID {
		return limitReached
	}

	// Sitelinks typically start at around 90% into the data buffer.
	// This optimization saves about 9 μs/op [Intel i9-9880H, 2.3GHz].
	guess := (limit * 7) / 8
	slStart := bytes.Index(data[guess:limit], []byte(`,"sitelinks":{`))
	if slStart > 0 {
		slStart += guess
	} else {
		slStart = bytes.Index(data, []byte(`,"sitelinks":{`))
	}

	if slStart >= 0 {
		pos := slStart + 15 // Scan past ,"sitelinks":{"
		for {
			siteStart := bytes.Index(data[pos:limit], []byte(`":{"site":"`))
			if siteStart < 0 {
				break
			}
			siteStart += pos + 11
			siteLen := bytes.IndexByte(data[siteStart:limit], '"')
			if siteLen < 2 || siteLen > 50 {
				break
			}

			siteBytes := data[siteStart : siteStart+siteLen]
			wikiPos := bytes.Index(siteBytes, []byte("wiki"))
			if wikiPos < 0 {
				break
			}

			lang := string(data[siteStart : siteStart+wikiPos])
			site := string(data[siteStart+wikiPos : siteStart+siteLen])
			if site == "wiki" {
				site = "wikipedia"
			}

			titleStart := siteStart + siteLen + 11
			titleLen := bytes.IndexByte(data[titleStart:limit], '"')
			if titleLen < 1 || titleLen > 5000 {
				break
			}

			title, ok := unquote(data[titleStart-1 : titleStart+titleLen+1])
			pos = titleStart + titleLen

			if ok {
				select {
				case sitelinks <- formatLine(lang, site, title, id):
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}
	}
	return nil
}

func writeSitelinks(ch <-chan string, w io.Writer, ctx context.Context) error {
	for {
		select {
		case line, ok := <-ch:
			if !ok { // channel closed, end of input
				return nil
			}
			if _, err := w.Write([]byte(line)); err != nil {
				return err
			}
			if _, err := w.Write([]byte{'\n'}); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
