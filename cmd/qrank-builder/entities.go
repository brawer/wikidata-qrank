package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"

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

func processEntities(testRun bool, path string, date time.Time, outDir string, ctx context.Context) (string, error) {
	year, month, day := date.Year(), date.Month(), date.Day()
	sitelinksPath := filepath.Join(
		outDir,
		fmt.Sprintf("sitelinks-%04d-%02d-%02d.bz2", year, month, day))
	_, err := os.Stat(sitelinksPath)
	if err == nil {
		return sitelinksPath, nil // use pre-existing file
	}
	if !os.IsNotExist(err) {
		return "", err
	}

	logger.Printf("processing entities of %04d-%02d", year, month)
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

	bzConfig := bzip2.WriterConfig{Level: bzip2.BestCompression}
	sitelinksWriter, err := bzip2.NewWriter(tmpSitelinksFile, &bzConfig)
	if err != nil {
		return "", err
	}
	defer sitelinksWriter.Close()

	ch := make(chan string, 10000)
	config := extsort.DefaultConfig()
	config.ChunkSize = 4 * 1024 * 1024 / 16 // 4 MiB, 16 Bytes/line avg
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

	reader, err := bzip2.NewReader(file, &bzip2.ReaderConfig{})
	if err != nil {
		return err
	}
	defer reader.Close()

	numLines := 0
	scanner := bufio.NewScanner(reader)
	maxLineSize := 8 * 1024 * 1024
	scanner.Buffer(make([]byte, maxLineSize), maxLineSize)
	for scanner.Scan() {
		numLines += 1
		buf := scanner.Bytes()
		if len(buf) < 10 {
			continue
		}
		if buf[len(buf)-1] == ',' {
			buf = buf[0 : len(buf)-1]
		}
		if testRun && numLines >= 100 {
			break
		}
		if err := processEntity(buf, sitelinks, ctx); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	logger.Printf("Processed %d entities", numLines)

	return nil
}

func processEntity(data []byte, sitelinks chan<- string, ctx context.Context) error {
	// Unoptimized: 228 μs/op [Intel i9-9880H, 2.3GHz]
	// var e struct {
	//	Id        string
	//	Sitelinks map[string]struct{ Title string }
	// }
	// json.Unmarshal(data, &e)

	// Optimized: 3 μs/op [Intel i9-9880H, 2.3GHz]
	// The optimized code is really ugly, but it seems to be worth it.
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

			// Rewrite "enwiki" to "en.wiki", "alswikinews"
			// to "als.wikinews", etc. To save (many) buffer allocations,
			// we do this in place.
			wiki := []byte("wiki")
			var site string
			if bytes.HasSuffix(data[siteStart:siteStart+siteLen], wiki) &&
				data[siteStart+siteLen-4] != '.' {
				p := siteStart + siteLen - 4
				data[p] = '.'
				data[p+1] = 'w'
				data[p+2] = 'i'
				data[p+3] = 'k'
				data[p+4] = 'i'
				site = string(data[siteStart : siteStart+siteLen+1])
			} else {
				site = string(data[siteStart : siteStart+siteLen])
			}
			if site == "commons.wiki" {
				site = "commons.wikimedia" // as in pageviews
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
				case sitelinks <- formatLine(site, title, id):
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
