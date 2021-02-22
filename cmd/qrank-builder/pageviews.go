package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"golang.org/x/sync/errgroup"

	"github.com/dsnet/compress/bzip2"
	"github.com/lanrat/extsort"
)

func buildPageviews(dumpsPath string, date time.Time, ctx context.Context) ([]string, error) {
	paths := make([]string, 0, 12)
	for i := 1; i <= 12; i++ {
		m := date.AddDate(0, -i, 0)
		path, err := buildMonthlyPageviews(dumpsPath, m.Year(), m.Month(), ctx)
		if err != nil {
			return nil, err
		}
		paths = append(paths, path)
		break // TODO: Remove
	}
	return paths, nil
}

func buildMonthlyPageviews(dumpsPath string, year int, month time.Month, ctx context.Context) (string, error) {
	outDir := "cache"
	if _, err := os.Stat(outDir); os.IsNotExist(err) {
		err = os.Mkdir(outDir, 0755)
		if err != nil {
			return "", err
		}
	} else if err != nil {
		return "", err
	}

	outPath := filepath.Join(
		outDir,
		fmt.Sprintf("pageviews-%04d-%02d.bz2", year, month))
	_, err := os.Stat(outPath)
	if err == nil {
		return outPath, nil // use pre-existing file
	}
	if !os.IsNotExist(err) {
		return "", err
	}

	logger.Printf("building monthly pageviews for %04d-%02d", year, month)
	start := time.Now()

	// We write our output into a temp file in the same directory
	// as the final location, and then rename it atomically at the
	// very end. This ensures we don't end up with incomplete data
	// (which would be preserved across runs) in case of crashes.
	tmpPath := outPath + ".tmp"
	tmpFile, err := os.Create(tmpPath)
	if err != nil {
		return "", err
	}
	defer tmpFile.Close()

	bzConfig := bzip2.WriterConfig{Level: bzip2.BestCompression}
	writer, err := bzip2.NewWriter(tmpFile, &bzConfig)
	if err != nil {
		return "", err
	}
	defer writer.Close()

	ch := make(chan string, 10000)
	config := extsort.DefaultConfig()
	config.ChunkSize = 8 * 1024 * 1024 / 64 // 8 MiB, 64 Bytes/line avg
	sorter, outChan, errChan := extsort.Strings(ch, config)

	g, subCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return readMonthlyPageviews(dumpsPath, year, month, ch, subCtx)
	})
	g.Go(func() error {
		sorter.Sort(subCtx)
		if err := combineCounts(outChan, writer, subCtx); err != nil {
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

	if err := writer.Close(); err != nil {
		return "", err
	}

	if err := tmpFile.Close(); err != nil {
	}

	if err := os.Rename(tmpPath, outPath); err != nil {
		return "", err
	}

	logger.Printf("built monthly pageviews for %04d-%02d in %.1fs",
		year, month, time.Since(start).Seconds())
	return outPath, nil
}

func combineCounts(ch <-chan string, w io.Writer, ctx context.Context) error {
	var lastKey string
	var lastCount int64
	for {
		select {
		case line, ok := <-ch:
			if !ok { // channel closed, end of input
				return writeCount(w, lastKey, lastCount)
			}
			cols := strings.Fields(line)
			key := cols[0]
			count, err := strconv.ParseInt(cols[1], 10, 64)
			if err != nil {
				return err
			}
			if key == lastKey {
				lastCount += count
			} else {
				err := writeCount(w, lastKey, lastCount)
				if err != nil {
					return err
				}
				lastKey, lastCount = key, count
			}

		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

func writeCount(w io.Writer, key string, count int64) error {
	if count <= 0 {
		return nil
	}

	var buf bytes.Buffer
	buf.Grow(len(key) + 16)
	if _, err := buf.WriteString(key); err != nil {
		return err
	}
	if err := buf.WriteByte(' '); err != nil {
		return err
	}
	if _, err := buf.WriteString(strconv.FormatInt(count, 10)); err != nil {
		return err
	}
	if err := buf.WriteByte('\n'); err != nil {
		return err
	}

	if _, err := buf.WriteTo(w); err != nil {
		return err
	}
	return nil
}

func readMonthlyPageviews(dumpsPath string, year int, month time.Month, ch chan<- string, ctx context.Context) error {
	defer close(ch)

	g, subCtx := errgroup.WithContext(ctx)
	t := time.Date(year, month, 1, 0, 0, 0, 0, time.UTC)
	numDays := t.AddDate(0, 1, -1).Day()
	for day := 1; day <= numDays; day++ {
		filename := fmt.Sprintf("pageviews-%04d%02d%02d-user.bz2",
			year, month, day)
		path := filepath.Join(
			dumpsPath, "other", "pageview_complete",
			fmt.Sprintf("%04d", year),
			fmt.Sprintf("%04d-%02d", year, month),
			filename)
		g.Go(func() error {
			return readPageviewsFile(path, ch, subCtx)
		})
	}

	return g.Wait()
}

func readPageviewsFile(path string, ch chan<- string, ctx context.Context) error {
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

	return readPageviews(reader, ch, ctx)
}

func readPageviews(reader io.Reader, ch chan<- string, ctx context.Context) error {
	scanner := bufio.NewScanner(reader)
	var lastSite, lastTitle string
	var lastCount int64
	n := 0
	for scanner.Scan() {
		n++
		//if n == 500 {
		//	break
		//}

		cols := strings.Fields(scanner.Text())
		if len(cols) != 6 {
			continue
		}

		site := cols[0]
		if strings.HasSuffix(site, ".wikipedia") {
			site = site[:len(site)-5]
		}

		// Some, but not all, queryies are urlescaped.
		// Try to unescape, but fall back to raw query
		// if the syntax is invalid.
		title, err := url.QueryUnescape(cols[1])
		if err != nil {
			title = cols[1]
		}

		if !utf8.ValidString(title) {
			continue
		}

		c, err := strconv.ParseInt(cols[4], 10, 64)
		if err != nil {
			continue
		}

		if site == lastSite && title == lastTitle {
			lastCount += c
		} else {
			if err := emitPageviews(lastSite, lastTitle, lastCount, ch, ctx); err != nil {
				return err
			}
			lastSite = site
			lastTitle = title
			lastCount = c
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if err := emitPageviews(lastSite, lastTitle, lastCount, ch, ctx); err != nil {
		return err
	}
	return nil
}

func emitPageviews(site, title string, count int64, ch chan<- string, ctx context.Context) error {
	if count > 0 {
		line := formatLine(site, title, strconv.FormatInt(count, 10))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ch <- line:
		}
	}
	return nil
}
