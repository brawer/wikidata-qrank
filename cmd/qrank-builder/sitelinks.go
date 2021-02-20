package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/dsnet/compress/bzip2"
)

func readSitelinks(path string, ctx context.Context) error {
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
		if numLines%10000 == 0 {
			log.Printf("Read %d entities", numLines)
		}
		if err := extractSitelinks(buf, ctx); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	fmt.Printf("Read %d lines\n", numLines)

	return nil
}

func extractSitelinks(data []byte, ctx context.Context) error {
	var e struct {
		Id        string
		Sitelinks map[string]struct{ Title string }
	}
	if err := json.Unmarshal(data, &e); err != nil {
		return err
	}
	for key, val := range e.Sitelinks {
		site := strings.Replace(key, "wiki", ".wiki", 1)

		fmt.Printf("*** GIRAFFE %s\n", formatLine(site, val.Title, e.Id))
	}
	return nil
}
