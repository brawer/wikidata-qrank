// SPDX-FileCopyrightText: 2024 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"

	"golang.org/x/sync/errgroup"

	"github.com/klauspost/compress/zstd"
	"github.com/lanrat/extsort"
)

// JoinLinkTargets joins the linktargets table with pagelinks, sending strings
// like "799\t<property>\tTalk:Zürich" to the output channel, indicating
// that page 799 links to page "Talk:Zürich". Property is an arbitrary string
// that will be emitted as the second column in the output, useful for joining.
func joinLinkTargets(ctx context.Context, site *WikiSite, property string, dumps string, out chan<- string) error {
	temp, err := os.CreateTemp("", "linktargets-*")
	if err != nil {
		return err
	}
	defer temp.Close()
	defer os.Remove(temp.Name())

	linesChan := make(chan string, 10000)
	config := extsort.DefaultConfig()
	config.ChunkSize = 8 * 1024 * 1024 / 64 // 8 MiB, 64 Bytes/line avg
	config.NumWorkers = runtime.NumCPU()
	sorter, outChan, errChan := extsort.Strings(linesChan, config)
	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error {
		defer close(linesChan)
		if err := readLinkTargets(groupCtx, site, "A", dumps, linesChan); err != nil {
			return err
		}
		if err := readLinkTargetsFromPageLinks(groupCtx, site, "B", dumps, linesChan); err != nil {
			return err
		}
		return nil
	})
	group.Go(func() error {
		sorter.Sort(groupCtx)
		joiner := NewLinkTargetJoiner(property, temp)
		for {
			select {
			case <-groupCtx.Done():
				return groupCtx.Err()

			case line, more := <-outChan:
				if !more {
					return joiner.Flush()
				}
				if err := joiner.Process(line); err != nil {
					return err
				}
			}
		}
	})
	if err := group.Wait(); err != nil {
		return err
	}
	if err := <-errChan; err != nil {
		return err
	}

	sorted, err := SortLines(ctx, temp.Name())
	if err != nil {
		return err
	}
	defer os.Remove(sorted)

	sortedFile, err := os.Open(sorted)
	if err != nil {
		return err
	}
	defer sortedFile.Close()
	defer os.Remove(sortedFile.Name())

	decompressor, err := zstd.NewReader(sortedFile)
	if err != nil {
		return err
	}
	defer decompressor.Close()

	scanner := bufio.NewScanner(decompressor)
	for scanner.Scan() {
		out <- scanner.Text()
	}
	if err := scanner.Err(); err != nil {
		return err
	}

	return nil
}

func readLinkTargets(ctx context.Context, site *WikiSite, property string, dumps string, out chan<- string) error {
	ymd := site.LastDumped.Format("20060102")
	filename := fmt.Sprintf("%s-%s-linktarget.sql.gz", site.Key, ymd)
	path := filepath.Join(dumps, site.Key, ymd, filename)
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	gz, err := gzip.NewReader(file)
	if err != nil {
		return err
	}
	defer gz.Close()

	reader, err := NewSQLReader(gz)
	if err != nil {
		return err
	}

	columns := reader.Columns()
	idCol := slices.Index(columns, "lt_id")
	namespaceCol := slices.Index(columns, "lt_namespace")
	titleCol := slices.Index(columns, "lt_title")

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		row, err := reader.Read()
		if err != nil {
			return err
		}
		if row == nil {
			return nil
		}

		id := row[idCol]
		title := row[titleCol]
		namespace := row[namespaceCol]

		var nsPrefix string
		if namespace != "0" {
			if ns, found := site.Namespaces[namespace]; found && ns.Localized != "" {
				nsPrefix = ns.Localized + ":"
			}
		}

		out <- fmt.Sprintf("%s\t%s\t%s%s", id, property, nsPrefix, title)
	}
}

func readLinkTargetsFromPageLinks(ctx context.Context, site *WikiSite, property string, dumps string, out chan<- string) error {
	ymd := site.LastDumped.Format("20060102")
	filename := fmt.Sprintf("%s-%s-pagelinks.sql.gz", site.Key, ymd)
	path := filepath.Join(dumps, site.Key, ymd, filename)
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	gz, err := gzip.NewReader(file)
	if err != nil {
		return err
	}
	defer gz.Close()

	reader, err := NewSQLReader(gz)
	if err != nil {
		return err
	}

	columns := reader.Columns()
	fromCol := slices.Index(columns, "pl_from")
	targetCol := slices.Index(columns, "pl_target_id")

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		row, err := reader.Read()
		if err != nil {
			return err
		}
		if row == nil {
			return nil
		}

		page := row[fromCol]
		target := row[targetCol]
		out <- fmt.Sprintf("%s\t%s\t%s", target, property, page)
	}
}

type LinkTargetJoiner struct {
	property   string
	out        *bufio.Writer
	linktarget string
	title      string
}

func NewLinkTargetJoiner(property string, w io.Writer) *LinkTargetJoiner {
	return &LinkTargetJoiner{
		property: property,
		out:      bufio.NewWriter(w),
	}
}

func (j *LinkTargetJoiner) Process(line string) error {
	cols := strings.Split(line, "\t")
	linktarget := cols[0]
	stream := cols[1]
	if stream == "A" {
		j.linktarget = linktarget
		j.title = cols[2]
		return nil
	}

	if stream == "B" && linktarget == j.linktarget {
		page := cols[2]
		line := fmt.Sprintf("%s\t%s\t%s\n", page, j.property, j.title)
		if _, err := j.out.WriteString(line); err != nil {
			return err
		}
	}

	return nil
}

func (j *LinkTargetJoiner) Flush() error {
	return j.out.Flush()
}
