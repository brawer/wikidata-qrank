// SPDX-License-Identifier: MIT

package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strconv"
)

// TODO: Implement k-way merge of sorted TileCount files.
func mergeTileCounts(r []io.Reader, out chan<- TileCount, ctx context.Context) error {
	defer close(out)
	if len(r) == 0 {
		return nil
	}

	if len(r) > 1 {
		fmt.Println("*** TODO: Should implement k-way merge.")
		fmt.Println("*** Currently, only one single week is getting processed")
	}

	scanner := bufio.NewScanner(r[len(r)-1])
	for scanner.Scan() {
		// Check if our task has been canceled. Typically this can happen
		// because of an error in another goroutine in the same x.sync.errroup.
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		match := tileLogRegexp.FindStringSubmatch(scanner.Text())
		if match == nil || len(match) != 5 {
			continue
		}
		zoom, _ := strconv.Atoi(match[1])
		if zoom < 0 {
			continue
		}
		x, _ := strconv.ParseUint(match[2], 10, 32)
		y, _ := strconv.ParseUint(match[3], 10, 32)
		count, _ := strconv.ParseUint(match[4], 10, 64)
		out <- TileCount{Zoom: uint8(zoom), X: uint32(x), Y: uint32(y), Count: count}
	}

	return nil
}
