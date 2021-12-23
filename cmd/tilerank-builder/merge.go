// SPDX-License-Identifier: MIT

package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
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

		if tc := ParseTileCount(scanner.Text()); tc.Count > 0 {
			out <- tc
		}
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	return nil
}
