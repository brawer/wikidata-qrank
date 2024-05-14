// SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"context"
)

// Build runs the entire QRank pipeline.
func Build(dumps string, numWeeks int, s3 S3) error {
	ctx := context.Background()

	pageviews, err := buildPageviews(ctx, dumps, numWeeks, s3)
	if err != nil {
		return err
	}

	sites, err := ReadWikiSites(dumps)
	if err != nil {
		return err
	}
	logger.Printf("found wikimedia dumps for %d sites", len(*sites))

	if err := buildPageSignals(ctx, dumps, sites, s3); err != nil {
		return err
	}

	_, err = buildItemSignals(ctx, pageviews, sites, s3)
	if err != nil {
		return err
	}

	return nil
}
