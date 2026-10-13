// SPDX-FileCopyrightText: 2026 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"slices"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"golang.org/x/sync/errgroup"
)

// ItemSignalsProvenance records the inputs of an item signals file,
// and which versions of qrank-builder produced them. It gets stored
// next to the item signals as item_signals/item_signals-YYYYMMDD.provenance.json,
// so that later stages can describe their own provenance without
// relying on the state of the world at the time they run. Item signals
// are built from intermediate files that may have been written by
// earlier releases, which is why Software can list several versions.
type ItemSignalsProvenance struct {
	Date      string              `json:"date"` // YYYY-MM-DD
	Dumps     DumpsProvenance     `json:"dumps"`
	Pageviews PageviewsProvenance `json:"pageviews"`
	Software  []string            `json:"software"`
}

// DumpsProvenance describes the Wikimedia database dumps that went into
// a build. Newest is the most recent dump date of any wiki. There is no
// oldest date, because some archived wikis have not been dumped in years.
type DumpsProvenance struct {
	Newest string `json:"newest"` // YYYY-MM-DD
	Wikis  int    `json:"wikis"`
}

// PageviewsProvenance describes the Wikimedia pageviews that went into a build.
type PageviewsProvenance struct {
	FirstDay string `json:"firstDay"` // YYYY-MM-DD
	LastDay  string `json:"lastDay"`  // YYYY-MM-DD
	Weeks    int    `json:"weeks"`
}

var pageviewsPathRegexp = regexp.MustCompile(`^pageviews/pageviews-(\d{4}-W\d{2})\.zst$`)

// ItemSignalsProvenancePath returns the storage path of the provenance
// record for the item signals of a given date.
func itemSignalsProvenancePath(date time.Time) string {
	return fmt.Sprintf("item_signals/item_signals-%s.provenance.json", date.Format("20060102"))
}

// BuildItemSignalsProvenance computes the provenance of the item signals
// for `date`, which get built from the page signals of `sites` and the
// weekly `pageviews` files in storage.
func buildItemSignalsProvenance(ctx context.Context, date time.Time, pageviews []string, sites *WikiSites, s3 S3) (*ItemSignalsProvenance, error) {
	p := &ItemSignalsProvenance{Date: date.Format(time.DateOnly)}

	inputs := make([]string, 0, len(sites.Sites)+len(pageviews))
	var newestDump time.Time
	for _, site := range sites.Sites {
		if site.LastDumped.After(newestDump) {
			newestDump = site.LastDumped
		}
		inputs = append(inputs, site.S3Path("page_signals"))
	}
	p.Dumps.Wikis = len(sites.Sites)
	if !newestDump.IsZero() {
		p.Dumps.Newest = newestDump.Format(time.DateOnly)
	}

	var firstDay, lastDay time.Time
	for _, pv := range pageviews {
		m := pageviewsPathRegexp.FindStringSubmatch(pv)
		if m == nil {
			return nil, fmt.Errorf("unexpected pageviews path: %s", pv)
		}
		year, week, err := ParseISOWeek(m[1])
		if err != nil {
			return nil, err
		}
		start := ISOWeekStart(year, week)
		end := start.AddDate(0, 0, 6)
		if firstDay.IsZero() || start.Before(firstDay) {
			firstDay = start
		}
		if end.After(lastDay) {
			lastDay = end
		}
		inputs = append(inputs, pv)
	}
	p.Pageviews.Weeks = len(pageviews)
	if !firstDay.IsZero() {
		p.Pageviews.FirstDay = firstDay.Format(time.DateOnly)
		p.Pageviews.LastDay = lastDay.Format(time.DateOnly)
	}

	// Find out which software versions produced the inputs. This is
	// about a thousand HEAD requests, so we send a few in parallel.
	software := map[string]bool{SoftwareVersion: true}
	var mutex sync.Mutex
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(16)
	for _, path := range inputs {
		group.Go(func() error {
			sw, err := StoredSoftware(groupCtx, s3, "qrank", path)
			if err != nil {
				return fmt.Errorf("%s: %w", path, err)
			}
			mutex.Lock()
			software[sw] = true
			mutex.Unlock()
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}

	p.Software = make([]string, 0, len(software))
	for sw := range software {
		p.Software = append(p.Software, sw)
	}
	slices.Sort(p.Software)
	return p, nil
}

// StoreItemSignalsProvenance puts a provenance record into storage.
func storeItemSignalsProvenance(ctx context.Context, p *ItemSignalsProvenance, s3 S3) error {
	date, err := time.Parse(time.DateOnly, p.Date)
	if err != nil {
		return err
	}
	data, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')

	tmp, err := os.CreateTemp("", "*-provenance.json")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	defer tmp.Close()
	if _, err := tmp.Write(data); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}

	return PutInStorage(ctx, tmp.Name(), s3, "qrank", itemSignalsProvenancePath(date), "application/json")
}

// ReadItemSignalsProvenance fetches the provenance record for the
// item signals of a given date from storage.
func readItemSignalsProvenance(ctx context.Context, date time.Time, s3 S3) (*ItemSignalsProvenance, error) {
	r, err := NewS3Reader(ctx, "qrank", itemSignalsProvenancePath(date), s3)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	var p ItemSignalsProvenance
	if err := json.NewDecoder(r).Decode(&p); err != nil {
		return nil, err
	}
	return &p, nil
}

// HasItemSignalsProvenance reports whether storage has a provenance record
// for the item signals of a given date.
func hasItemSignalsProvenance(ctx context.Context, date time.Time, s3 S3) (bool, error) {
	want := itemSignalsProvenancePath(date)
	for obj := range s3.ListObjects(ctx, "qrank", minio.ListObjectsOptions{Prefix: "item_signals/"}) {
		if obj.Err != nil {
			return false, obj.Err
		}
		if obj.Key == want {
			return true, nil
		}
	}
	return false, nil
}

// PruneItemSignalsProvenance removes old provenance records from
// storage, keeping only the newest `keep` versions.
func pruneItemSignalsProvenance(ctx context.Context, keep int, s3 S3) error {
	re := regexp.MustCompile(`^item_signals/item_signals-(\d{8})\.provenance\.json$`)
	return pruneFiles(ctx, "item_signals/", re, keep, s3)
}
