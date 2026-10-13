// SPDX-FileCopyrightText: 2026 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// datapackage.go emits public/datapackage.json, a Frictionless Data Package
// v2 descriptor that the webserver serves at /download/datapackage.json.
// It tells clients the current version and lists each file (QRank, stats,
// BOM) by relative path, byte size and SHA-256. The builder uploads it last,
// so every file it names is already in storage.
//
// Like the BOM, it is hand-written and deterministic: encoding/json emits
// struct fields in declaration order, and every input is derived from the
// build, so a rebuild of the same version is byte-identical.

const (
	datapackageSchema = "https://datapackage.org/profiles/2.0/datapackage.json"
	datapackageName   = "qrank"
	datapackageTitle  = "QRank — ranking of Wikidata items"
	datapackageDesc   = "Wikidata items ranked by their aggregated pageviews " +
		"across Wikipedia, Wikivoyage, Wikibooks and other Wikimedia projects " +
		"over the past 52 weeks."
)

// datapackagePath is where the data package is stored. Unlike the files
// it lists, it has no date in its name, and gets replaced by every build.
const datapackagePath = "public/datapackage.json"

// datapackageID is the dataset's stable identity: the same value for every
// build, forever. It is a UUIDv5 derived from a fixed name, so the derivation
// is self-documenting rather than a frozen literal.
var datapackageID = uuid.NewSHA1(bomNamespace, []byte("qrank-datapackage")).String()

// datapackageFile describes one file listed in the data package.
type datapackageFile struct {
	Path   string // dated basename, e.g. "qrank-20261004.csv.gz"
	Bytes  int64
	SHA256 string // lower-case hex
}

// datapackageInputs is everything writeDatapackage needs that is not constant.
// Every field is derived from the build, nothing from the wall clock.
type datapackageInputs struct {
	Date  time.Time
	QRank datapackageFile
	Stats datapackageFile
	BOM   datapackageFile
}

func buildDatapackage(in datapackageInputs) *fdPackage {
	return &fdPackage{
		Schema:      datapackageSchema,
		Name:        datapackageName,
		ID:          datapackageID,
		Title:       datapackageTitle,
		Description: datapackageDesc,
		Version:     in.Date.UTC().Format(time.DateOnly),
		Created:     in.Date.UTC().Format(time.RFC3339),
		Homepage:    bomWebsiteURL,
		Licenses: []fdLicense{{
			Name:  "CC0-1.0",
			Path:  "https://creativecommons.org/publicdomain/zero/1.0/",
			Title: "Creative Commons Zero v1.0 Universal",
		}},
		Sources: []fdSource{
			{Title: "Wikimedia database dumps", Path: bomDumpsURL},
			{Title: "Wikimedia pageviews", Path: bomPageviewsURL},
		},
		Contributors: []fdContributor{{
			Title: bomSupplierName,
			Path:  bomSupplierURL,
			Roles: []string{"author", "publisher"},
		}},
		Resources: []fdResource{
			{
				Name:        "qrank",
				Type:        "table",
				Path:        in.QRank.Path,
				Title:       "QRank",
				Description: "Wikidata items sorted by decreasing QRank, which is the number of pageviews over the past 52 weeks. Items without pageviews are left out. Gzip-compressed CSV.",
				Format:      "csv",
				Mediatype:   "text/csv",
				Encoding:    "utf-8",
				Bytes:       in.QRank.Bytes,
				Hash:        "sha256:" + in.QRank.SHA256,
				Schema: &fdTableSchema{
					Fields: []fdField{
						{
							Name:        "Entity",
							Type:        "string",
							Description: "Wikidata item ID",
							Constraints: &fdConstraints{
								Required: true,
								Unique:   true,
								Pattern:  "^Q[1-9][0-9]*$",
							},
						},
						{
							Name:        "QRank",
							Type:        "integer",
							Description: "Pageviews over the past 52 weeks, summed over all Wikimedia projects",
							Constraints: &fdConstraints{
								Required: true,
								Minimum:  1,
							},
						},
					},
					PrimaryKey: []string{"Entity"},
				},
			},
			{
				Name:        "stats",
				Path:        in.Stats.Path,
				Title:       "QRank statistics",
				Description: "A sample of the QRank distribution for plotting. Samples holds [Entity, rank, QRank] triples; Median is the index of the median item in Samples.",
				Format:      "json",
				Mediatype:   "application/json",
				Encoding:    "utf-8",
				Bytes:       in.Stats.Bytes,
				Hash:        "sha256:" + in.Stats.SHA256,
			},
			{
				Name:        "bom",
				Path:        in.BOM.Path,
				Title:       "CycloneDX 1.7 bill of materials",
				Description: "Provenance of the QRank file: the versions of the Wikimedia dumps and pageviews, and of every qrank-builder release that processed them.",
				Format:      "json",
				Mediatype:   "application/vnd.cyclonedx+json; version=1.7",
				Encoding:    "utf-8",
				Bytes:       in.BOM.Bytes,
				Hash:        "sha256:" + in.BOM.SHA256,
				Describes:   "qrank",
			},
		},
	}
}

// writeDatapackage builds the descriptor for in and writes it to path atomically.
func writeDatapackage(path string, in datapackageInputs) error {
	return writeJSON(path, buildDatapackage(in))
}

// StoredDatapackageVersion returns the version of the data package
// in storage, or the empty string if there is no data package yet.
func storedDatapackageVersion(ctx context.Context, s3 S3) (string, error) {
	r, err := NewS3Reader(ctx, "qrank", datapackagePath, s3)
	if err != nil {
		return "", err
	}
	defer r.Close()

	var dp struct {
		Version string `json:"version"`
	}
	if err := json.NewDecoder(r).Decode(&dp); err != nil {
		return "", err
	}
	return dp.Version, nil
}

// Frictionless Data Package v2 JSON model, covering only the fields the builder
// emits. Field order is the JSON output order; keep it stable.

type fdPackage struct {
	Schema       string          `json:"$schema"`
	Name         string          `json:"name"`
	ID           string          `json:"id"`
	Title        string          `json:"title"`
	Description  string          `json:"description"`
	Version      string          `json:"version"`
	Created      string          `json:"created"`
	Homepage     string          `json:"homepage"`
	Licenses     []fdLicense     `json:"licenses"`
	Sources      []fdSource      `json:"sources"`
	Contributors []fdContributor `json:"contributors"`
	Resources    []fdResource    `json:"resources"`
}

type fdLicense struct {
	Name  string `json:"name"`
	Path  string `json:"path"`
	Title string `json:"title"`
}

type fdSource struct {
	Title string `json:"title"`
	Path  string `json:"path"`
}

type fdContributor struct {
	Title string   `json:"title"`
	Path  string   `json:"path"`
	Roles []string `json:"roles"`
}

type fdResource struct {
	Name        string         `json:"name"`
	Type        string         `json:"type,omitempty"`
	Path        string         `json:"path"`
	Title       string         `json:"title"`
	Description string         `json:"description"`
	Format      string         `json:"format"`
	Mediatype   string         `json:"mediatype"`
	Encoding    string         `json:"encoding"`
	Bytes       int64          `json:"bytes"`
	Hash        string         `json:"hash"`
	Describes   string         `json:"describes,omitempty"`
	Schema      *fdTableSchema `json:"schema,omitempty"`
}

type fdTableSchema struct {
	Fields     []fdField `json:"fields"`
	PrimaryKey []string  `json:"primaryKey"`
}

type fdField struct {
	Name        string         `json:"name"`
	Type        string         `json:"type"`
	Description string         `json:"description,omitempty"`
	Constraints *fdConstraints `json:"constraints,omitempty"`
}

type fdConstraints struct {
	Required bool   `json:"required,omitempty"`
	Unique   bool   `json:"unique,omitempty"`
	Pattern  string `json:"pattern,omitempty"`
	Minimum  int64  `json:"minimum,omitempty"`
}
