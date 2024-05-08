// SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

var logger *log.Logger

func main() {
	ctx := context.Background()

	var dumps = flag.String("dumps", "/public/dumps/public", "path to Wikimedia dumps")
	var testRun = flag.Bool("testRun", false, "if true, we process only a small fraction of the data; used for testing")
	storagekey := flag.String("", "", "path to key with storage access credentials")
	flag.Parse()

	// https://wikitech.wikimedia.org/wiki/Help:Toolforge/Build_Service#Using_NFS_shared_storage
	if toolDir := os.Getenv("TOOL_DATA_DIR"); toolDir != "" {
		if err := os.Chdir(toolDir); err != nil {
			log.Fatal(err)
		}
	}

	workdir, _ := os.Getwd()
	logPath := filepath.Join("logs", "qrank-builder.log")
	fmt.Printf("logs written to %s in workdir=%s", logPath, workdir)
	fmt.Fprintf(os.Stderr, "logs written to %s in workdir=%s", logPath, workdir)
	logfile, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer logfile.Close()
	logger = log.New(logfile, "", log.Ldate|log.Ltime|log.LUTC|log.Lshortfile)
	logger.Printf("qrank-builder starting up")

	storage, err := NewStorageClient(*storagekey)
	if err != nil {
		logger.Fatal(err)
	}

	bucketExists, err := storage.BucketExists(ctx, "qrank")
	if err != nil {
		logger.Fatal(err)
	}
	if !bucketExists {
		logger.Fatal("storage bucket \"qrank\" does not exist")
	}

	if err := computeQRank(*dumps, *testRun, storage); err != nil {
		logger.Printf("ComputeQRank failed: %v", err)
		log.Fatal(err)
		return
	}

	logger.Printf("qrank-builder exiting")
}

// NewStorageClient sets up a client for accessing S3-compatible object storage.
func NewStorageClient(keypath string) (*minio.Client, error) {
	var config struct{ Endpoint, Key, Secret string }

	if keypath == "" {
		config.Endpoint = os.Getenv("S3_ENDPOINT")
		config.Key = os.Getenv("S3_KEY")
		config.Secret = os.Getenv("S3_SECRET")
	} else {
		data, err := os.ReadFile(keypath)
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(data, &config); err != nil {
			return nil, err
		}
	}

	client, err := minio.New(config.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(config.Key, config.Secret, ""),
		Secure: true,
	})
	if err != nil {
		return nil, err
	}

	client.SetAppInfo("QRankBuilder", "0.1")
	return client, nil
}

func computeQRank(dumpsPath string, testRun bool, storage *minio.Client) error {
	ctx := context.Background()
	var s3 S3 = storage

	// Build pageviews files. We're changing how pageviews get aggregated,
	// and this is the new, vastly more efficient approach. But it is not
	// fully implemented yet, so we do not yet actually use the output.
	// The older approach is done by the call to processPageviews below.
	// https://github.com/brawer/wikidata-qrank/issues/23
	_, err := buildPageviews(ctx, dumpsPath, 52, s3) // for past 52 weeks
	if err != nil {
		return err
	}

	sites, err := ReadWikiSites(dumpsPath)
	if err != nil {
		return err
	}
	logger.Printf("found wikimedia dumps for %d sites", len(*sites))
	if err := buildPageEntities(ctx, dumpsPath, sites, s3); err != nil {
		return err
	}

	// TOOD: The following is just a benchmark to see how long it takes
	// to read all page_entities within the Wikimedia datacenter.
	// Once we know it's reasonable to do this, we can remove this code.
	logger.Printf("start reading page_entities")
	numEntities := 0
	scanner := NewPageEntitiesScanner(sites, s3)
	for scanner.Scan() {
		numEntities += 1
		if numEntities < 5 || numEntities%1000000 == 0 {
			logger.Printf("  %d: %s\n", numEntities, scanner.Text())
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	logger.Printf("finished reading %d page_entities", numEntities)

	// TODO: Old code starts here, remove after new implementation is done.
	return nil

	outDir := "cache"
	if testRun {
		outDir = "cache-testrun"
	}

	if err := os.MkdirAll(outDir, 0755); err != nil {
		return err
	}

	if err := CleanupCache(outDir); err != nil {
		return err
	}

	edate, epath, err := findEntitiesDump(dumpsPath)
	if err != nil {
		return err
	}

	pageviews, err := processPageviews(testRun, dumpsPath, edate, outDir, ctx)
	if err != nil {
		return err
	}

	sitelinks, err := processEntities(testRun, epath, edate, outDir, ctx)
	if err != nil {
		return err
	}

	qviews, err := buildQViews(testRun, edate, sitelinks, pageviews, outDir, ctx)
	if err != nil {
		return err
	}

	qrank, err := buildQRank(edate, qviews, outDir, ctx)
	if err != nil {
		return err
	}

	stats, err := buildStats(edate, qrank, 50, 1000, outDir)
	if err != nil {
		return err
	}

	if storage != nil {
		if err := upload(edate, qrank, stats, storage); err != nil {
			return err
		}
	}

	return nil
}

// Upload puts the final output files into an S3-compatible object storage.
func upload(date time.Time, qrank, stats string, storage *minio.Client) error {
	ymd := date.Format("20060102")
	qrankDest := fmt.Sprintf("public/qrank-%s.csv.gz", ymd)
	if err := uploadFile(qrankDest, qrank, "text/csv", storage); err != nil {
		return err
	}

	statsDest := fmt.Sprintf("public/qrank-stats-%s.json", ymd)
	if err := uploadFile(statsDest, stats, "application/json", storage); err != nil {
		return err
	}

	return nil
}

// UploadFile puts one single file into an S3-compatible object storage.
func uploadFile(dest, src, contentType string, storage *minio.Client) error {
	ctx := context.Background()
	bucket := "qrank"

	// Check if the output file already exists in storage.
	_, err := storage.StatObject(ctx, bucket, dest, minio.StatObjectOptions{})
	if err == nil {
		logmsg := fmt.Sprintf("Already in object storage: %s/%s", bucket, dest)
		fmt.Println(logmsg)
		if logger != nil {
			logger.Println(logmsg)
		}
		return nil
	}

	opts := minio.PutObjectOptions{ContentType: contentType}
	_, err = storage.FPutObject(ctx, bucket, dest, src, opts)
	if err != nil {
		return err
	}

	logmsg := fmt.Sprintf("Uploaded to object storage: %s/%s", bucket, dest)
	fmt.Println(logmsg)
	if logger != nil {
		logger.Println(logmsg)
	}

	return nil
}
