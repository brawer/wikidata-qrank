// SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/brawer/wikidata-qrank/v2/internal/version"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

var logger *log.Logger

// SoftwareVersion identifies this build in the startup log. main() resolves
// it via internal/version: a released build reports its version, any other
// build the source revision. A linker flag
// (-ldflags "-X main.SoftwareVersion=…") still overrides it if set.
var SoftwareVersion = version.Base

func main() {
	SoftwareVersion = version.Resolve(SoftwareVersion)
	ctx := context.Background()

	var dumps = flag.String("dumps", "/public/dumps/public", "path to Wikimedia dumps")
	var testRun = flag.Bool("testRun", false, "if true, we process only a small fraction of the data; used for testing")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()
	if *showVersion {
		fmt.Println(SoftwareVersion)
		return
	}

	// Log the version to stderr too, so it shows in the Toolforge job logs
	// even if the log file below cannot be opened.
	log.Printf("starting %s, built with %s", SoftwareVersion, runtime.Version())

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
	logger.Printf("starting %s, built with %s", SoftwareVersion, runtime.Version())

	storage, err := NewStorageClient()
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

// NewStorageClient sets up a client for accessing S3-compatible object
// storage, configured by the S3_ENDPOINT, S3_KEY and S3_SECRET environment
// variables.
func NewStorageClient() (*minio.Client, error) {
	endpoint := os.Getenv("S3_ENDPOINT")
	key := os.Getenv("S3_KEY")
	secret := os.Getenv("S3_SECRET")

	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(key, secret, ""),
		Secure: true,
	})
	if err != nil {
		return nil, err
	}

	client.SetAppInfo("QRankBuilder", appVersion(SoftwareVersion))
	return client, nil
}

// appVersion returns the version part of v, such as "v2.0.0+1a2b3c4d5e6f"
// for "QRank/v2.0.0+1a2b3c4d5e6f", for the User-Agent of storage requests.
func appVersion(v string) string {
	return strings.TrimPrefix(v, version.Base+"/")
}

func computeQRank(dumpsPath string, testRun bool, storage *minio.Client) error {
	return Build(&http.Client{}, dumpsPath /*numWeeks*/, 52, storage)
}
