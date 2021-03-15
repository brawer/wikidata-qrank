package main

import (
	"context"
	"flag"
	"log"
	"os"
)

var logger *log.Logger

func main() {
	var dumps = flag.String("dumps", "/public/dumps/public", "path to Wikimedia dumps")
	var testRun = flag.Bool("testRun", false, "if true, we process only a small fraction of the data; used for testing")
	flag.Parse()

	logfile, err := os.OpenFile("logs/qrank-builder.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer logfile.Close()
	logger = log.New(logfile, "", log.Ldate|log.Ltime|log.LUTC|log.Lshortfile)

	if err := computeQRank(*dumps, *testRun); err != nil {
		log.Fatal(err)
		return
	}
}

func computeQRank(dumpsPath string, testRun bool) error {
	ctx := context.Background()

	outDir := "cache"
	if testRun {
		outDir = "cache-testrun"
	}

	if err := os.MkdirAll(outDir, 0755); err != nil {
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

	_, err = buildStats(edate, qrank, outDir)
	if err != nil {
		return err
	}

	return nil
}
