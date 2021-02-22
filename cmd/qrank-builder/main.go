package main

import (
	"context"
	"flag"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var logger *log.Logger

func main() {
	var dumps = flag.String("dumps", "/public/dumps/public", "path to Wikimedia dumps")
	flag.Parse()

	logfile, err := os.OpenFile("logs/qrank-builder.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer logfile.Close()
	logger = log.New(logfile, "", log.Ldate|log.Ltime|log.LUTC|log.Lshortfile)

	if err := computeQRank(*dumps); err != nil {
		log.Fatal(err)
		return
	}
}

func computeQRank(dumpsPath string) error {
	path := filepath.Join(dumpsPath, "wikidatawiki", "entities", "latest-all.json.bz2")
	wikidataPath, err := filepath.EvalSymlinks(path)
	if err != nil {
		return err
	}

	parts := strings.Split(wikidataPath, string(os.PathSeparator))
	date, err := time.Parse("20060102", parts[len(parts)-2])
	if err != nil {
		return err
	}

	_, err = buildPageviews(dumpsPath, date, context.Background())
	if err != nil {
		return err
	}

	return nil
}
