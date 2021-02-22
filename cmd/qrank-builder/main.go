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
	ctx := context.Background()

	outDir := "cache"
	if err := os.MkdirAll(outDir, 0755); err != nil {
		return err
	}

	edate, epath, err := findEntitiesDump(dumpsPath)
	if err != nil {
		return err
	}

	_, err = processEntities(epath, edate, outDir, ctx)
	if err != nil {
		return err
	}

	_, err = processPageviews(dumpsPath, edate, outDir, ctx)
	if err != nil {
		return err
	}

	return nil
}
