package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func main() {
	var dumps = flag.String("dumps", "/public/dumps/public", "path to Wikimedia dumps")
	flag.Parse()
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

	pv, err := buildPageviews(dumpsPath, date, context.Background())
	if err != nil {
		return err
	}
	fmt.Println("*** DONE:", pv)
	return nil
}
