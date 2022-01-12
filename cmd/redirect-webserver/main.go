// Webserver to redirect from toolforge.org to wmcloud.org.
//
// Initially, the QRank project was running on toolforge.org,
// but in January 2022 it migratged to wmcloud.org. This webserver
// is running on the old toolforge infrastructure and redirects
// to the new place.
//
// SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
)

var logger *log.Logger

func main() {
	var port = flag.Int("port", 0, "port for serving HTTP requests")
	flag.Parse()
	if *port == 0 {
		*port, _ = strconv.Atoi(os.Getenv("PORT"))
	}

	logger = NewLogger("redirect-webserver.log")
	_, cancel := context.WithCancel(context.Background())
	http.HandleFunc("/", HandleRedirect)
	http.ListenAndServe(":"+strconv.Itoa(*port), nil)
	cancel()
}

// NewLogger creates a logger. If the log file already exists, its
// present content is preserved, and new log entries will get appended
// after the existing ones.
func NewLogger(logname string) *log.Logger {
	logpath := filepath.Join("logs", logname)
	if err := os.MkdirAll("logs", os.ModePerm); err != nil {
		log.Fatal(err)
	}

	logfile, err := os.OpenFile(logpath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}

	return log.New(logfile, "", log.Ldate|log.Ltime|log.LUTC|log.Lshortfile)
}

// Redirect handles HTTP requests by redirecting them to qrank.wmcloud.org.
func HandleRedirect(w http.ResponseWriter, req *http.Request) {
	location := "https://qrank.wmcloud.org" + req.URL.Path
	if logger != nil {
		logger.Printf("redirect to %s", location)
	}
	http.Redirect(w, req, location, http.StatusMovedPermanently)
}
