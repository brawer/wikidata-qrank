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
	"strconv"
	"strings"

	//"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	port := flag.Int("port", 0, "port for serving HTTP requests")
	storagekey := flag.String("storage-key", "keys/storage-key", "path to key with storage access credentials")
	workdir := flag.String("workdir", "webserver-workdir", "path to working directory on local disk")
	flag.Parse()

	if *port == 0 {
		*port, _ = strconv.Atoi(os.Getenv("PORT"))
	}

	storage, err := NewStorage(*storagekey, *workdir)
	if err != nil {
		log.Fatal(err)
	}

	if err := storage.Reload(context.Background()); err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go storage.Watch(ctx)
	server := &Webserver{storage: storage}
	http.HandleFunc("/", server.HandleMain)
	http.HandleFunc("/robots.txt", server.HandleRobotsTxt)
	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/download/", server.HandleDownload)
	log.Printf("Listening for HTTP requests on port %d", *port)
	http.ListenAndServe(":"+strconv.Itoa(*port), nil)
	cancel()
}

type Webserver struct {
	storage *Storage
}

func (ws *Webserver) HandleMain(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "%s",
		`<html>
<head>
<link href='https://tools-static.wmflabs.org/fontcdn/css?family=Roboto+Slab:400,700' rel='stylesheet' type='text/css'/>
<style>
* {
  font-family: 'Roboto Slab', serif;
}
h1 {
  color: #0066ff;
  margin-left: 1em;
  margin-top: 1em;
}
p {
  margin-left: 5em;
}
</style>
</head>
<body><h1>Wikidata QRank</h1>

<p>QRank is ranking <a href="https://www.wikidata.org/">Wikidata entities</a>
by aggregating page views on Wikipedia, Wikispecies, Wikibooks, Wikiquote,
and other Wikimedia projects. For an introduction, see the <a href="https://github.com/brawer/wikidata-qrank/blob/main/README.md">README file</a>. For additional background,
 check out the
<a href="https://github.com/brawer/wikidata-qrank/blob/main/doc/design.md">Technical Design Document</a>. The source code that computes the ranking is <a href="https://github.com/brawer/wikidata-qrank">here</a>.</p>

<p>To <b>download</b> the latest QRank data, <a href="/download/qrank.csv.gz">click
here</a>.  The file gets updated periodically; use
<a href="https://developer.mozilla.org/en-US/docs/Web/HTTP/Conditional_requests"
>conditional requests</a> to check for updates.
The QRank data is dedicated to the <b>Public Domain</b> via <a
href="https://creativecommons.org/publicdomain/zero/1.0/">Creative
Commons Zero 1.0</a>. To the extent possible under law, we have waived
all copyright and related or neighboring rights to this work. This work
is published from Switzerland, using infrastructure of the Wikimedia
Foundation in the United States.</p>

<p><img src="https://mirrors.creativecommons.org/presskit/buttons/88x31/svg/cc-zero.svg"
width="88" height="31" alt="Public Domain" style="float:left"/></p>

</body></html>`)
}

func (ws *Webserver) HandleDownload(w http.ResponseWriter, req *http.Request) {
	if !strings.HasPrefix(req.URL.Path, "/download/") {
		http.NotFound(w, req)
		return
	}

	path := strings.TrimPrefix(req.URL.Path, "/download/")
	c, err := ws.storage.Retrieve(path)
	if err != nil {
		http.NotFound(w, req)
		return
	}

	// As per https://tools.ietf.org/html/rfc7232, ETag must have quotes.
	w.Header().Set("ETag", fmt.Sprintf(`"%s"`, c.ETag))
	w.Header().Set("Content-Type", c.ContentType)
	w.Header().Set("Access-Control-Allow-Origin", "*")
	http.ServeContent(w, req, "", c.LastModified, c)
	c.Close()
}

// HandleRobotsTxt sends a constant robots.txt file back to the
// client, allowing web crawlers to access our entire site.  If we
// didn't handle /robots.txt ourselves, Wikimedia's proxy would inject
// a deny-all response and return that to the caller.
func (ws *Webserver) HandleRobotsTxt(w http.ResponseWriter, r *http.Request) {
	// https://wikitech.wikimedia.org/wiki/Help:Toolforge/Web#/robots.txt
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintf(w, "%s", "User-Agent: *\nAllow: /\n")
}
