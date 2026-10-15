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
	"runtime"
	"strconv"
	"strings"

	"github.com/brawer/wikidata-qrank/v2/internal/version"
	//"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ServerVersion is returned to HTTP clients as the Server header. main()
// resolves it via internal/version: a released build reports its version,
// any other build the source revision. A linker flag
// (-ldflags "-X main.ServerVersion=…") still overrides it if set.
var ServerVersion = version.Base

func main() {
	ServerVersion = version.Resolve(ServerVersion)
	port := flag.Int("port", 0, "port for serving HTTP requests")
	workdir := flag.String("workdir", "webserver-workdir", "path to working directory on local disk")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()
	if *showVersion {
		fmt.Println(ServerVersion)
		return
	}

	if *port == 0 {
		*port, _ = strconv.Atoi(os.Getenv("PORT"))
		if *port == 0 {
			*port = 8000
		}
	}

	storage, err := NewStorage(*workdir)
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
	log.Printf("%s, built with %s, listening for HTTP requests on port %d",
		ServerVersion, runtime.Version(), *port)
	err = http.ListenAndServe(":"+strconv.Itoa(*port), withServerHeader(http.DefaultServeMux))
	cancel()
	log.Fatalf("HTTP server stopped: %v", err)
}

// withServerHeader wraps h so that every response, including errors and
// /metrics, carries ServerVersion in its Server header.
func withServerHeader(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Server", ServerVersion)
		h.ServeHTTP(w, r)
	})
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
Every version is also listed in a <a href="/download/datapackage.json">data
package</a>, together with a bill of materials that tells which inputs and
software versions went into it; see
<a href="https://github.com/brawer/wikidata-qrank/blob/main/doc/downloads.md">Downloads
and provenance</a>.
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
	defer c.Close()

	h := w.Header()
	switch req.Method {
	case http.MethodHead:
		fallthrough

	case http.MethodGet:
		// As per https://tools.ietf.org/html/rfc7232, ETag must have quotes.
		h.Set("ETag", fmt.Sprintf(`"%s"`, c.ETag))
		h.Set("Content-Type", c.ContentType)
		h.Set("Access-Control-Allow-Origin", "*")
		if c.Immutable {
			h.Set("Cache-Control", "public, max-age=31536000, immutable")
		}
		if c.Canonical != "" {
			// Tell clients of an undated alias, such as qrank.csv.gz,
			// which dated version they are getting.
			h.Set("Link", fmt.Sprintf(`</download/%s>; rel="canonical"`, c.Canonical))
		}
		http.ServeContent(w, req, "", c.LastModified, c)

	case http.MethodOptions: // CORS pre-flight
		h.Set("Allow", "GET, HEAD, OPTIONS")
		h.Set("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS")
		h.Set("Access-Control-Allow-Headers", "ETag, If-Match, If-None-Match, If-Modified-Since, If-Range, Range")
		h.Set("Access-Control-Allow-Origin", "*")
		h.Set("Access-Control-Expose-Headers", "ETag")
		h.Set("Access-Control-Max-Age", "86400") // 1 day
		w.WriteHeader(http.StatusNoContent)

	default:
		h.Set("Allow", "GET, HEAD, OPTIONS")
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
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
