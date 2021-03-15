// SPDX-License-Identifier: MIT

package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"strconv"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	var portFlag = flag.Int("port", 0, "port for serving HTTP requests")
	flag.Parse()

	port := *portFlag
	if port == 0 {
		port, _ = strconv.Atoi(os.Getenv("PORT"))
	}

	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/", handleMain)
	http.ListenAndServe(":"+strconv.Itoa(port), nil)
}

func handleMain(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "%s", "<html><body><h1>Wikidata QRank</h1>\n"+
		"<div>See <a href=\"https://github.com/brawer/wikidata-qrank\">"+
		"here</a> for background.</div></body></html>\n")
}
