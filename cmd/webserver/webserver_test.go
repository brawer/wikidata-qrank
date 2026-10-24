// SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func sendRequest(method, path string, reqHeader http.Header) (status int, h http.Header, body []byte, err error) {
	req := httptest.NewRequest(method, path, nil)
	req.Header = reqHeader
	w := httptest.NewRecorder()
	testWebserver.HandleDownload(w, req)
	res := w.Result()
	defer res.Body.Close()
	body, err = io.ReadAll(res.Body)
	if err != nil {
		return res.StatusCode, res.Header, body, err
	}
	return res.StatusCode, res.Header, body, nil
}

func TestWebserver_Download(t *testing.T) {
	rh := make(http.Header)
	status, header, body, err := sendRequest("GET", "/download/c.txt", rh)
	if err != nil {
		t.Error(err)
		return
	}

	if status != http.StatusOK {
		t.Errorf("want StatusCode %d, got %d", http.StatusOK, status)
	}

	want := "Content"
	if string(body) != want {
		t.Errorf(`want body="%s", got "%s"`, want, string(body))
	}

	want = "text/plain"
	if got := header.Get("Content-Type"); got != want {
		t.Errorf(`want "Content-Type: %s", got "%s"`, want, got)
	}

	want = "Tue, 21 Nov 2023 19:20:21 GMT"
	if got := header.Get("Last-Modified"); got != want {
		t.Errorf(`expected "Last-Modified: %s", got "%s"`, want, got)
	}

	want = `"ETag-123"`
	if got := header.Get("ETag"); got != want {
		t.Errorf(`expected "ETag: %s", got "%s"`, want, got)
	}

	want = "*"
	if got := header.Get("Access-Control-Allow-Origin"); got != want {
		t.Errorf(`expected "Access-Control-Allow-Origin: %s", got "%s"`, want, got)
	}
}

func TestWebserver_DownloadETagMatch(t *testing.T) {
	rh := make(http.Header)
	rh.Set("If-None-Match", `"ETag-123"`)
	status, header, body, err := sendRequest("GET", "/download/c.txt", rh)
	if err != nil {
		t.Error(err)
		return
	}

	if status != http.StatusNotModified {
		t.Errorf("want StatusCode %d, got %d", http.StatusNotModified, status)
	}

	if len(body) > 0 {
		t.Errorf(`want empty body, got "%s"`, string(body))
	}

	want := `"ETag-123"`
	if got := header.Get("ETag"); got != want {
		t.Errorf(`expected "ETag: %s", got "%s"`, want, got)
	}
}

func TestWebserver_DownloadNotFound(t *testing.T) {
	rh := make(http.Header)
	status, _, _, err := sendRequest("GET", "/download/unkown", rh)
	if err != nil {
		t.Error(err)
		return
	}

	if status != http.StatusNotFound {
		t.Errorf("want StatusCode %d, got %d", http.StatusNotFound, status)
	}
}

func TestWebserver_DownloadOptions(t *testing.T) {
	rh := make(http.Header)
	status, header, body, err := sendRequest("OPTIONS", "/download/c.txt", rh)
	if err != nil {
		t.Error(err)
		return
	}

	if status != http.StatusNoContent {
		t.Errorf("want StatusCode %d, got %d", http.StatusNoContent, status)
	}

	if len(body) > 0 {
		t.Errorf(`want empty body, got "%s"`, string(body))
	}

	want := "GET, HEAD, OPTIONS"
	if got := header.Get("Allow"); got != want {
		t.Errorf(`expected "Allow: %s", got "%s"`, want, got)
	}
	if got := header.Get("Access-Control-Allow-Methods"); got != want {
		t.Errorf(`expected "Access-Control-Allow-Methods: %s", got "%s"`, want, got)
	}

	want = "*"
	if got := header.Get("Access-Control-Allow-Origin"); got != want {
		t.Errorf(`expected "Access-Control-Allow-Origin: %s", got "%s"`, want, got)
	}

	want = "ETag, If-Match, If-None-Match, If-Modified-Since, If-Range, Range"
	if got := header.Get("Access-Control-Allow-Headers"); got != want {
		t.Errorf(`expected "Access-Control-Allow-Headers: %s", got "%s"`, want, got)
	}

	want = "ETag"
	if got := header.Get("Access-Control-Expose-Headers"); got != want {
		t.Errorf(`expected "Access-Control-Expose-Headers: %s", got "%s"`, want, got)
	}

	want = "86400"
	if got := header.Get("Access-Control-Max-Age"); got != want {
		t.Errorf(`expected "Access-Control-Max-Age: %s", got "%s"`, want, got)
	}

}

func TestWebserver_DownloadOptionsNotFound(t *testing.T) {
	rh := make(http.Header)
	status, _, _, err := sendRequest("OPTIONS", "/download/unkown", rh)
	if err != nil {
		t.Error(err)
		return
	}

	if status != http.StatusNotFound {
		t.Errorf("want StatusCode %d, got %d", http.StatusNotFound, status)
	}
}

func TestWebserver_DownloadMethodNotAllowed(t *testing.T) {
	rh := make(http.Header)
	status, header, body, err := sendRequest("DELETE", "/download/c.txt", rh)
	if err != nil {
		t.Error(err)
		return
	}

	if status != http.StatusMethodNotAllowed {
		t.Errorf("want StatusCode %d, got %d", http.StatusMethodNotAllowed, status)
	}

	if len(body) > 0 {
		t.Errorf(`want empty body, got "%s"`, string(body))
	}

	want := "GET, HEAD, OPTIONS"
	if got := header.Get("Allow"); got != want {
		t.Errorf(`expected "Allow: %s", got "%s"`, want, got)
	}
}

func TestWebserver_DownloadDated(t *testing.T) {
	storage := &Storage{
		client: newMapStorageClient(map[string]string{
			"public/qrank-20261004.csv.gz": "2026-10-04T10:00:00Z",
		}),
		workdir: t.TempDir(),
	}
	if err := storage.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	ws := &Webserver{storage: storage}

	for _, tc := range []struct{ path, cacheControl, link string }{
		{"/download/qrank-20261004.csv.gz", "public, max-age=31536000, immutable", ""},
		{"/download/qrank.csv.gz", "", `</download/qrank-20261004.csv.gz>; rel="canonical"`},
	} {
		w := httptest.NewRecorder()
		ws.HandleDownload(w, httptest.NewRequest("GET", tc.path, nil))
		res := w.Result()
		if res.StatusCode != http.StatusOK {
			t.Errorf("GET %s: got status %d", tc.path, res.StatusCode)
		}
		if got := res.Header.Get("Cache-Control"); got != tc.cacheControl {
			t.Errorf(`GET %s: got "Cache-Control: %s", want "%s"`, tc.path, got, tc.cacheControl)
		}
		if got := res.Header.Get("Link"); got != tc.link {
			t.Errorf(`GET %s: got "Link: %s", want "%s"`, tc.path, got, tc.link)
		}
		if got := res.Header.Get("Content-Type"); got != "application/gzip" {
			t.Errorf(`GET %s: got "Content-Type: %s"`, tc.path, got)
		}
	}
}

var testWebserver *Webserver = makeTestWebserver()

func makeTestWebserver() *Webserver {
	storage := &Storage{
		client:  &fakeStorageClient{},
		workdir: os.TempDir(),
		files:   make(map[string]*localFile, 10),
		ready:   true,
	}

	path := filepath.Join(storage.workdir, "c.txt")
	if err := os.WriteFile(path, []byte("Content"), 0644); err != nil {
		log.Fatal(err)
	}

	lastmod, _ := time.Parse(time.RFC3339, "2023-11-21T19:20:21Z")
	storage.files["c.txt"] = &localFile{
		Path:         path,
		ContentType:  "text/plain",
		ETag:         "ETag-123",
		LastModified: lastmod,
	}

	return &Webserver{storage: storage}
}

func TestWebserver_ServerHeader(t *testing.T) {
	handler := withServerHeader(http.HandlerFunc(testWebserver.HandleRobotsTxt))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest("GET", "/robots.txt", nil))
	if got := w.Result().Header.Get("Server"); got == "" || got != ServerVersion {
		t.Errorf(`want "Server: %s", got "%s"`, ServerVersion, got)
	}
}

func TestWebserver_DownloadNotReady(t *testing.T) {
	ws := &Webserver{storage: &Storage{workdir: t.TempDir()}}
	w := httptest.NewRecorder()
	ws.HandleDownload(w, httptest.NewRequest("GET", "/download/qrank.csv.gz", nil))
	if got := w.Result().StatusCode; got != http.StatusServiceUnavailable {
		t.Errorf("got status %d, want %d", got, http.StatusServiceUnavailable)
	}
	if got := w.Result().Header.Get("Retry-After"); got == "" {
		t.Error("want Retry-After header")
	}
}
