// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"testing"
)

// A fake HTTP transport that answers the same requests as planet.osm.org.
type FakeOSMPlanet struct {
	// If true, return 503 Service Unavailable for all requests.
	Broken bool
}

func (f *FakeOSMPlanet) RoundTrip(req *http.Request) (*http.Response, error) {
	header := make(http.Header)

	if f.Broken {
		header.Add("Content-Type", "text/plain")
		body := ioutil.NopCloser(bytes.NewBufferString("Service Unavailable"))
		return &http.Response{StatusCode: 503, Body: body, Header: header}, nil
	}

	url := req.URL.String()
	if url == "https://planet.openstreetmap.org/tile_logs/" {
		body, err := os.Open("testdata/tile_logs.html")
		if err != nil {
			return nil, err
		}

		header.Add("Content-Type", "text/html;charset=UTF-8")
		return &http.Response{StatusCode: 200, Body: body, Header: header}, nil
	}

	return nil, fmt.Errorf("unexpected request: %s", url)
}

func TestGetAvailableWeeks(t *testing.T) {
	client := &http.Client{Transport: &FakeOSMPlanet{}}
	weeks, err := GetAvailableWeeks(client)
	if err != nil {
		t.Error(err)
		return
	}

	got := fmt.Sprintf("%s", weeks)
	if got != "[2021-W52 2022-W01]" {
		t.Errorf("expected [2021-W52 2022-W01], got %s", got)
	}
}

func TestGetAvailableWeeksServerError(t *testing.T) {
	client := &http.Client{Transport: &FakeOSMPlanet{Broken: true}}
	_, err := GetAvailableWeeks(client)
	if !strings.HasPrefix(err.Error(), "failed to fetch") {
		t.Errorf("expected fetch failure, got %v", err)
	}
}

func TestGetTileLogs(t *testing.T) {
	client := &http.Client{Transport: &FakeOSMPlanet{}}
	cachedir, err := ioutil.TempDir("", "tilelogs_test")
	if err != nil {
		t.Error(err)
		return
	}
	_, err = GetTileLogs("2021-W52", client, cachedir)
	if err != nil {
		t.Error(err)
		return
	}
}

func TestGetTileLogsCached(t *testing.T) {
	reader, err := GetTileLogs("2042-W08", nil, "testdata")
	if err != nil {
		t.Error(err)
		return
	}

	got := readStream(reader)
	if got != "Hello world" {
		t.Errorf(`expected "Hello World", got "%s"`, got)
	}
}

// Read an io.Stream into a string. Helper for testing.
func readStream(r io.Reader) string {
	buf, err := io.ReadAll(r)
	if err != nil {
		return fmt.Sprintf("error: %v", err)
	}
	return string(buf)
}

func TestWeekStart(t *testing.T) {
	for _, tc := range []struct {
		year     int
		day      int
		expected string
	}{
		{2018, -1, "2017-12-18"},
		{2018, 0, "2017-12-25"},
		{2018, 1, "2018-01-01"},
		{2018, 2, "2018-01-08"},
		{2019, 1, "2018-12-31"},
		{2019, 2, "2019-01-07"},
		{2019, 53, "2019-12-30"},
		{2019, 54, "2020-01-06"},
	} {
		got := weekStart(tc.year, tc.day).Format("2006-01-02")
		if tc.expected != got {
			t.Errorf("expected weekStart(%d, %d) = %s, got %s", tc.year, tc.day, tc.expected, got)
		}
	}
}
