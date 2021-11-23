package main

import (
	"bytes"
	"fmt"
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
