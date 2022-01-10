// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"context"
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

	if strings.HasPrefix(url, "https://planet.openstreetmap.org/tile_logs/tiles-2567-03-") {
		body, err := os.Open("testdata/rapperswil.xz")
		if err != nil {
			return nil, err
		}

		header.Add("Content-Type", "application/x-xz")
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
	s := NewFakeStorage()
	reader, err := GetTileLogs("2567-W12", client, cachedir, s)
	if err != nil {
		t.Error(err)
		return
	}

	// Contents of testdata/rapperswil.xz
	expected := `14/8593/5747 1421
15/17186/11494 693
16/34372/22988 315
16/34373/22988 476
17/68747/45977 70
16/34372/22989 1071
17/68744/45978 77
17/68745/45978 147
17/68744/45979 154
18/137489/91959 77
17/68745/45979 196
18/137490/91959 84
18/137491/91959 84
16/34373/22989 1246
17/68746/45978 224
17/68747/45978 231
17/68746/45979 259
18/137492/91959 91
17/68747/45979 252
18/137495/91958 70
18/137494/91959 70
18/137495/91959 84
15/17187/11494 728
16/34374/22988 532
17/68748/45977 70
16/34375/22988 476
17/68750/45977 105
17/68751/45977 133
16/34374/22989 1316
17/68748/45978 266
17/68749/45978 210
17/68748/45979 308
18/137496/91959 84
18/137497/91959 77
17/68749/45979 210
16/34375/22989 1197
17/68750/45978 252
17/68751/45978 252
17/68750/45979 210
17/68751/45979 161
15/17186/11495 539
16/34372/22990 1001
17/68744/45980 168
18/137489/91960 77
18/137489/91961 70
17/68745/45980 224
18/137490/91960 70
18/137491/91960 91
18/137490/91961 70
18/137491/91961 91
17/68744/45981 140
18/137489/91962 77
17/68745/45981 168
18/137490/91962 315
18/137491/91962 322
16/34373/22990 1274
17/68746/45980 287
18/137492/91960 91
18/137493/91960 77
18/137492/91961 84
17/68747/45980 280
17/68746/45981 203
18/137492/91962 329
18/137493/91962 91
18/137492/91963 70
17/68747/45981 154
16/34372/22991 497
17/68744/45982 133
17/68745/45982 147
17/68744/45983 91
17/68745/45983 98
16/34373/22991 574
17/68746/45982 147
17/68747/45982 126
17/68746/45983 98
17/68747/45983 84
15/17187/11495 553
16/34374/22990 1323
17/68748/45980 280
17/68749/45980 231
17/68748/45981 112
17/68749/45981 98
16/34375/22990 1085
17/68750/45980 182
17/68751/45980 133
17/68750/45981 91
16/34374/22991 532
17/68748/45982 84
17/68749/45982 84
17/68748/45983 70
17/68749/45983 77
16/34375/22991 385
17/68750/45982 70
17/68750/45983 70
`
	got := readStream(reader)
	if expected != got {
		t.Errorf("expected %v, got %v", expected, got)
		fmt.Println(got)
	}

	ctx := context.Background()
	remotePath := "internal/osmviews-builder/tilelogs-2567-W12.br"
	stat, err := s.Stat(ctx, "qrank", remotePath)
	if err != nil {
		t.Fatal(err)
	}

	if want := "application/x-brotli"; stat.ContentType != want {
		t.Errorf(`got "%s", want "%s"`, stat.ContentType, want)
	}
}

func TestGetTileLogsCached(t *testing.T) {
	ctx := context.Background()
	s := NewFakeStorage()
	if err := s.PutFile(ctx, "qrank", "internal/osmviews-builder/tilelogs-2042-W08.br", "testdata/tilelogs-2042-W08.br", "application/x-brotli"); err != nil {
		t.Fatal(err)
	}
	reader, err := GetTileLogs("2042-W08", nil, "", s)
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

func ExampleParseWeek() {
	fmt.Println(ParseWeek("2018-W51")) // Output: 2018 51 <nil>
}
