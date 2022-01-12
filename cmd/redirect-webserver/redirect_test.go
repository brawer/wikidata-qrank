// SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRedirect(t *testing.T) {
	for _, tc := range []struct{ path, location string }{
		{"/", "https://qrank.wmcloud.org/"},
		{"/download/qrank.csv.gz", "https://qrank.wmcloud.org/download/qrank.csv.gz"},
	} {
		req, err := http.NewRequest("GET", tc.path, nil)
		if err != nil {
			t.Fatal(err)
		}

		rec := httptest.NewRecorder()
		handler := http.HandlerFunc(HandleRedirect)
		handler.ServeHTTP(rec, req)
		if status := rec.Code; status != http.StatusMovedPermanently {
			t.Errorf("got status %d, want %d", status, http.StatusMovedPermanently)
		}
		if got := rec.Header().Get("Location"); got != tc.location {
			t.Errorf("got Location: %s, want %s", got, tc.location)
		}
	}
}
