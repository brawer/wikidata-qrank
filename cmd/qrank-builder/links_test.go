// SPDX-FileCopyrightText: 2024 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"reflect"
	"testing"
)

func TestLinkToBytes(t *testing.T) {
	// Serialize and then de-serialize a Link struct.
	link := Link{111, 222}
	got := LinkFromBytes(link.ToBytes()).(Link)
	if !reflect.DeepEqual(got, link) {
		t.Errorf("got %v, want %v", got, link)
	}
}

func TestLinkLess(t *testing.T) {
	type testcase struct {
		a, b Link
		want bool
	}
	for _, tc := range []testcase{
		{Link{1, 1}, Link{1, 1}, false},
		{Link{1, 5}, Link{1, 1}, false},
		{Link{1, 1}, Link{5, 1}, true},
		{Link{1, 1}, Link{1, 5}, true},
		{Link{1, 1}, Link{5, 5}, true},
	} {
		if got := LinkLess(tc.a, tc.b); got != tc.want {
			t.Errorf("got %v for %v, want %v", got, tc, tc.want)
		}
	}
}
