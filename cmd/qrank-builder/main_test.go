// SPDX-FileCopyrightText: 2026 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import "testing"

func TestAppVersion(t *testing.T) {
	for _, tc := range []struct{ v, want string }{
		{"QRank/v2.0.0+1a2b3c4d5e6f", "v2.0.0+1a2b3c4d5e6f"},
		{"QRank/git-1a2b3c4d5e6f", "git-1a2b3c4d5e6f"},
		{"QRank", "QRank"},
		{"custom-1.2", "custom-1.2"},
	} {
		if got := appVersion(tc.v); got != tc.want {
			t.Errorf("appVersion(%q) = %q, want %q", tc.v, got, tc.want)
		}
	}
}
