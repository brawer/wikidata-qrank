// SPDX-FileCopyrightText: 2026 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package version

import (
	"regexp"
	"testing"
)

func TestResolve(t *testing.T) {
	tests := []struct {
		name            string
		v, release, rev string
		want            string
	}{
		{"linker flag wins", "QRank/9.9.9", "", "", "QRank/9.9.9"},
		{"linker flag wins over release", "QRank/9.9.9", "0.1.1", "abc123", "QRank/9.9.9"},
		{"release, no revision", Base, "0.1.2", "", "QRank/v0.1.2"},
		{"release with revision", Base, "0.1.2", "1a2b3c4d5e6f", "QRank/v0.1.2+1a2b3c4d5e6f"},
		{"release with dirty revision", Base, "0.1.2", "1a2b3c4d5e6f-modified", "QRank/v0.1.2+1a2b3c4d5e6f-modified"},
		{"no release, no revision", Base, "", "", Base},
		{"no release, revision", Base, "", "1a2b3c4d5e6f", "QRank/git-1a2b3c4d5e6f"},
		{"no release, dirty revision", Base, "", "1a2b3c4d5e6f-modified", "QRank/git-1a2b3c4d5e6f-modified"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := resolve(tt.v, tt.release, tt.rev); got != tt.want {
				t.Errorf("resolve(%q, %q, %q) = %q, want %q", tt.v, tt.release, tt.rev, got, tt.want)
			}
		})
	}
}

func TestRevision(t *testing.T) {
	rev, modified := Revision()
	// A `go test` binary usually records no VCS revision; a toolchain that
	// does must give a full-length hex string, not the truncated form.
	if rev != "" && !regexp.MustCompile(`^[0-9a-f]{40}$`).MatchString(rev) {
		t.Errorf("Revision() = %q, want empty or a 40-hex revision", rev)
	}
	// Whatever Revision reports must agree with what Resolve embeds.
	if rev == "" {
		return
	}
	want := "QRank/v" + Release + "+" + rev[:12]
	if modified {
		want += "-modified"
	}
	if Release == "" {
		want = "QRank/git-" + rev[:12]
		if modified {
			want += "-modified"
		}
	}
	if got := Resolve(Base); got != want {
		t.Errorf("Resolve(Base) = %q, want %q (from Revision)", got, want)
	}
}

func TestResolveRealBuild(t *testing.T) {
	// A linker-flag build is passed through untouched.
	if got := Resolve("QRank/9.9.9"); got != "QRank/9.9.9" {
		t.Errorf("Resolve(override) = %q, want it unchanged", got)
	}

	// This test binary embeds Release and the toolchain records no VCS
	// revision, so Resolve(Base) is "QRank/v<Release>"; guard against a
	// toolchain that does stamp one, and against Release being cleared.
	got := Resolve(Base)
	var re *regexp.Regexp
	if Release != "" {
		re = regexp.MustCompile(`^QRank/v` + regexp.QuoteMeta(Release) + `(\+[0-9a-f]{7,12}(-modified)?)?$`)
	} else {
		re = regexp.MustCompile(`^QRank(/git-[0-9a-f]{7,12}(-modified)?)?$`)
	}
	if !re.MatchString(got) {
		t.Errorf("Resolve(%q) = %q, want match for %v", Base, got, re)
	}
}
