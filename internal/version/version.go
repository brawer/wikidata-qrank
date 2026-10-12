// SPDX-FileCopyrightText: 2026 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

// Package version resolves the build identifier that qrank-builder and
// webserver report (in its startup log and the Server HTTP header).
package version

import "runtime/debug"

// Base is the value of the version variables before a release build
// overrides them with a linker flag.
const Base = "QRank"

// Resolve returns the identifier to report for v:
//
//   - a linker flag that set v at build time (v != Base) is used verbatim;
//   - otherwise, on a release build ([Release] is set) it returns
//     "QRank/v<release>", with the source revision appended as
//     SemVer build metadata when the toolchain recorded one, e.g.
//     "QRank/v0.1.2+1a2b3c4d5e6f" ("…-modified" if the tree was dirty);
//   - otherwise it returns "QRank/git-<revision>", or the bare Base
//     when no revision was recorded (e.g. a `go test` binary).
func Resolve(v string) string {
	return resolve(v, Release, revision())
}

func resolve(v, release, rev string) string {
	if v != Base {
		return v
	}
	if release != "" {
		if rev == "" {
			return v + "/v" + release
		}
		return v + "/v" + release + "+" + rev
	}
	if rev == "" {
		return v
	}
	return v + "/git-" + rev
}

// Revision returns the full source revision recorded in the build info and
// whether the working tree was modified when the binary was built. The
// revision is "" if the toolchain recorded none, e.g. a `go test` binary or
// a build from an unpacked source archive. Unlike the string that Resolve
// returns, this revision is not truncated.
func Revision() (revision string, modified bool) {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return "", false
	}
	for _, s := range bi.Settings {
		switch s.Key {
		case "vcs.revision":
			revision = s.Value
		case "vcs.modified":
			modified = s.Value == "true"
		}
	}
	return revision, modified
}

// revision returns the source revision recorded in the build info, trimmed
// to 12 hex digits and suffixed with "-modified" when the working tree was
// dirty. It is "" if no revision was recorded.
func revision() string {
	rev, dirty := Revision()
	if rev == "" {
		return ""
	}
	if len(rev) > 12 {
		rev = rev[:12]
	}
	if dirty {
		rev += "-modified"
	}
	return rev
}
