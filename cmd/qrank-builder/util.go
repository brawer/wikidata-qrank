package main

import (
	"strings"
	"unicode"
	"unicode/utf16"
	"unicode/utf8"

	"golang.org/x/text/cases"
	"golang.org/x/text/unicode/norm"
)

// Caser is stateless and safe to use concurrently by multiple goroutines.
// https://pkg.go.dev/golang.org/x/text/cases#Fold
var caser = cases.Fold()

func formatLine(lang, site, title, value string) string {
	// https://en.wikipedia.org/wiki/List_of_Wikipedias#Wikipedia_edition_codes
	switch lang {
	case "":
		lang = "und"
		switch site {
		case "wikidatawiki":
			site = "wikidata"
		case "wikimaniawiki":
			site = "wikimania"
		}

	case "az":
		title = strings.ToLowerSpecial(unicode.AzeriCase, title)

	case "als":
		lang = "gsw"

	case "bat_smg":
		lang = "sgs"

	case "be_x_old":
		lang = "be-tarask"

	case "commons":
		lang = "und"
		site = "commons"

	case "fiu_vro":
		lang = "vro"

	case "incubator":
		// Q11736 in Wikidata entitities dump has site: "incubatorwiki"
		// (passed to as as lang="incubator", site="wikipedia")
		// "title": "Wp/cpx/Teng-cing-ch\u012b"
		parts := strings.SplitN(title, "/", 3)
		if len(parts) == 3 && (parts[0] == "Wp" || parts[0] == "wp") &&
			len(parts[1]) < 20 {
			lang = strings.ToLower(parts[1])
			title = parts[2]
		}

	case "media": // mediawiki.org
		lang = "und"
		site = "mediawiki"

	case "meta": // meta.wikimedia.org
		lang = "und"
		site = "metawiki"

	case "roa_rup":
		lang = "rup"

	case "simple":
		lang = "en-x-simple" // Simplified English

	case "sources":
		// Q16574 in Wikidata has site: "wikisources"
		// title: "Author:蒋中正"
		lang = "und"
		site = "wikisource"

	case "species":
		lang = "und"
		site = "wikispecies"

	case "nds_nl":
		lang = "nds-NL"

	case "tr":
		title = strings.ToLowerSpecial(unicode.TurkishCase, title)

	case "zh_classical":
		lang = "lzh"

	case "zh_min_nan":
		// https://phabricator.wikimedia.org/T30442
		// https://phabricator.wikimedia.org/T86915
		lang = "nan"

	case "zh_yue":
		lang = "yue"
	}

	var buf strings.Builder
	buf.Grow(len(lang) + len(site) + len(title) + len(value) + 6)
	buf.WriteString(lang)
	buf.WriteByte('.')
	buf.WriteString(site)
	buf.WriteByte('/')
	var it norm.Iter
	it.InitString(norm.NFC, caser.String(title))
	for !it.Done() {
		c := it.Next()
		if c[0] > 0x20 {
			buf.Write(c)
		} else {
			buf.WriteByte('_')
		}
	}
	buf.WriteByte(' ')
	buf.WriteString(value)
	return buf.String()
}

// getu4 decodes \uXXXX from the beginning of s, returning the hex value,
// or it returns -1.
func getu4(s []byte) rune {
	// Source: https://golang.org/src/encoding/json/decode.go
	// License: BSD-3-Clause
	// License-URL: https://github.com/golang/go/blob/master/LICENSE
	if len(s) < 6 || s[0] != '\\' || s[1] != 'u' {
		return -1
	}
	var r rune
	for _, c := range s[2:6] {
		switch {
		case '0' <= c && c <= '9':
			c = c - '0'
		case 'a' <= c && c <= 'f':
			c = c - 'a' + 10
		case 'A' <= c && c <= 'F':
			c = c - 'A' + 10
		default:
			return -1
		}
		r = r*16 + rune(c)
	}
	return r
}

// unquote converts a quoted JSON string literal s into an actual string t.
// The rules are different than for Go, so cannot use strconv.Unquote.
func unquote(s []byte) (t string, ok bool) {
	// Source: https://golang.org/src/encoding/json/decode.go
	// License: BSD-3-Clause
	// License-URL: https://github.com/golang/go/blob/master/LICENSE
	s, ok = unquoteBytes(s)
	t = string(s)
	return
}

func unquoteBytes(s []byte) (t []byte, ok bool) {
	// Source: https://golang.org/src/encoding/json/decode.go
	// License: BSD-3-Clause
	// License-URL: https://github.com/golang/go/blob/master/LICENSE
	if len(s) < 2 || s[0] != '"' || s[len(s)-1] != '"' {
		return
	}
	s = s[1 : len(s)-1]

	// Check for unusual characters. If there are none,
	// then no unquoting is needed, so return a slice of the
	// original bytes.
	r := 0
	for r < len(s) {
		c := s[r]
		if c == '\\' || c == '"' || c < ' ' {
			break
		}
		if c < utf8.RuneSelf {
			r++
			continue
		}
		rr, size := utf8.DecodeRune(s[r:])
		if rr == utf8.RuneError && size == 1 {
			break
		}
		r += size
	}
	if r == len(s) {
		return s, true
	}

	b := make([]byte, len(s)+2*utf8.UTFMax)
	w := copy(b, s[0:r])
	for r < len(s) {
		// Out of room? Can only happen if s is full of
		// malformed UTF-8 and we're replacing each
		// byte with RuneError.
		if w >= len(b)-2*utf8.UTFMax {
			nb := make([]byte, (len(b)+utf8.UTFMax)*2)
			copy(nb, b[0:w])
			b = nb
		}
		switch c := s[r]; {
		case c == '\\':
			r++
			if r >= len(s) {
				return
			}
			switch s[r] {
			default:
				return
			case '"', '\\', '/', '\'':
				b[w] = s[r]
				r++
				w++
			case 'b':
				b[w] = '\b'
				r++
				w++
			case 'f':
				b[w] = '\f'
				r++
				w++
			case 'n':
				b[w] = '\n'
				r++
				w++
			case 'r':
				b[w] = '\r'
				r++
				w++
			case 't':
				b[w] = '\t'
				r++
				w++
			case 'u':
				r--
				rr := getu4(s[r:])
				if rr < 0 {
					return
				}
				r += 6
				if utf16.IsSurrogate(rr) {
					rr1 := getu4(s[r:])
					if dec := utf16.DecodeRune(rr, rr1); dec != unicode.ReplacementChar {
						// A valid pair; consume.
						r += 6
						w += utf8.EncodeRune(b[w:], dec)
						break
					}
					// Invalid surrogate; fall back to replacement rune.
					rr = unicode.ReplacementChar
				}
				w += utf8.EncodeRune(b[w:], rr)
			}

		// Quote, control characters are invalid.
		case c == '"', c < ' ':
			return

		// ASCII
		case c < utf8.RuneSelf:
			b[w] = c
			r++
			w++

		// Coerce to well-formed UTF-8.
		default:
			rr, size := utf8.DecodeRune(s[r:])
			r += size
			w += utf8.EncodeRune(b[w:], rr)
		}
	}
	return b[0:w], true
}
