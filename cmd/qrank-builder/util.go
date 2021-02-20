package main

import (
	"bytes"
	"strings"
	"unicode"

	"golang.org/x/text/cases"
	"golang.org/x/text/unicode/norm"
)

// Caser is stateless and safe to use concurrently by multiple goroutines.
// https://pkg.go.dev/golang.org/x/text/cases#Fold
var caser = cases.Fold()

func formatLine(site, title, value string) string {
	// Turkish needs special casefolding. Azeri follows Turkish rules.
	// We do this if the wikimedia site starts in "tr." or "az.".
	if len(site) > 2 && site[2] == '.' {
		c1, c2 := site[0], site[1]
		if (c1 == 't' && c2 == 'r') || (c1 == 'a' && c2 == 'z') {
			title = strings.ToLowerSpecial(unicode.TurkishCase, title)
		}
	}

	var buf bytes.Buffer
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
