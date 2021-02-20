package main

import (
	"bytes"
	"strings"
	"unicode"

	"golang.org/x/text/cases"
)

// Caser is stateless and safe to use concurrently by multiple goroutines.
// https://pkg.go.dev/golang.org/x/text/cases#Fold
var caser = cases.Fold()

func formatLine(site, title, value string) string {
	// Turkish needs special casefolding. Azeri follows Turkish rules.
	// We do this if the wikimedia site starts in "tr." or "az.".
	var turkish bool
	if len(site) > 2 && site[2] == '.' {
		c1, c2 := site[0], site[1]
		turkish = (c1 == 't' && c2 == 'r') || (c1 == 'a' && c2 == 'z')
	}
	if turkish {
		title = strings.ToLowerSpecial(unicode.TurkishCase, title)
	} else {
		title = caser.String(title)
	}

	var buf bytes.Buffer
	buf.WriteString(site)
	buf.WriteByte('/')
	for _, c := range title {
		if c > 0x20 {
			buf.WriteRune(c)
		} else {
			buf.WriteByte('_')
		}
	}
	buf.WriteByte(' ')
	buf.WriteString(value)
	return buf.String()
}
