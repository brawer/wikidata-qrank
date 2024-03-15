// SPDX-FileCopyrightText: 2024 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"bufio"
	"io"
	"strings"
	"unicode"
)

type sqlToken int

const (
	unexpected = iota
	word       // DROP, TABLE, CHARSET, blob, float, int, unsigned
	name       // `page_props`, `pp_propname_sortkey_page`
	number     // 12, 12.3, -4
	text       // "foo"
	comment    // -- MySQL dump
	leftParen
	rightParen
	comma
	semicolon
	minus
	slash
)

type sqlLexer struct {
	reader *bufio.Reader
}

func newSQLLexer(r io.Reader) *sqlLexer {
	return &sqlLexer{reader: bufio.NewReader(r)}
}

func (lex *sqlLexer) read() (sqlToken, string, error) {
	var c rune
	var err error
	for {
		c, _, err = lex.reader.ReadRune()
		if err != nil || !unicode.IsSpace(c) {
			break
		}
	}
	if err != nil {
		return unexpected, "", err
	}

	switch c {
	case '`':
		text, err := lex.readUntil('`')
		if err != nil {
			return unexpected, "", err
		}
		return name, text, err
	case '-':
		next, _, err := lex.reader.ReadRune()
		if err == io.EOF {
			return minus, "", nil
		} else if err != nil {
			return unexpected, "", err
		}
		if unreadErr := lex.reader.UnreadRune(); unreadErr != nil {
			return unexpected, "", unreadErr
		}
		if next == '-' {
			text, err := lex.readUntil('\n')
			if err != nil {
				return unexpected, "", err
			}
			return comment, strings.TrimSpace(text[1:len(text)]), nil
		}
		if isNumberStart(next) {
			return lex.readNumber(c)
		}
		return minus, "", nil
	case '\'':
		// TODO: Handle escapes? Check what we actually see in Wikimedia dumps.
		t, err := lex.readUntil('\'')
		if err != nil {
			return unexpected, "", err
		}
		return text, t, err
	case '/':
		next, _, err := lex.reader.ReadRune()
		if err == io.EOF {
			return slash, "", nil
		} else if err != nil {
			return unexpected, "", err
		}
		if next == '*' {
			return lex.readSlashStarComment()
		}
		if unreadErr := lex.reader.UnreadRune(); unreadErr != nil {
			return unexpected, "", unreadErr
		}
		return slash, "", err
	case '(':
		return leftParen, "", nil
	case ')':
		return rightParen, "", nil
	case ',':
		return comma, "", nil
	case ';':
		return semicolon, "", nil
	}
	if isWordChar(c) {
		return lex.readWord(c)
	}
	if isNumberStart(c) {
		return lex.readNumber(c)
	}
	return unexpected, string(c), nil
}

func (lex *sqlLexer) readWord(start rune) (sqlToken, string, error) {
	var buf strings.Builder
	buf.WriteRune(start)
	for {
		c, _, err := lex.reader.ReadRune()
		if err == io.EOF {
			break
		} else if err != nil {
			return unexpected, "", err
		}
		if isWordChar(c) {
			buf.WriteRune(c)
			continue
		}
		if err := lex.reader.UnreadRune(); err != nil {
			return unexpected, "", err
		}
		break
	}
	text := buf.String()
	return word, text, nil
}

func (lex *sqlLexer) readNumber(start rune) (sqlToken, string, error) {
	gotDot := (start == '.')
	var buf strings.Builder
	buf.WriteRune(start)
	for {
		c, _, err := lex.reader.ReadRune()
		if err == io.EOF {
			break
		} else if err != nil {
			return unexpected, "", err
		}
		if c == '.' && !gotDot {
			buf.WriteRune(c)
			gotDot = true
			continue
		}
		if c >= '0' && c <= '9' {
			buf.WriteRune(c)
			continue
		}
		if err := lex.reader.UnreadRune(); err != nil {
			return unexpected, "", err
		}
		break
	}
	text := buf.String()
	return number, text, nil
}

func (lex *sqlLexer) readUntil(delim rune) (string, error) {
	var buf strings.Builder
	for {
		c, _, err := lex.reader.ReadRune()
		if c == delim || err == io.EOF {
			break
		} else if err != nil {
			return "", err
		}
		buf.WriteRune(c)
	}
	return buf.String(), nil
}

func (lex *sqlLexer) readSlashStarComment() (sqlToken, string, error) {
	var buf strings.Builder
	var last rune
	for {
		c, _, err := lex.reader.ReadRune()
		if err == io.EOF {
			break
		} else if err != nil {
			return unexpected, "", err
		}
		if c == '/' && last == '*' {
			break
		}
		buf.WriteRune(c)
		last = c
	}
	txt := strings.TrimSpace(strings.TrimSuffix(buf.String(), "*"))
	return comment, txt, nil
}

func isNumberStart(c rune) bool {
	return (c >= '0' && c <= '9') || c == '.'
}

func isWordChar(c rune) bool {
	return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
}
