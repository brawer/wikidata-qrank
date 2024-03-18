// SPDX-FileCopyrightText: 2024 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"bufio"
	"errors"
	"io"
	"strings"
	"unicode"
)

// SQLReader parses Mediawiki SQL dumps.
type SQLReader struct {
	lexer   sqlLexer
	columns []string // The names of database table columns, such as ["pp_page", "pp_propname"]
}

var parseError = errors.New("sql parse error")

func NewSQLReader(r io.Reader) (*SQLReader, error) {
	rd := &SQLReader{
		lexer:   sqlLexer{bufio.NewReader(r)},
		columns: make([]string, 0, 8),
	}

	if err := rd.skipUntil(word, "CREATE"); err != nil {
		return nil, err
	}
	if err := rd.parseCreate(); err != nil {
		return nil, err
	}

	if err := rd.skipUntil(word, "INSERT"); err != nil {
		return nil, err
	}
	if err := rd.skipUntil(word, "VALUES"); err != nil {
		return nil, err
	}

	return rd, nil
}

func (r *SQLReader) Columns() []string {
	return r.columns
}

func (r *SQLReader) Read() ([]string, error) {
	token, _, err := r.readToken()
	if err != nil {
		return nil, err
	}
	if token == semicolon {
		return nil, nil
	}

	if token == comma {
		token, _, err = r.readToken()
		if err != nil {
			return nil, err
		}
	}

	if token != leftParen {
		return nil, parseError
	}

	row := make([]string, 0, len(r.columns))
	for {
		token, txt, err := r.readToken()
		if err != nil {
			return nil, err
		}
		if token == number || token == text {
			row = append(row, txt)
		} else if token == word && txt == "NULL" {
			row = append(row, "")
		} else {
			return nil, parseError
		}

		token, _, err = r.readToken()
		if token == comma {
			continue
		} else if token == rightParen {
			break
		} else {
			return nil, parseError
		}
	}

	return row, nil
}

func (r *SQLReader) parseCreate() error {
	if err := r.skipUntil(leftParen, ""); err != nil {
		return err
	}
	for {
		token, tokenText, err := r.readToken()
		if err != nil {
			return err
		}
		if token != name {
			return r.skipUntil(semicolon, "")
		}
		r.columns = append(r.columns, tokenText)
		if err := r.skipUntilEither(comma, rightParen); err != nil {
			return err
		}
	}
}

func (r *SQLReader) skipUntil(token sqlToken, tokenText string) error {
	for {
		tok, txt, err := r.lexer.read()
		if err != nil {
			return err
		}
		if tok == token && txt == tokenText {
			return nil
		}
	}
}

func (r *SQLReader) skipUntilEither(t1 sqlToken, t2 sqlToken) error {
	parenDepth := 0
	for {
		tok, _, err := r.readToken()
		if err != nil {
			return err
		}
		if tok == leftParen {
			parenDepth += 1
			continue
		}
		if tok == rightParen && parenDepth > 0 {
			parenDepth -= 1
			continue
		}
		if tok == t1 || tok == t2 {
			return nil
		}
	}
}

func (r *SQLReader) readToken() (sqlToken, string, error) {
	for {
		got, gotTxt, err := r.lexer.read()
		if got == comment && err == nil {
			continue
		}
		return got, gotTxt, err
	}
}

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
