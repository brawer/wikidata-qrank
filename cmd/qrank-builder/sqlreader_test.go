// SPDX-FileCopyrightText: 2024 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"io"
	"strings"
	"testing"
)

func TestSQLLexer(t *testing.T) {
	for _, tc := range []struct{ input, want string }{
		{"", ""},
		{" ", ""},
		{"✱", "Unexpected[✱]"},
		{"-- MySQL dump 10.19\n", "Comment[MySQL dump 10.19]"},
		{" ABC\nNULL ", "Word[ABC] Word[NULL]"},
		{"DROP TABLE `page_props`;", "Word[DROP] Word[TABLE] Name[page_props] Semicolon"},
		{"-", "Minus"},
		{"-A", "Minus Word[A]"},
		{"- A", "Minus Word[A]"},
		{"42", "Number[42]"},
		{"0.1", "Number[0.1]"},
		{".7, -42, 1.8", "Number[.7] Comma Number[-42] Comma Number[1.8]"},
		{"- 42", "Minus Number[42]"},
		{"int(10)", "Word[int] LeftParen Number[10] RightParen"},
		{"'foo'", "Text[foo]"},
		{"/", "Slash"},
		{"2/3", "Number[2] Slash Number[3]"},
		{"/* foo */", "Comment[foo]"},
	} {
		if got := lex(tc.input); got != tc.want {
			t.Errorf("input %v: got %v, want %v", tc.input, got, tc.want)
		}
	}
}

// Lex returns a debug string for the lexed input.
func lex(s string) string {
	lexer := newSQLLexer(strings.NewReader(s))
	var buf strings.Builder
	for {
		token, txt, err := lexer.read()
		if err == io.EOF {
			return buf.String()
		} else if err != nil {
			return err.Error()
		}
		if buf.Len() > 0 {
			buf.WriteRune(' ')
		}
		switch token {
		case unexpected:
			buf.WriteString("Unexpected")
		case word:
			buf.WriteString("Word")
		case name:
			buf.WriteString("Name")
		case number:
			buf.WriteString("Number")
		case text:
			buf.WriteString("Text")
		case comment:
			buf.WriteString("Comment")
		case leftParen:
			buf.WriteString("LeftParen")
		case rightParen:
			buf.WriteString("RightParen")
		case comma:
			buf.WriteString("Comma")
		case semicolon:
			buf.WriteString("Semicolon")
		case minus:
			buf.WriteString("Minus")
		case slash:
			buf.WriteString("Slash")
		default:
			buf.WriteString("?")
		}

		if txt != "" {
			buf.WriteRune('[')
			buf.WriteString(txt)
			buf.WriteRune(']')
		}
	}
}
