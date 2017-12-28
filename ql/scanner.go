package ql

import (
	"unicode"
	"unicode/utf8"
	"strings"
)

type Scanner struct {
	data   []byte
	index  int
	offset int
}

var eof = rune(0)

func NewScanner(ql string) *Scanner {
	return &Scanner{
		data: []byte(ql),
	}
}

//Scan for the next token
func (s *Scanner) Scan() (tok Token, lit string) {
	ch := s.read()

	//consume whitespace
	for ch != eof && isWhitespace(ch) {
		ch = s.read()
	}

	//set the start offset
	s.offset = s.index - utf8.RuneLen(ch)

	switch {
	case isIdentRune(ch, 0):
		s.scanIdent()
		str := s.value()
		if tok, ok := isReserved(str); ok {
			return tok, strings.ToUpper(str)
		}
		return IDENT, str

	case unicode.IsDigit(ch):
		tok = s.scanNumber()
		return tok, s.value()

	default:
		switch ch {
		case eof:
			return EOF, ""

		case '\'', '"', '`':
			s.scanString(ch)
			return STRING, s.value()

		case '.': //maybe float (number)
			if unicode.IsDigit(s.peek()) {
				s.scanFloat()
				return FLOAT, s.value()
			}
			return DOT, "."

		case '?':
			return PLACEHOLDER, "?"

		case '(':
			return LEFT_PARENTHESIS, "("

		case ')':
			return RIGHT_PARENTHESIS, ")"

		case '=', '!', '>', '<', '*', '%', '/', '-', '+':
			return OPERATOR, s.value()
		}
	}
	return ILLEGAL, s.value()
}


// value returns the token
func (s *Scanner) value() string {
	if s.offset == s.index {
		return ""
	}
	return string(s.data[s.offset:s.index])
}

func (s *Scanner) scanString(quote rune) {
	ch := s.read()
	for ch != quote {
		switch ch {
		case eof:
			//s.error("literal not terminated")
			return
		case quote:
			return
		case '\\':
			s.read()
		}
		ch = s.read()
	}
	return
}

func (s *Scanner) scanNumber() Token {
	for ch := s.read(); ch != eof; ch = s.read() {
		if ch == '.' {
			s.scanFloat()
			return FLOAT
		} else if !unicode.IsDigit(ch) {
			s.index -= utf8.RuneLen(ch)
			return INTEGER
		}
	}
	return INTEGER
}

func (s *Scanner) scanFloat() {
	for ch := s.read(); ch != eof; ch = s.read() {
		if !unicode.IsDigit(ch) {
			s.index -= utf8.RuneLen(ch)
			return
		}
	}
}

// scanIdent consumes the current rune and all contiguous ident runes.
func (s *Scanner) scanIdent() {
	for ch := s.read(); ch != eof; ch = s.read() {
		if ch == '.' {
			if !isIdentRune(s.peek(), 0) {
				s.index -= utf8.RuneLen(ch)
				return
			}
			ch = s.read()
		} else if !isIdentRune(ch, 1) {
			s.index -= utf8.RuneLen(ch)
			return
		}
	}
}

// isWhitespace returns true if the rune is a space, tab, or newline.
func isWhitespace(ch rune) bool {
	return ch == ' ' || ch == '\t' || ch == '\n'
}

// isIdentRune checks if the character match the identifier pattern
func isIdentRune(ch rune, pos int) bool {
	return ch == '_' || unicode.IsLetter(ch) || (unicode.IsDigit(ch) && pos > 0)
}

// read a rune
func (s *Scanner) read() rune {
	if s.index >= len(s.data) {
		return eof
	}

	c := s.data[s.index]
	if c < utf8.RuneSelf {
		s.index++
		return rune(c)
	}
	r, n := utf8.DecodeRune(s.data[s.index:])
	s.index += n
	return r
}

// peek for the rune, does not advance
func (s *Scanner) peek() rune {
	if s.index >= len(s.data) {
		return eof
	}

	c := s.data[s.index]
	if c < utf8.RuneSelf {
		return rune(c)
	}
	r, _ := utf8.DecodeRune(s.data[s.index:])
	return r
}
