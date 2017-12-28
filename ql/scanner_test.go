package ql

import (
	"testing"
)

//on item

type tokenValue struct {
	token Token
	value string
}

func TestScanner(t *testing.T) {
	testCases := []struct {
		description string
		expression  string
		tokens      []tokenValue
	}{
		{
			expression: `abc = ?`,
			tokens: []tokenValue{
				{IDENT, "abc"},
				{OPERATOR, "="},
				{PLACEHOLDER, "?"},
			},
		}, {
			expression: `abc.defg.c = ?`,
			tokens: []tokenValue{
				{IDENT, "abc.defg.c"},
				{OPERATOR, "="},
				{PLACEHOLDER, "?"},
			},
		}, {
			expression: `MAX(abc.defg) > 0`,
			tokens: []tokenValue{
				{FUNCTION, "MAX"},
				{LEFT_PARENTHESIS, "("},
				{IDENT, "abc.defg"},
				{RIGHT_PARENTHESIS, ")"},
				{OPERATOR, ">"},
				{INTEGER, "0"},
			},
		}, {
			expression: `LENGTH(abc.defg) != abc.defg`,
			tokens: []tokenValue{
				{FUNCTION, "LENGTH"},
				{LEFT_PARENTHESIS, "("},
				{IDENT, "abc.defg"},
				{RIGHT_PARENTHESIS, ")"},
				{OPERATOR, "!"},
				{OPERATOR, "="},
				{IDENT, "abc.defg"},
			},
		}, {
			expression: `abc.defg - abc.c = ?`,
			tokens: []tokenValue{
				{IDENT, "abc.defg"},
				{OPERATOR, "-"},
				{IDENT, "abc.c"},
				{OPERATOR, "="},
				{PLACEHOLDER, "?"},
			},
		}, {
			expression: `abc.defg NOT IN ?`,
			tokens: []tokenValue{
				{IDENT, "abc.defg"},
				{OPERATOR, "NOT"},
				{OPERATOR, "IN"},
				{PLACEHOLDER, "?"},
			},
		}, {
			expression: `abc.defg = "test quoted text"`,
			tokens: []tokenValue{
				{IDENT, "abc.defg"},
				{OPERATOR, "="},
				{STRING, `"test quoted text"`},
			},
		}, {
			expression: `abc.defg = "TEST with escaped [\"] quote "`,
			tokens: []tokenValue{
				{IDENT, "abc.defg"},
				{OPERATOR, "="},
				{STRING, `"TEST with escaped [\"] quote "`},
			},
		}, {
			expression: "abc.defg = `TEST raw quoted text`",
			tokens: []tokenValue{
				{IDENT, "abc.defg"},
				{OPERATOR, "="},
				{STRING, "`TEST raw quoted text`"},
			},
		}, {
			expression: "abc.defg = `TEST raw quoted text with escaped [\\`] quote`",
			tokens: []tokenValue{
				{IDENT, "abc.defg"},
				{OPERATOR, "="},
				{STRING, "`TEST raw quoted text with escaped [\\`] quote`"},
			},
		}, {
			expression: `abc.defg != 1234`,
			tokens: []tokenValue{
				{IDENT, "abc.defg"},
				{OPERATOR, "!"},
				{OPERATOR, "="},
				{INTEGER, "1234"},
			},
		}, {
			expression: `abc.defg = .123`,
			tokens: []tokenValue{
				{IDENT, "abc.defg"},
				{OPERATOR, "="},
				{FLOAT, ".123"},
			},
		}, {
			expression: `abc.defg = 45.123`,
			tokens: []tokenValue{
				{IDENT, "abc.defg"},
				{OPERATOR, "="},
				{FLOAT, "45.123"},
			},
		},
	}

	for testNum, tc := range testCases {
		s := NewScanner(tc.expression)
		i := 0
		for tok, val := s.Scan(); tok != EOF; tok, val = s.Scan() {
			i++
			if i > len(tc.tokens) {
				t.Errorf("not expecting (%d) tokens but got more tokens for test (%d) %s", len(tc.tokens), testNum, tc.description)
				break
			}

			if tc.tokens[i-1].token != tok {
				t.Errorf("expecting token %d(%s) for pos %d but got token %d(%s) for test (%d) %s", tc.tokens[i-1].token, TokenToString(tc.tokens[i-1].token), i, tok, TokenToString(tok), testNum, tc.description)
			}

			if tc.tokens[i-1].value != val {
				t.Errorf("expecting token value `%s` for pos %d but got value `%s` for test (%d) %s", tc.tokens[i-1].value, i, val, testNum, tc.description)
			}
		}

		if i != len(tc.tokens) {
			t.Errorf("expected (%d) tokens but got more (%d) tokens for test (%d) %s", len(tc.tokens), i, testNum, tc.description)
		}

	}
}
