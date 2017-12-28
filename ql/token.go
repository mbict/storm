package ql

import "strings"

type Token int

const (
	ILLEGAL Token = iota
	EOF

	LEFT_PARENTHESIS   // (
	RIGHT_PARENTHESIS  // )
	STRING             // "text in between"
	INTEGER            // 1234
	FLOAT              // 123.45  or .123
	PLACEHOLDER        // ?
	IDENT              //table, column, functions etc
	OPERATOR           // =, !=, <>, >, <, >=, <=, +, -, %, *, /
	DOT                // .
	FUNCTION           // MIN, MAX, LENGTH, AVG, GROUP
	NULL_VALUE         // NULL, nil
)

var tokenToString = map[Token]string{
	ILLEGAL:           "illegal",
	EOF:               "EOF",
	LEFT_PARENTHESIS:  "left parenthesis",
	RIGHT_PARENTHESIS: "right parenthesis",
	STRING:            "string",
	INTEGER:           "integer",
	FLOAT:             "float",
	PLACEHOLDER:       "placeholder",
	IDENT:             "ident",
	OPERATOR:          "operator",
	DOT:               "dot",
	FUNCTION:          "function",
	NULL_VALUE:        "null",
}

var functionIdents = map[string]Token{
	"IN":     OPERATOR,
	"NOT":    OPERATOR,
	"NULL":   NULL_VALUE,
	"NIL":    NULL_VALUE,
	"MIN":    FUNCTION,
	"MAX":    FUNCTION,
	"LENGTH": FUNCTION,
	"AVG":    FUNCTION,
	"GROUP":  FUNCTION,
	"COUNT":  FUNCTION,
	"SUM":    FUNCTION,
}

func isReserved(ident string) (Token, bool) {
	v, ok := functionIdents[strings.ToUpper(ident)]
	return v, ok
}

func TokenToString(t Token) string {
	if v, ok := tokenToString[t]; ok {
		return v
	}
	return ""
}
