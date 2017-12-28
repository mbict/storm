package storm

import (
	"github.com/mbict/storm/dialect"
	"log"
	"reflect"
)

type dbContext interface {
	db() sqlCommon
	storm() Storm
	dialect() dialect.Dialect
	table(t reflect.Type) (tbl *schema, ok bool)
	tableByName(s string) (tbl *schema, ok bool)
	logger() *log.Logger
}

