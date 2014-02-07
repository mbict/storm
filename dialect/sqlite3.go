package dialect

import (
	"database/sql"
	"fmt"
	"time"
)

type sqlite3 struct {
}

func (*sqlite3) InsertAutoIncrement(stmt *sql.Stmt, bind ...interface{}) (int64, error) {
	return defaultInsertAutoIncrement(stmt, bind...)
}

func (*sqlite3) SqlType(column interface{}, size int) string {
	switch column.(type) {
	case time.Time:
		return "datetime"
	case bool, sql.NullBool:
		return "bool"
	case int, int8, int16, int32, uint, uint8, uint16, uint32:
		return "integer"
	case int64, uint64, sql.NullInt64:
		return "bigint"
	case float32, float64, sql.NullFloat64:
		return "real"
	case []byte:
		return "blob"
	case string, sql.NullString:
		if size > 0 && size < 65532 {
			return fmt.Sprintf("varchar(%d)", size)
		} else {
			return "text"
		}
	default:
		panic(fmt.Sprintf("Invalid sql type for sqlite3 (%v)", column))
	}
}

func (*sqlite3) SqlPrimaryKey(column interface{}, size int) string {
	return "INTEGER PRIMARY KEY"
}

func (*sqlite3) Quote(key string) string {
	return fmt.Sprintf("`%s`", key)
}
