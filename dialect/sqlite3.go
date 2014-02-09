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
		return "DATETIME"
	case bool, sql.NullBool:
		return "BOOL"
	case int, int8, int16, int32, uint, uint8, uint16, uint32:
		return "INTEGER"
	case int64, uint64, sql.NullInt64:
		return "BIGINT"
	case float32, float64, sql.NullFloat64:
		return "REAL"
	case []byte:
		return "BLOB"
	case string, sql.NullString:
		if size > 0 && size < 65532 {
			return fmt.Sprintf("VARCHAR(%d)", size)
		} else {
			return "TEXT"
		}
	default:
		panic(fmt.Sprintf("Invalid sql type for sqlite3 (%v)", column))
	}
}

func (*sqlite3) SqlPrimaryKey(column interface{}, size int) string {
	switch column.(type) {
	case int, int8, int16, int32, uint, uint8, uint16, uint32, int64, uint64:
		return "PRIMARY KEY"
	default:
		panic("Invalid primary key type")
	}

}

func (*sqlite3) Quote(key string) string {
	return fmt.Sprintf("`%s`", key)
}
