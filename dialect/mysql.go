package dialect

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/mbict/null"
)

type mysql struct {
}

func (*mysql) InsertAutoIncrement(stmt *sql.Stmt, bind ...interface{}) (int64, error) {
	return defaultInsertAutoIncrement(stmt, bind...)
}

func (*mysql) SqlType(column interface{}, size int) string {

	switch column.(type) {
	case time.Time, *time.Time:
		return "DATETIME"
	case bool, sql.NullBool, null.Bool, *bool:
		return "BOOLEAN"
	case int, int8, int16, int32, uint, uint8, uint16, uint32, *int, *int8, *int16, *int32, *uint, *uint8, *uint16, *uint32:
		return "INT"
	case int64, uint64, sql.NullInt64, null.Int, *int64, *uint64:
		return "BIGINT"
	case float32, float64, sql.NullFloat64, null.Float, *float32, *float64:
		return "DOUBLE"
	case []byte:
		if size > 0 && size < 65532 {
			return fmt.Sprintf("VARBINARY(%d)", size)
		} else {
			return "LONGBLOB"
		}
	case string, sql.NullString, null.String, *string:
		if size > 0 && size < 65532 {
			return fmt.Sprintf("VARCHAR(%d)", size)
		} else {
			return "LONGTEXT"
		}
	default:
		panic(fmt.Sprintf("Invalid sql type for mysql (%#v / %T)", column))
	}
}

func (*mysql) SqlPrimaryKey(column interface{}, size int) string {
	switch column.(type) {
	case int, int8, int16, int32, uint, uint8, uint16, uint32, int64, uint64:
		return "NOT NULL AUTO_INCREMENT PRIMARY KEY"
	default:
		panic("Invalid primary key type")
	}
}

func (*mysql) Quote(key string) string {
	return fmt.Sprintf("`%s`", key)
}
