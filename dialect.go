package storm

import (
	"database/sql"
)

type Dialect interface {
	InsertAutoIncrement(stmt *sql.Stmt, bind ...interface{}) (int64, error)
}

//-- SQLITE ------------------------
type SqliteDialect struct {
}

func (d *SqliteDialect) InsertAutoIncrement(stmt *sql.Stmt, bind ...interface{}) (int64, error) {
	return defaultInsertAutoIncrement(stmt, bind...)
}

//-- MySql -------------------------
type MySqlDialect struct {
}

func (d *MySqlDialect) InsertAutoIncrement(stmt *sql.Stmt, bind ...interface{}) (int64, error) {
	return defaultInsertAutoIncrement(stmt, bind...)
}

//-- Helper functions ---------------
func defaultInsertAutoIncrement(stmt *sql.Stmt, bind ...interface{}) (int64, error) {
	res, err := stmt.Exec(bind...)
	if err != nil {
		return 0, err
	}

	return res.LastInsertId()
}
