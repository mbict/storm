package storm

import (
	"database/sql"
)

//@todo need to be a interface
type Dialect interface {
	InsertAutoIncrement(stmt *sql.Stmt, bind ...interface{}) (int64, error)
}

//-- SQLITE -------------------------

type SqliteDialect struct {
}

func (d *SqliteDialect) InsertAutoIncrement(stmt *sql.Stmt, bind ...interface{}) (int64, error) {

	res, err := stmt.Exec(bind...)
	if err != nil {
		return 0, err
	}

	return res.LastInsertId()
}

//-- MySql -------------------------
type MySqlDialect struct {
}

func (d *MySqlDialect) InsertAutoIncrement(stmt *sql.Stmt, bind ...interface{}) (int64, error) {

	res, err := stmt.Exec(bind...)
	if err != nil {
		return 0, err
	}

	return res.LastInsertId()
}
