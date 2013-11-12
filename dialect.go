// Copyright (c) 2013 Michael Boke (MBIct). All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

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
