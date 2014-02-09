package dialect

import "database/sql"

type Dialect interface {
	InsertAutoIncrement(stmt *sql.Stmt, bind ...interface{}) (int64, error)
	SqlType(column interface{}, size int) string
	SqlPrimaryKey(column interface{}, size int) string
	Quote(string) string
}

func New(driver string) Dialect {

	switch driver {
	case "mysql":
		return &mysql{}
	case "sqlite3":
		return &sqlite3{}
	}
	return nil
}

//-- Helper functions ---------------
func defaultInsertAutoIncrement(stmt *sql.Stmt, bind ...interface{}) (int64, error) {
	res, err := stmt.Exec(bind...)
	if err != nil {
		return 0, err
	}

	return res.LastInsertId()
}
