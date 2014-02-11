package storm

import (
	"database/sql"
	"errors"
	"fmt"
	"io/ioutil"

	_ "github.com/mattn/go-sqlite3"
)

type testStructure struct {
	Id   int
	Name string
}

func newTestStorm() *Storm {
	s, err := Open(`sqlite3`, `:memory:`)
	if err != nil {
		panic(err)
	}

	s.RegisterStructure((*testStructure)(nil), `testStructure`)
	s.db.Exec("DROP TABLE `testStructure`")
	s.db.Exec("CREATE TABLE `testStructure` (`id` INTEGER PRIMARY KEY, `name` TEXT)")
	s.db.SetMaxIdleConns(10)
	s.db.SetMaxOpenConns(10)

	return s
}

func newTestStormFile() *Storm {

	//create unique temporary datastore
	tmp, err := ioutil.TempFile("", "storm_test.sqlite_")
	if err != nil {
		panic(err)
	}
	tmp.Close()

	s, _ := Open(`sqlite3`, `file:`+tmp.Name()+`?mode=rwc`)
	s.RegisterStructure((*testStructure)(nil), `testStructure`)
	s.db.Exec("DROP TABLE `testStructure`")
	s.db.Exec("CREATE TABLE `testStructure` (`id` INTEGER PRIMARY KEY, `name` TEXT)")
	s.db.SetMaxIdleConns(10)
	s.db.SetMaxOpenConns(10)

	return s
}

func assertEntity(actual *testStructure, expected *testStructure) error {
	if actual == nil {
		return errors.New(`Nil record returned`)
	}

	if actual == expected {
		return errors.New(fmt.Sprintf("Data mismatch expected `%v` but got `%v`", expected, actual))
	}

	return nil
}

func assertTableExist(table string, db sqlCommon) (result int, err error) {

	//sqlite3 way
	res := db.QueryRow(`SELECT 1 FROM sqlite_master WHERE type='table' AND name='` + table + `'`)

	err = res.Scan(&result)
	if err == sql.ErrNoRows {
		err = nil
		result = 0
	}
	return
}
