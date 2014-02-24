package storm

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type testCustomType int64

func (tct *testCustomType) Scan(value interface{}) (err error) {
	switch v := value.(type) {
	case string:
		var in int
		in, err = strconv.Atoi(v)
		*tct = (testCustomType)(in)
		return
	case int64:
		*tct = (testCustomType)(v)
		return
	case int:
		*tct = (testCustomType)(v)
		return
	}
	return nil
}

func (tct *testCustomType) Value() (driver.Value, error) {
	return int64(*tct), nil
}

type testStructure struct {
	Id   int
	Name string
}

type testAllTypeStructure struct {
	Id             int
	TestCustomType testCustomType `db:"type(int)"`
	Time           time.Time
	Byte           []byte
	String         string
	Int            int
	Int64          int64
	Float64        float64
	Bool           bool
	NullString     sql.NullString
	NullInt        sql.NullInt64
	NullFloat      sql.NullFloat64
	NullBool       sql.NullBool

	//test invoke params
	beforeInsertInvoked bool
	afterInsertInvoked  bool
	beforeUpdateInvoked bool
	afterUpdateInvoked  bool
	beforeDeleteInvoked bool
	afterDeleteInvoked  bool
	beforeFindInvoked   bool
	afterFindInvoked    bool
}

//all posibile callbacks
func (t *testAllTypeStructure) beforeInsert() { t.beforeInsertInvoked = true }
func (t *testAllTypeStructure) afterInsert()  { t.afterInsertInvoked = true }
func (t *testAllTypeStructure) beforeUpdate() { t.beforeUpdateInvoked = true }
func (t *testAllTypeStructure) afterUpdate()  { t.afterUpdateInvoked = true }
func (t *testAllTypeStructure) beforeDelete() { t.beforeDeleteInvoked = true }
func (t *testAllTypeStructure) afterDelete()  { t.afterDeleteInvoked = true }
func (t *testAllTypeStructure) beforeFind()   { t.beforeFindInvoked = true }
func (t *testAllTypeStructure) afterFind()    { t.afterFindInvoked = true }

func newTestStorm() *Storm {
	s, err := Open(`sqlite3`, `:memory:`)
	if err != nil {
		panic(err)
	}

	s.Log(log.New(ioutil.Discard, "", 0))
	s.RegisterStructure((*testStructure)(nil), `testStructure`)
	s.RegisterStructure((*testAllTypeStructure)(nil), `testAllTypeStructure`)
	s.db.Exec("DROP TABLE `testStructure`")
	s.db.Exec("CREATE TABLE `testStructure` (`id` INTEGER PRIMARY KEY, `name` TEXT)")
	s.db.Exec("DROP TABLE `testAllTypeStructure`")
	s.db.Exec("CREATE TABLE `testAllTypeStructure` " +
		"(`id` INTEGER PRIMARY KEY,`test_custom_type` INTEGER,`time` DATETIME,`byte` BLOB,`string` TEXT,`int` INTEGER,`int64` BIGINT," +
		"`float64` REAL,`bool` BOOL,`null_string` TEXT,`null_int` BIGINT,`null_float` REAL,`null_bool` BOOL)")
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
	s.Log(log.New(ioutil.Discard, "", 0))
	s.RegisterStructure((*testStructure)(nil), `testStructure`)
	s.RegisterStructure((*testAllTypeStructure)(nil), `testAllTypeStructure`)
	s.db.Exec("DROP TABLE `testStructure`")
	s.db.Exec("CREATE TABLE `testStructure` (`id` INTEGER PRIMARY KEY, `name` TEXT)")
	s.db.Exec("DROP TABLE `testAllTypeStructure`")
	s.db.Exec("CREATE TABLE `testAllTypeStructure` " +
		"(`id` INTEGER PRIMARY KEY,`test_custom_type` INTEGER,`time` DATETIME,`byte` BLOB,`string` TEXT,`int` INTEGER,`int64` BIGINT," +
		"`float64` REAL,`bool` BOOL,`null_string` TEXT,`null_int` BIGINT,`null_float` REAL,`null_bool` BOOL)")
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
