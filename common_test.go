package storm

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	. "gopkg.in/check.v1"
)

//hook up the testing framework to test
func Test(t *testing.T) { TestingT(t) }

type testCustomType int64

func (tct *testCustomType) Scan(value interface{}) (err error) {
	switch v := value.(type) {
	case []byte:
		var in int
		in, err = strconv.Atoi(string(v))
		*tct = (testCustomType)(in)
		return
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
	return errors.New("Cannot convert input to a custom type")
}

func (tct testCustomType) Value() (driver.Value, error) {
	return int64(tct), nil
}

type testRelatedStructure struct {
	Id              int
	TestStructureId int
	Name            string
}

type testStructure struct {
	Id        int
	Name      string
	TestSlice []testRelatedStructure

	//test invoke params
	onInsertInvoked      bool
	onPostInserteInvoked bool
	onUpdateInvoked      bool
	onPostUpdateInvoked  bool
	onDeleteInvoked      bool
	onPostDeleteInvoked  bool
	onInitInvoked        bool
}

//all posibile callbacks
func (t *testStructure) OnInsert()     { t.onInsertInvoked = true }
func (t *testStructure) OnPostInsert() { t.onPostInserteInvoked = true }
func (t *testStructure) OnUpdate()     { t.onUpdateInvoked = true }
func (t *testStructure) OnPostUpdate() { t.onPostUpdateInvoked = true }
func (t *testStructure) OnDelete()     { t.onDeleteInvoked = true }
func (t *testStructure) OnPostDelete() { t.onPostDeleteInvoked = true }
func (t *testStructure) OnInit()       { t.onInitInvoked = true }

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
}

func newTestStorm() *Storm {
	s, err := Open(`sqlite3`, `:memory:`)
	if err != nil {
		panic(err)
	}

	s.Log(log.New(ioutil.Discard, "", 0))
	s.RegisterStructure((*testStructure)(nil))
	s.RegisterStructure((*testRelatedStructure)(nil))
	s.RegisterStructure((*testAllTypeStructure)(nil))
	s.db.Exec("DROP TABLE `test_structure`")
	s.db.Exec("CREATE TABLE `test_structure` (`id` INTEGER PRIMARY KEY, `name` TEXT)")
	s.db.Exec("CREATE TABLE `test_related_structure` (`id` INTEGER PRIMARY KEY, test_structure_id INTEGER, `name` TEXT)")
	s.db.Exec("DROP TABLE `test_all_type_structure`")
	s.db.Exec("CREATE TABLE `test_all_type_structure` " +
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
	s.RegisterStructure((*testStructure)(nil))
	s.RegisterStructure((*testRelatedStructure)(nil))
	s.RegisterStructure((*testAllTypeStructure)(nil))
	s.db.Exec("DROP TABLE `test_structure`")
	s.db.Exec("CREATE TABLE `test_structure` (`id` INTEGER PRIMARY KEY, `name` TEXT)")
	s.db.Exec("CREATE TABLE `test_related_structure` (`id` INTEGER PRIMARY KEY, test_structure_id INTEGER, `name` TEXT)")
	s.db.Exec("DROP TABLE `test_all_type+structure`")
	s.db.Exec("CREATE TABLE `test_all_type_structure` " +
		"(`id` INTEGER PRIMARY KEY,`test_custom_type` INTEGER,`time` DATETIME,`byte` BLOB,`string` TEXT,`int` INTEGER,`int64` BIGINT," +
		"`float64` REAL,`bool` BOOL,`null_string` TEXT,`null_int` BIGINT,`null_float` REAL,`null_bool` BOOL)")
	s.db.SetMaxIdleConns(10)
	s.db.SetMaxOpenConns(10)

	return s
}

func assertEntity(actual *testStructure, expected *testStructure) error {
	if actual == nil {
		return errors.New(`nil record returned`)
	}

	if actual.Id != expected.Id || actual.Name != expected.Name {
		return fmt.Errorf("data mismatch expected `%v` but got `%v`", expected, actual)
	}

	return nil
}

func assertRelatedEntity(actual *testRelatedStructure, expected *testRelatedStructure) error {
	if actual == nil {
		return errors.New(`nil record returned`)
	}

	if actual.Id != expected.Id || actual.TestStructureId != expected.TestStructureId || actual.Name != expected.Name {
		return fmt.Errorf("data mismatch expected `%v` but got `%v`", expected, actual)
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
