package storm

import (
	"database/sql"
	"database/sql/driver"
	"errors"
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

type testStructure struct {
	Id   int
	Name string

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
	PtrString      *string
	PtrInt         *int
	PtrInt64       *int64
	PtrFloat       *float64
	PtrBool        *bool
}

type Person struct {
	Id                int
	Name              string
	Address           *Address
	AddressId         int
	OptionalAddress   *Address
	OptionalAddressId sql.NullInt64
	Telephones        []*Telephone

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
func (person *Person) OnInsert()     { person.onInsertInvoked = true }
func (person *Person) OnPostInsert() { person.onPostInserteInvoked = true }
func (person *Person) OnUpdate()     { person.onUpdateInvoked = true }
func (person *Person) OnPostUpdate() { person.onPostUpdateInvoked = true }
func (person *Person) OnDelete()     { person.onDeleteInvoked = true }
func (person *Person) OnPostDelete() { person.onPostDeleteInvoked = true }
func (person *Person) OnInit()       { person.onInitInvoked = true }

type Address struct {
	Id        int
	Line1     string
	Line2     string
	Country   *Country
	CountryId int
}

type Country struct {
	Id   int
	Name string
}

type Telephone struct {
	Id       int
	PersonId int
	Number   string
}
