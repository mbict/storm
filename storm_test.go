package storm

import (
	"database/sql"
	"fmt"
	"reflect"
	"time"

	. "gopkg.in/check.v1"
)

type testErrorCallbackStruct struct{ Id int }

func (testErrorCallbackStruct) OnDelete() error {
	return fmt.Errorf("delete callback error")
}

func (testErrorCallbackStruct) OnInsert() error {
	return fmt.Errorf("insert callback error")
}

func (testErrorCallbackStruct) OnUpdate() error {
	return fmt.Errorf("update callback error")
}

//*** test suite setup ***/
type stormSuite struct {
	db *Storm
}

var _ = Suite(&stormSuite{})

func (s *stormSuite) SetUpTest(c *C) {

	var err error
	s.db, err = Open(`sqlite3`, `:memory:`)
	c.Assert(s.db, NotNil)
	c.Assert(err, IsNil)

	s.db.SetMaxIdleConns(10)
	s.db.SetMaxOpenConns(10)
}

/*** tests ***/

//need to return a instance to it self
func (s *stormSuite) TestStorm(c *C) {
	c.Assert(s.db.Storm(), Equals, s.db)
}

func (s *stormSuite) TestClose(c *C) {
	//we dont close the connetion for the test, we create a new one
	db, err := Open(`sqlite3`, `:memory:`)

	c.Assert(err, IsNil)
	c.Assert(db, NotNil)
	c.Assert(db.Close(), IsNil)

	//check by running sql query to force error
	_, err = db.DB().Exec("SELECT * FROM DUAL")
	c.Assert(err, ErrorMatches, "sql: database is closed")
}

//not realy a usefull test, but now we know ping doesnt generate a error
func (s *stormSuite) TestPing(c *C) {
	c.Assert(s.db.Ping(), IsNil)
}

func (s *stormSuite) TestRegisterStructure_Object(c *C) {
	person := Person{}
	c.Assert(s.db.RegisterStructure(person), IsNil)

	tbl, ok := s.db.tables[reflect.TypeOf(Person{})]
	c.Assert(ok, Equals, true)
	c.Assert(tbl, NotNil)
}

func (s *stormSuite) TestRegisterStructure_ObjectPtr(c *C) {
	person := &Person{}
	c.Assert(s.db.RegisterStructure(person), IsNil)
	tbl, ok := s.db.tables[reflect.TypeOf(Person{})]
	c.Assert(ok, Equals, true)
	c.Assert(tbl, NotNil)
}

func (s *stormSuite) TestRegisterStructure_NilType(c *C) {
	c.Assert(s.db.RegisterStructure((*Person)(nil)), IsNil)
	tbl, ok := s.db.tables[reflect.TypeOf(Person{})]
	c.Assert(ok, Equals, true)
	c.Assert(tbl, NotNil)
}

func (s *stormSuite) TestRegisterStructure_ErrorNotAStructure(c *C) {
	c.Assert(s.db.RegisterStructure((*int)(nil)), ErrorMatches, `provided input is not a structure type`)
	c.Assert(s.db.RegisterStructure(string("test")), ErrorMatches, `provided input is not a structure type`)
}

func (s *stormSuite) TestRegisterStructure_ErrorDuplicateRegister(c *C) {
	c.Assert(s.db.RegisterStructure((*Person)(nil)), IsNil)
	c.Assert(s.db.RegisterStructure((*Person)(nil)), ErrorMatches, `duplicate structure, 'storm.Person' already exists`)
}

func (s *stormSuite) TestRegisterStructure_ResolveRelations(c *C) {
	c.Assert(s.db.RegisterStructure((*Person)(nil)), IsNil)
	c.Assert(s.db.RegisterStructure((*Address)(nil)), IsNil)
	c.Assert(s.db.RegisterStructure((*Telephone)(nil)), IsNil)

	tbl, ok := s.db.tables[reflect.TypeOf(Person{})]
	c.Assert(ok, Equals, true)
	c.Assert(tbl, NotNil)

	tblTelephone, ok := s.db.tables[reflect.TypeOf(Telephone{})]
	c.Assert(ok, Equals, true)
	c.Assert(tblTelephone, NotNil)

	tblAddress, ok := s.db.tables[reflect.TypeOf(Address{})]
	c.Assert(ok, Equals, true)
	c.Assert(tblAddress, NotNil)

	//3 relations
	c.Assert(tbl.relations, HasLen, 3)

	//one to one (person.address -> address)
	c.Assert(tbl.relations[0].goIndex, DeepEquals, []int{2})
	c.Assert(tbl.relations[0].goSingularType, Equals, reflect.TypeOf((*Address)(nil)))
	c.Assert(tbl.relations[0].goType, Equals, reflect.TypeOf((*Address)(nil)))
	c.Assert(tbl.relations[0].name, Equals, "address")
	c.Assert(tbl.relations[0].relColumn, Equals, tbl.columns[2])
	c.Assert(tbl.relations[0].relTable, IsNil)

	//one to one (person.optional_address -> address)
	c.Assert(tbl.relations[1].goIndex, DeepEquals, []int{4})
	c.Assert(tbl.relations[1].goSingularType, Equals, reflect.TypeOf((*Address)(nil)))
	c.Assert(tbl.relations[1].goType, Equals, reflect.TypeOf((*Address)(nil)))
	c.Assert(tbl.relations[1].name, Equals, "optional_address")
	c.Assert(tbl.relations[1].relColumn, Equals, tbl.columns[3])
	c.Assert(tbl.relations[1].relTable, IsNil)

	//one to many (person.telephones -> telephone)
	c.Assert(tbl.relations[2].goIndex, DeepEquals, []int{6})
	c.Assert(tbl.relations[2].goSingularType, Equals, reflect.TypeOf((*Telephone)(nil)).Elem())
	c.Assert(tbl.relations[2].goType, Equals, reflect.TypeOf(([]*Telephone)(nil)))
	c.Assert(tbl.relations[2].name, Equals, "telephones")
	c.Assert(tbl.relations[2].relColumn, Equals, tblTelephone.columns[1])
	c.Assert(tbl.relations[2].relTable, Equals, tblTelephone)
}

//test alias (full test is done in query)
func (s *stormSuite) TestFind(c *C) {
	type testStruct struct{ Id int }
	var (
		single *testStruct
		slice  []*testStruct
	)
	c.Assert(s.db.RegisterStructure((*testStruct)(nil)), IsNil)
	_, err := s.db.DB().Exec("CREATE TABLE `test_struct` (`id` INTEGER PRIMARY KEY)")
	c.Assert(err, IsNil)
	c.Assert(s.db.Find(&single, "id = ?", 1), Equals, sql.ErrNoRows)
	c.Assert(s.db.Find(&slice, "id > ?", 1), Equals, sql.ErrNoRows)
}

func (s *stormSuite) TestDelete(c *C) {
	c.Assert(s.db.RegisterStructure((*testStructure)(nil)), IsNil)
	_, err := s.db.DB().Exec("CREATE TABLE `test_structure` (`id` INTEGER PRIMARY KEY, `name` TEXT)")
	c.Assert(err, IsNil)
	_, err = s.db.DB().Exec("INSERT INTO `test_structure` (`id`) VALUES (2)")
	c.Assert(err, IsNil)

	input := testStructure{Id: 2}
	c.Assert(s.db.Delete(&input), IsNil)

	//check if callback beforeDelete is called
	c.Assert(input.onDeleteInvoked, Equals, true)

	//check if callback AfterDelete is called
	c.Assert(input.onPostDeleteInvoked, Equals, true)

	//check if record is deleted
	c.Assert(s.db.Find(&input, "id = ?", 2), Equals, sql.ErrNoRows)
}

func (s *stormSuite) TestDelete_ErrorNotByReference(c *C) {
	c.Assert(s.db.Delete(Person{}), ErrorMatches, "provided input is not by reference")
}

func (s *stormSuite) TestDelete_ErrorNotRegistered(c *C) {
	type notRegisteredStruct struct{}
	c.Assert(s.db.Delete(&notRegisteredStruct{}), ErrorMatches, "no registered structure for `storm.notRegisteredStruct` found")
}

func (s *stormSuite) TestDelete_ErrorNotAStructure(c *C) {
	var notStruct int = 1
	c.Assert(s.db.Delete(&notStruct), ErrorMatches, "provided input is not a structure type")
	var notStructPtr *int = new(int)
	*notStructPtr = 1
	c.Assert(s.db.Delete(&notStructPtr), ErrorMatches, "provided input is not a structure type")
}

func (s *stormSuite) TestDelete_ErrorNullPointer(c *C) {
	input := (*Person)(nil)
	c.Assert(s.db.Delete(&input), ErrorMatches, "provided input is a nil pointer")
}

//force a sql error (not table exists)
func (s *stormSuite) TestDelete_ErrorSqlError(c *C) {
	input := Person{Id: 1}
	c.Assert(s.db.RegisterStructure((*Person)(nil)), IsNil)
	c.Assert(s.db.Delete(&input), ErrorMatches, "no such table: person")
}

func (s *stormSuite) TestDelete_ErrorOnDeleteCallback(c *C) {
	input := testErrorCallbackStruct{}
	c.Assert(s.db.RegisterStructure((*testErrorCallbackStruct)(nil)), IsNil)
	c.Assert(s.db.Delete(&input), ErrorMatches, "delete callback error")
}

func (s *stormSuite) TestSave_Insert(c *C) {
	c.Assert(s.db.RegisterStructure((*testStructure)(nil)), IsNil)
	_, err := s.db.DB().Exec("CREATE TABLE `test_structure` (`id` INTEGER PRIMARY KEY, `name` TEXT)")
	c.Assert(err, IsNil)
	_, err = s.db.DB().Exec("INSERT INTO `test_structure` (`id`, `name`) VALUES (1, 'first')")
	c.Assert(err, IsNil)
	_, err = s.db.DB().Exec("INSERT INTO `test_structure` (`id`, `name`) VALUES (2, 'wow 2nd')")
	c.Assert(err, IsNil)

	input := &testStructure{Id: 0, Name: "test inserted"}
	c.Assert(s.db.Save(&input), IsNil)
	c.Assert(input.Id, Equals, 3) // check pk

	//check if callback OnInsert is called
	c.Assert(input.onInsertInvoked, Equals, true)

	//check if callback OnInserted is called
	c.Assert(input.onPostInserteInvoked, Equals, true)

	//check if all the fields are correctly saved
	input = nil
	c.Assert(s.db.Find(&input, "id = ?", 3), IsNil)
	c.Assert(input.Id, Equals, 3)
	c.Assert(input.Name, Equals, "test inserted")
}

func (s *stormSuite) TestSave_Update(c *C) {
	c.Assert(s.db.RegisterStructure((*testStructure)(nil)), IsNil)
	_, err := s.db.DB().Exec("CREATE TABLE `test_structure` (`id` INTEGER PRIMARY KEY, `name` TEXT)")
	c.Assert(err, IsNil)
	_, err = s.db.DB().Exec("INSERT INTO `test_structure` (`id`, `name`) VALUES (1, 'first')")
	c.Assert(err, IsNil)
	_, err = s.db.DB().Exec("INSERT INTO `test_structure` (`id`, `name`) VALUES (2, 'wow 2nd')")
	c.Assert(err, IsNil)

	input := &testStructure{Id: 2, Name: "updated 2nd"}
	c.Assert(s.db.Save(&input), IsNil)

	//check if callback OnInsert is called
	c.Assert(input.onUpdateInvoked, Equals, true)

	//check if callback OnInserted is called
	c.Assert(input.onPostUpdateInvoked, Equals, true)

	//check if all the fields are correctly saved
	input = nil
	c.Assert(s.db.Find(&input, "id = ?", 2), IsNil)
	c.Assert(input.Id, Equals, 2)
	c.Assert(input.Name, Equals, "updated 2nd")
}

func (s *stormSuite) TestSave_AllSupportedTypes(c *C) {
	c.Assert(s.db.RegisterStructure((*testAllTypeStructure)(nil)), IsNil)
	_, err := s.db.DB().Exec("CREATE TABLE `test_all_type_structure` (" +
		"`id` INTEGER PRIMARY KEY,`test_custom_type` INTEGER,`time` DATETIME,`byte` BLOB,`string` TEXT,`int` INTEGER,`int64` BIGINT," +
		"`float64` REAL,`bool` BOOL,`null_string` TEXT,`null_int` BIGINT,`null_float` REAL,`null_bool` BOOL," +
		"`ptr_string` TEXT,`ptr_int` INTEGER,`ptr_int64` BIGINT,`ptr_float` REAL,`ptr_bool` BOOL)")
	c.Assert(err, IsNil)

	var compare *testAllTypeStructure
	input := &testAllTypeStructure{
		Id:             0,
		TestCustomType: 3,
		Time:           time.Date(2010, time.December, 31, 23, 59, 59, 0, time.Local),
		Byte:           []byte("1234567890"),
		String:         "test 1",
		Int:            1234,
		Int64:          5678,
		Float64:        1234.56,
		Bool:           false,
		NullString:     sql.NullString{String: "", Valid: false},
		NullInt:        sql.NullInt64{Int64: 0, Valid: false},
		NullFloat:      sql.NullFloat64{Float64: 0, Valid: false},
		NullBool:       sql.NullBool{Bool: false, Valid: false},
		PtrString:      nil,
		PtrInt:         nil,
		PtrInt64:       nil,
		PtrFloat:       nil,
		PtrBool:        nil,
	}
	c.Assert(s.db.Save(&input), IsNil)
	c.Assert(input.Id, Equals, 1)
	c.Assert(s.db.Find(&compare, "id = ?", 1), IsNil)

	c.Assert(compare, DeepEquals, input)

	str := string("")
	i := int(0)
	i64 := int64(0)
	f := float64(0)
	b := bool(false)
	//overwrite (null values with)
	input = &testAllTypeStructure{
		Id:             1,
		TestCustomType: 3,
		Time:           time.Date(2010, time.December, 31, 23, 59, 59, 0, time.Local),
		Byte:           []byte("1234567890"),
		String:         "test 1",
		Int:            1234,
		Int64:          5678,
		Float64:        1234.56,
		Bool:           false,
		NullString:     sql.NullString{String: "", Valid: true},
		NullInt:        sql.NullInt64{Int64: 0, Valid: true},
		NullFloat:      sql.NullFloat64{Float64: 0, Valid: true},
		NullBool:       sql.NullBool{Bool: false, Valid: true},
		PtrString:      &str,
		PtrInt:         &i,
		PtrInt64:       &i64,
		PtrFloat:       &f,
		PtrBool:        &b,
	}
	c.Assert(s.db.Save(&input), IsNil)
	c.Assert(s.db.Find(&compare, "id = ?", 1), IsNil)
	c.Assert(compare, DeepEquals, input)

	str = string("string")
	i = int(0)
	i64 = int64(0)
	f = float64(0)
	b = bool(false)
	input = &testAllTypeStructure{
		Id:             1,
		TestCustomType: 3,
		Time:           time.Date(2010, time.December, 31, 23, 59, 59, 0, time.Local),
		Byte:           []byte("1234567890"),
		String:         "test 1",
		Int:            1234,
		Int64:          5678,
		Float64:        1234.56,
		Bool:           false,
		NullString:     sql.NullString{String: "string", Valid: true},
		NullInt:        sql.NullInt64{Int64: 1234, Valid: true},
		NullFloat:      sql.NullFloat64{Float64: 1234.56, Valid: true},
		NullBool:       sql.NullBool{Bool: true, Valid: true},
		PtrString:      &str,
		PtrInt:         &i,
		PtrInt64:       &i64,
		PtrFloat:       &f,
		PtrBool:        &b,
	}
	c.Assert(s.db.Save(&input), IsNil)
	c.Assert(s.db.Find(&compare, "id = ?", 1), IsNil)

	c.Assert(compare, DeepEquals, input)

	//update it back to null
	input = &testAllTypeStructure{
		Id:             1,
		TestCustomType: 3,
		Time:           time.Date(2010, time.December, 31, 23, 59, 59, 0, time.Local),
		Byte:           []byte("1234567890"),
		String:         "test 1",
		Int:            1234,
		Int64:          5678,
		Float64:        1234.56,
		Bool:           false,
		NullString:     sql.NullString{String: "", Valid: false},
		NullInt:        sql.NullInt64{Int64: 0, Valid: false},
		NullFloat:      sql.NullFloat64{Float64: 0, Valid: false},
		NullBool:       sql.NullBool{Bool: false, Valid: false},
		PtrString:      nil,
		PtrInt:         nil,
		PtrInt64:       nil,
		PtrFloat:       nil,
		PtrBool:        nil,
	}
	c.Assert(s.db.Save(&input), IsNil)
	c.Assert(s.db.Find(&compare, "id = ?", 1), IsNil)
	c.Assert(compare, DeepEquals, input)
}

func (s *stormSuite) TestSave_ErrorNotByReference(c *C) {
	c.Assert(s.db.Save(Person{}), ErrorMatches, "provided input is not by reference")
}

func (s *stormSuite) TestSave_ErrorNotRegistered(c *C) {
	type notRegisteredStruct struct{}
	c.Assert(s.db.Save(&notRegisteredStruct{}), ErrorMatches, "no registered structure for `storm.notRegisteredStruct` found")
}

func (s *stormSuite) TestSave_ErrorNotAStructure(c *C) {
	var notStruct int = 1
	c.Assert(s.db.Save(&notStruct), ErrorMatches, "provided input is not a structure type")
	var notStructPtr *int = new(int)
	*notStructPtr = 1
	c.Assert(s.db.Save(&notStructPtr), ErrorMatches, "provided input is not a structure type")
}

func (s *stormSuite) TestSave_ErrorNullPointer(c *C) {
	person := (*Person)(nil)
	c.Assert(s.db.Save(&person), ErrorMatches, "provided input is a nil pointer")
}

func (s *stormSuite) TestSave_ErrorOnInsertCallback(c *C) {
	input := testErrorCallbackStruct{Id: 0}
	c.Assert(s.db.RegisterStructure((*testErrorCallbackStruct)(nil)), IsNil)
	c.Assert(s.db.Save(&input), ErrorMatches, "insert callback error")
}

func (s *stormSuite) TestSave_ErrorOnUpdateCallback(c *C) {
	input := testErrorCallbackStruct{Id: 5}
	c.Assert(s.db.RegisterStructure((*testErrorCallbackStruct)(nil)), IsNil)
	c.Assert(s.db.Save(&input), ErrorMatches, "update callback error")
}

//force sql error, table not found
func (s *stormSuite) TestSave_ErrorSqlError(c *C) {
	c.Assert(s.db.RegisterStructure((*Person)(nil)), IsNil)
	c.Assert(s.db.Save(&Person{Id: 5}), ErrorMatches, "no such table: person")
	c.Assert(s.db.Save(&Person{}), ErrorMatches, "no such table: person")
}

func (s *stormSuite) TestCreateTable(c *C) {
	c.Assert(s.db.RegisterStructure((*Person)(nil)), IsNil)
	c.Assert(s.db.CreateTable((*Person)(nil)), IsNil)
	c.Assert(s.db.CreateTable((*Person)(nil)), ErrorMatches, "table `person` already exists")
}

func (s *stormSuite) TestCreateTable_ErrorNotAStructure(c *C) {
	c.Assert(s.db.CreateTable(int(1)), ErrorMatches, "provided input is not a structure type")
	c.Assert(s.db.CreateTable(string("test")), ErrorMatches, "provided input is not a structure type")
	c.Assert(s.db.CreateTable((*int)(nil)), ErrorMatches, "provided input is not a structure type")
}

func (s *stormSuite) TestCreateTable_ErrorNotRegistered(c *C) {
	type notRegisteredStruct struct{}
	c.Assert(s.db.CreateTable((*notRegisteredStruct)(nil)), ErrorMatches, "no registered structure for `storm.notRegisteredStruct` found")
}

func (s *stormSuite) TestDropTable(c *C) {
	c.Assert(s.db.RegisterStructure((*Person)(nil)), IsNil)
	_, err := s.db.DB().Exec("CREATE TABLE `person` (`id` INTEGER PRIMARY KEY, `name` TEXT)")
	c.Assert(err, IsNil)
	c.Assert(s.db.DropTable((*Person)(nil)), IsNil)
	c.Assert(s.db.DropTable((*Person)(nil)), ErrorMatches, "no such table: person")
}

func (s *stormSuite) TestDropTable_ErrorNotAStructure(c *C) {
	c.Assert(s.db.DropTable((*int)(nil)), ErrorMatches, "provided input is not a structure type")
}

func (s *stormSuite) TestDropTable_ErrorNotRegistered(c *C) {
	type notRegisteredStruct struct{}
	c.Assert(s.db.DropTable((*notRegisteredStruct)(nil)), ErrorMatches, "no registered structure for `storm.notRegisteredStruct` found")
}

func (s *stormSuite) TestWhere(c *C) {
	q := s.db.Where("id = ?", 123)

	c.Assert(q.where, HasLen, 1)
	c.Assert(q.where[0].Statement, Equals, "id = ?")
	c.Assert(q.where[0].Bindings, HasLen, 1)
	c.Assert(q.where[0].Bindings[0], FitsTypeOf, int(123))
	c.Assert(q.where[0].Bindings[0].(int), Equals, 123)
}

func (s *stormSuite) TestOrder(c *C) {
	q := s.db.Order("test", ASC)

	c.Assert(q.order, HasLen, 1)
	c.Assert(q.order[0].Statement, Equals, "test")
	c.Assert(q.order[0].Direction, Equals, ASC)
}

func (s *stormSuite) TestLimit(c *C) {
	c.Assert(s.db.Limit(123).limit, Equals, 123)
}

func (s *stormSuite) TestOffset(c *C) {
	c.Assert(s.db.Offset(123).offset, Equals, 123)
}

func (s *stormSuite) TestBegin(c *C) {
	c.Assert(s.db.Begin().DB(), Not(Equals), s.db.DB())
}

//--------------------------------------
// SQL helpers
//--------------------------------------
func (s *stormSuite) TestGenerateDeleteSql(c *C) {
	c.Assert(s.db.RegisterStructure((*Person)(nil)), IsNil)
	person := Person{Id: 1, Name: "test"}
	tbl, ok := s.db.table(reflect.TypeOf(person))
	c.Assert(tbl, NotNil)
	c.Assert(ok, Equals, true)

	v := reflect.ValueOf(person)
	sqlQuery, bind := s.db.generateDeleteSQL(v, tbl)

	c.Assert(bind, HasLen, 1)
	c.Assert(bind[0], Equals, 1)
	c.Assert(sqlQuery, Equals, "DELETE FROM `person` WHERE `id` = ?")
}

func (s *stormSuite) TestGenerateInsertSQL(c *C) {
	c.Assert(s.db.RegisterStructure((*Person)(nil)), IsNil)
	person := Person{Id: 0, Name: "test", AddressId: 2}
	tbl, ok := s.db.table(reflect.TypeOf(person))
	c.Assert(tbl, NotNil)
	c.Assert(ok, Equals, true)

	v := reflect.ValueOf(person)
	sqlQuery, bind := s.db.generateInsertSQL(v, tbl)
	c.Assert(bind, HasLen, 3)
	c.Assert(bind[0], Equals, "test")
	c.Assert(bind[1], Equals, 2)
	c.Assert(bind[2], Equals, sql.NullInt64{})
	c.Assert(sqlQuery, Equals, "INSERT INTO `person` (`name`, `address_id`, `optional_address_id`) VALUES (?, ?, ?)")
}

func (s *stormSuite) TestGenerateUpdateSQL(c *C) {
	c.Assert(s.db.RegisterStructure((*Person)(nil)), IsNil)
	person := Person{Id: 3, Name: "test", AddressId: 2}
	tbl, ok := s.db.table(reflect.TypeOf(person))
	c.Assert(tbl, NotNil)
	c.Assert(ok, Equals, true)

	v := reflect.ValueOf(person)
	sqlQuery, bind := s.db.generateUpdateSQL(v, tbl)
	c.Assert(bind, HasLen, 4)
	c.Assert(bind[0], Equals, "test")
	c.Assert(bind[1], Equals, 2)
	c.Assert(bind[2], Equals, sql.NullInt64{})
	c.Assert(bind[3], Equals, 3)
	c.Assert(sqlQuery, Equals, "UPDATE `person` SET `name` = ?, `address_id` = ?, `optional_address_id` = ? WHERE `id` = ?")
}

func (s *stormSuite) TestGenerateCreateTableSQL(c *C) {
	c.Assert(s.db.RegisterStructure((*testAllTypeStructure)(nil)), IsNil)
	tbl, ok := s.db.table(reflect.TypeOf((*testAllTypeStructure)(nil)).Elem())
	c.Assert(ok, Equals, true)
	c.Assert(tbl, NotNil)

	c.Assert(s.db.generateCreateTableSQL(tbl), Equals, "CREATE TABLE `test_all_type_structure` ("+
		"`id` INTEGER PRIMARY KEY,"+
		"`test_custom_type` INTEGER,"+
		"`time` DATETIME,"+
		"`byte` BLOB,"+
		"`string` TEXT,"+
		"`int` INTEGER,"+
		"`int64` BIGINT,"+
		"`float64` REAL,"+
		"`bool` BOOL,"+
		"`null_string` TEXT,"+
		"`null_int` BIGINT,"+
		"`null_float` REAL,"+
		"`null_bool` BOOL,"+
		"`ptr_string` TEXT,"+
		"`ptr_int` INTEGER,"+
		"`ptr_int64` BIGINT,"+
		"`ptr_float` REAL,"+
		"`ptr_bool` BOOL"+
		")")
}

func (s *stormSuite) TestGenerateDropTableSQL(c *C) {
	c.Assert(s.db.RegisterStructure((*Person)(nil)), IsNil)
	tbl, ok := s.db.table(reflect.TypeOf((*Person)(nil)).Elem())
	c.Assert(ok, Equals, true)
	c.Assert(s.db.generateDropTableSQL(tbl), Equals, "DROP TABLE `person`")
}

func (s *stormSuite) TestTable(c *C) {
	c.Assert(s.db.RegisterStructure((*Person)(nil)), IsNil)

	tbl, ok := s.db.table(reflect.TypeOf((*Person)(nil)).Elem())
	c.Assert(ok, Equals, true)
	c.Assert(tbl, NotNil)
	c.Assert(tbl.tableName, Equals, "person")

	type notRegisteredStruct struct{}
	_, ok = s.db.table(reflect.TypeOf((*notRegisteredStruct)(nil)).Elem())
	c.Assert(ok, Equals, false)
}

func (s *stormSuite) TestTableByName(c *C) {
	c.Assert(s.db.RegisterStructure((*Person)(nil)), IsNil)
	epectedTbl, ok := s.db.table(reflect.TypeOf((*Person)(nil)).Elem())
	c.Assert(ok, Equals, true)

	tbl, ok := s.db.tableByName("person")
	c.Assert(ok, Equals, true)
	c.Assert(tbl, Equals, epectedTbl)

	tbl, ok = s.db.tableByName("tableNoExistie")
	c.Assert(ok, Equals, false)
	c.Assert(tbl, IsNil)
}
