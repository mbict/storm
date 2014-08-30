package storm

import (
	"bytes"
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	. "gopkg.in/check.v1"
	"reflect"
	"testing"
	"time"
)

//*** test suite setup ***/
type stormSuite struct {
	db *Storm
}

var _ = Suite(&stormSuite{})

func (s *stormSuite) SetUpSuite(c *C) {

	var err error
	s.db, err = Open(`sqlite3`, `:memory:`)
	c.Assert(s.db, NotNil)
	c.Assert(err, IsNil)

	s.db.RegisterStructure((*testStructure)(nil))
	s.db.RegisterStructure((*testAllTypeStructure)(nil))
	s.db.SetMaxIdleConns(10)
	s.db.SetMaxOpenConns(10)

	s.db.DB().Exec("CREATE TABLE `test_structure` (`id` INTEGER PRIMARY KEY, `name` TEXT)")
	s.db.DB().Exec("CREATE TABLE `test_related_structure` (`id` INTEGER PRIMARY KEY, test_structure_id INTEGER, `name` TEXT)")
	s.db.DB().Exec("CREATE TABLE `test_all_type_structure` " +
		"(`id` INTEGER PRIMARY KEY,`test_custom_type` INTEGER,`time` DATETIME,`byte` BLOB,`string` TEXT,`int` INTEGER,`int64` BIGINT," +
		"`float64` REAL,`bool` BOOL,`null_string` TEXT,`null_int` BIGINT,`null_float` REAL,`null_bool` BOOL)")
}

/*** tests ***/
func (s *stormSuite) TestRegisterStructure(c *C) {
}

func TestStorm_RegisterStructure(t *testing.T) {

	var (
		s          *Storm
		err        error
		typeStruct = reflect.TypeOf(testAllTypeStructure{})
		tbl        *table
		ok         bool
	)

	//register by casted nil type
	s, _ = Open(`sqlite3`, `:memory:`)
	err = s.RegisterStructure((*testAllTypeStructure)(nil))
	if err != nil {
		t.Fatalf("Failed with error : %v", err)
	}

	if tbl, ok = s.tables[typeStruct]; !ok || tbl == nil {
		t.Fatalf("added table information not found")
	}

	//register by element
	s, _ = Open(`sqlite3`, `:memory:`)
	structure := testAllTypeStructure{}
	err = s.RegisterStructure(structure)
	if err != nil {
		t.Fatalf("Failed with error : %v", err)
	}

	if tbl, ok = s.tables[typeStruct]; !ok || tbl == nil {
		t.Fatalf("added table information not found")
	}

	//register by nil element
	s, _ = Open(`sqlite3`, `:memory:`)
	structurePtr := &testAllTypeStructure{}
	err = s.RegisterStructure(structurePtr)
	if err != nil {
		t.Fatalf("Failed with error : %v", err)
	}

	if tbl, ok = s.tables[typeStruct]; !ok || tbl == nil {
		t.Fatalf("added table information not found")
	}
}

func (s *stormSuite) TestRegisterStructureWrongInput(c *C) {
	c.Assert(s.db.RegisterStructure((*int)(nil)), ErrorMatches, `provided input is not a structure type`)
	c.Assert(s.db.RegisterStructure(string("test")), ErrorMatches, `provided input is not a structure type`)
	c.Assert(s.db.RegisterStructure((*TestProduct)(nil)), IsNil)
	c.Assert(s.db.RegisterStructure((*TestProduct)(nil)), ErrorMatches, `duplicate structure, 'storm.TestProduct' already exists`)
}

func TestStorm_RegisterStructureResolveRelations_OneToMany(t *testing.T) {

	var (
		s          *Storm
		typeStruct = reflect.TypeOf(testStructure{})
	)

	s, _ = Open(`sqlite3`, `:memory:`)
	if nil != s.RegisterStructure((*testStructure)(nil)) ||
		nil != s.RegisterStructure((*testRelatedStructure)(nil)) {
		t.Fatalf("Failed adding test structures")
	}

	tbl, _ := s.tables[typeStruct]
	if len(tbl.relations) != 1 {
		t.Fatalf("Expected to get 1 relational field but got `%d`", len(tbl.relations))
	}

	rel := tbl.relations[0]
	if nil == rel.relColumn || nil == rel.relTable {
		t.Fatalf("Relational table information missing, no relation found")
	}

	if rel.relTable.tableName != "test_related_structure" {
		t.Fatalf("Wrong table found expected %s but got %s", "test_related_structure", rel.relTable.tableName)
	}

	if rel.relColumn.columnName != "test_structure_id" {
		t.Fatalf("Wrong column name found expected %s but got %s", "test_structure_id", rel.relColumn.columnName)
	}

}

func TestStorm_Find_Single(t *testing.T) {
	var (
		err   error
		input *testStructure
		s     = newTestStorm()
	)
	s.DB().Exec("INSERT INTO `test_structure` (`id`, `name`) VALUES (1, 'name')")
	s.DB().Exec("INSERT INTO `test_structure` (`id`, `name`) VALUES (2, 'name 2nd')")

	//empty result, no match
	if err = s.Find(&input, 999); err != sql.ErrNoRows {
		t.Fatalf("Got wrong error back, expected `%v` but got `%v`", sql.ErrNoRows, err)
	}

	//find first result
	input = nil
	if err = s.Find(&input); err != nil {
		t.Fatalf("Failed getting by id with error `%v`", err)
	}

	if err = assertEntity(input, &testStructure{Id: 1, Name: "name"}); err != nil {
		t.Fatalf("Error: %v", err)
	}

	//find by id
	input = nil
	if err = s.Find(&input, 2); err != nil {
		t.Fatalf("Failed getting by id with error `%v`", err)
	}

	if err = assertEntity(input, &testStructure{Id: 2, Name: "name 2nd"}); err != nil {
		t.Fatalf("Error: %v", err)
	}

	//find by string
	input = nil
	if err = s.Find(&input, `id = 1`); err != nil {
		t.Fatalf("Failed getting by id with error `%v`", err)
	}

	if err = assertEntity(input, &testStructure{Id: 1, Name: "name"}); err != nil {
		t.Fatalf("Error: %v", err)
	}

	//find by query bind string (shorthand for query)
	input = nil
	if err = s.Find(&input, `id = ?`, 1); err != nil {
		t.Fatalf("Failed getting by id with error `%v`", err)
	}

	if err = assertEntity(input, &testStructure{Id: 1, Name: "name"}); err != nil {
		t.Fatalf("Error: %v", err)
	}

	//find by multiple bind string (shorthand for query)
	input = nil
	if err = s.Find(&input, `id = ? AND name = ?`, 1, `name`); err != nil {
		t.Fatalf("Failed getting by id with error `%v`", err)
	}

	if err = assertEntity(input, &testStructure{Id: 1, Name: "name"}); err != nil {
		t.Fatalf("Error: %v", err)
	}

	//check if callback OnInit is called
	if input.onInitInvoked != true {
		t.Errorf("OnInit function not invoked")
	}

	//BUG: make sure if we recycle a pointer its initialized to zero
	if err = s.Find(&input, `id = ?`, 999); err != sql.ErrNoRows {
		t.Fatalf("Failed getting by id with error `%v`", err)
	}

	if err = assertEntity(input, &testStructure{Id: 0, Name: ""}); err != nil {
		t.Fatalf("Error: %v", err)
	}
}

func TestStorm_FindWrongInput(t *testing.T) {

	var (
		err           error
		expectedError string
	)
	s := newTestStorm()

	//not a pointer
	var input testStructure
	if err = s.Find(input, 1); err == nil {
		t.Fatalf("Expected a error but got none")
	}

	expectedError = `provided input is not a pointer type`
	if err.Error() != expectedError {
		t.Fatalf("Expected error `%v`, but got `%v`", expectedError, err.Error())
	}

	//not a structure pointer
	var inputIntPtr *int
	if err = s.Find(inputIntPtr, 1); err == nil {
		t.Fatalf("Expected a error but got none")
	}

	expectedError = `provided input is not a structure type`
	if err.Error() != expectedError {
		t.Fatalf("Expected error `%v`, but got `%v`", expectedError, err.Error())
	}

	//not registered structure
	type testNonRegisteredStruct struct{}
	if err = s.Find(&testNonRegisteredStruct{}, 1); err == nil {
		t.Fatalf("Expected a error but got none")
	}

	expectedError = "no registered structure for `storm.testNonRegisteredStruct` found"
	if err.Error() != expectedError {
		t.Fatalf("Expected error `%v`, but got `%v`", expectedError, err.Error())
	}
}

func TestStorm_Delete(t *testing.T) {
	var (
		err   error
		input = testStructure{Id: 1, Name: "name"}
		s     = newTestStormFile()
		res   *sql.Row
	)

	_, err = s.DB().Exec("INSERT INTO `test_structure` (`id`, `name`) VALUES (1, 'name')")
	if err != nil {
		t.Fatalf("Failure on saving testdate to store `%v`", err)
	}

	if err = s.Delete(&input); err != nil {
		t.Fatalf("Failed delete with error `%v`", err.Error())
	}

	//check if callback beforeDelete is called
	if input.onDeleteInvoked != true {
		t.Errorf("OnDelete callback not invoked")
	}

	//check if callback AfterDelete is called
	if input.onPostDeleteInvoked != true {
		t.Errorf("OnDeleted callback not invoked")
	}

	res = s.DB().QueryRow("SELECT * FROM `test_structure` WHERE `id` = ?", 1)
	if err = res.Scan(&input.Id, &input.Name); err != sql.ErrNoRows {
		if err == nil {
			t.Fatalf("Record not deleted")
		}
		t.Fatalf("Expected to get a ErrNoRows but got %v", err)
	}
}

func TestStorm_DeleteWrongInput(t *testing.T) {

	var err error
	var expectedError string
	s := newTestStorm()

	//not a structure
	var inputInt int
	if err = s.Delete(inputInt); err == nil {
		t.Fatalf("Expected a error but got none")
	}

	expectedError = `provided input is not a pointer type`
	if err == nil || err.Error() != expectedError {
		t.Fatalf("Expected error `%v`, but got `%v`", expectedError, err)
	}

	if err = s.Delete(&inputInt); err == nil {
		t.Fatalf("Expected a error but got none")
	}
	expectedError = `provided input is not a structure type`
	if err == nil || err.Error() != expectedError {
		t.Fatalf("Expected error `%v`, but got `%v`", expectedError, err)
	}

	//not a structure pointer
	var inputIntPtr *int
	if err = s.Delete(inputIntPtr); err == nil {
		t.Fatalf("Expected a error but got none")
	}

	expectedError = `provided input is not a structure type`
	if err == nil || err.Error() != expectedError {
		t.Fatalf("Expected error `%v`, but got `%v`", expectedError, err)
	}

	//not registered structure
	type testNonRegisteredStruct struct{}
	if err = s.Delete(&testNonRegisteredStruct{}); err == nil {
		t.Fatalf("Expected a error but got none")
	}

	expectedError = "no registered structure for `storm.testNonRegisteredStruct` found"
	if err == nil || err.Error() != expectedError {
		t.Fatalf("Expected error `%v`, but got `%v`", expectedError, err)
	}
}

func TestStorm_Save(t *testing.T) {

	var (
		err   error
		input *testStructure
		s     = newTestStormFile()
		res   *sql.Row
	)

	//update a existing entity
	_, err = s.DB().Exec("INSERT INTO `test_structure` (`id`, `name`) VALUES (1, 'name')")
	_, err = s.DB().Exec("INSERT INTO `test_structure` (`id`, `name`) VALUES (2, '2nd')")
	if err != nil {
		t.Fatalf("Failure on saving testdate to store `%v`", err)
	}

	input = &testStructure{Id: 1, Name: "test updated"}
	if err = s.Save(input); err != nil {
		t.Fatalf("Failed save (update) with error `%v`", err.Error())
	}

	res = s.DB().QueryRow("SELECT id, name FROM `test_structure` WHERE `id` = ?", 1)
	if err = res.Scan(&input.Id, &input.Name); err != nil {
		t.Fatalf("Expected to get a row back but got error %v", err)
	}

	if input.Name != "test updated" {
		t.Fatalf("Entity data not updated")
	}

	//check if callback OnUpdate is called
	if input.onUpdateInvoked != true {
		t.Errorf("OnUpdate callback not invoked")
	}

	//check if callback OnUpdated is called
	if input.onPostUpdateInvoked != true {
		t.Errorf("OnUpdated callback not invoked")
	}

	//insert a new entity
	input = &testStructure{Id: 0, Name: "test inserted"}
	if err = s.Save(input); err != nil {
		t.Fatalf("Failed save (insert) with error `%v`", err.Error())
	}

	if input.Id == 0 {
		t.Fatalf("Entity pk id not set")
	}

	if input.Id != 3 {
		t.Fatalf("Expected to get entity PK 3 but got %v", input.Id)
	}

	//query for entity
	res = s.DB().QueryRow("SELECT id, name FROM `test_structure` WHERE `id` = ?", 3)
	if err = res.Scan(&input.Id, &input.Name); err != nil {
		t.Fatalf("Expected to get a row back but got error %v", err)
	}

	if err = assertEntity(input, &testStructure{Id: 3, Name: "test inserted"}); err != nil {
		t.Fatalf(err.Error())
	}

	//check if callback OnInsert is called
	if input.onInsertInvoked != true {
		t.Errorf("OnInsert callback not invoked")
	}

	//check if callback OnInserted is called
	if input.onPostInserteInvoked != true {
		t.Errorf("OnInserted callback not invoked")
	}
}

func TestStorm_SaveFindAllTypes(t *testing.T) {

	var (
		err    error
		input  *testAllTypeStructure
		result *testAllTypeStructure
		s      = newTestStormFile()
	)

	assertEqualField := func(v1, v2 interface{}, message string) {
		if v1 != v2 {
			t.Errorf(message, v1, v2)
		}
	}

	//update a existing entity
	if _, err = s.DB().Exec("INSERT INTO `test_all_type_structure` (`id`,`test_custom_type`,`time`,`byte`,`string`,`int`,`int64`,`float64`,`bool`,`null_string`,`null_int`,`null_float`,`null_bool`) VALUES " +
		"(1, 5, '2010-12-31 23:59:59 +0000 UTC', '1234567890ABCDEFG', 'stringvalue', 99, 999, 99.1234, 'TRUE', 'null_String_value', 99, 99.1234, 'TRUE')"); err != nil {
		t.Fatalf("Failure on saving testdate to store `%v`", err)
	}

	if _, err = s.DB().Exec("INSERT INTO `test_all_type_structure` (`id`,`test_custom_type`,`time`,`byte`,`string`,`int`,`int64`,`float64`,`bool`,`null_string`,`null_int`,`null_float`,`null_bool`) VALUES " +
		"(2, 6, NULL, 'GFEDCBA0987654321', '2nd string value', 199, 1999, 199.1234, 'FALSE', NULL, NULL, NULL, NULL)"); err != nil {
		t.Fatalf("Failure on saving testdate to store `%v`", err)
	}

	input = &testAllTypeStructure{
		Id:             1,
		TestCustomType: 3,
		Time:           time.Date(2010, time.December, 31, 23, 59, 59, 0, time.UTC),
		Byte:           []byte("1234567890"),
		String:         "test update",
		Int:            1234,
		Int64:          5678,
		Float64:        1234.56,
		Bool:           false,
		NullString:     sql.NullString{String: "", Valid: false},
		NullInt:        sql.NullInt64{Int64: 0, Valid: false},
		NullFloat:      sql.NullFloat64{Float64: 0, Valid: false},
		NullBool:       sql.NullBool{Bool: false, Valid: false},
	}

	//save item
	if err = s.Save(input); err != nil {
		t.Fatalf("Failed save (update) with error `%v`", err.Error())
	}

	//fetch item we just uupdated and compare if the items are set as expected
	if err = s.Find(&result, 1); err != nil {
		t.Fatalf("Unable to find modified record `%v`", err.Error())
	}

	//assert row equals
	assertEqualField(1, result.Id, "Id mismatches %d != %d")
	assertEqualField(testCustomType(3), result.TestCustomType, "TestCustomType mismatches %d != %d")
	assertEqualField(input.Time, result.Time, "Time mismatches %s != %s")
	if bytes.Equal([]byte("1234567890"), result.Byte) != true {
		t.Errorf("Byte mismatches %v != %v", input.Byte, result.Byte)
	}
	assertEqualField("test update", result.String, "String mismatches %s != %s")
	assertEqualField(int(1234), result.Int, "Int mismatches %d != %d")
	assertEqualField(int64(5678), result.Int64, "Int64 mismatches %d != %d")
	assertEqualField(float64(1234.56), result.Float64, "Float64 mismatches %f != %f")
	assertEqualField(input.Bool, result.Bool, "Bool mismatches %c != %c")
	assertEqualField(input.NullString, result.NullString, "NullString mismatches %v != %v")
	assertEqualField(input.NullInt, result.NullInt, "NullInt mismatches %v != %v")
	assertEqualField(input.NullFloat, result.NullFloat, "NullFloat mismatches %v != %v")
	assertEqualField(input.NullBool, result.NullBool, "NullBool mismatches %v != %v")

	//save with null values (only check the null value rows)
	input = &testAllTypeStructure{
		Id:             2,
		TestCustomType: 4,
		Time:           time.Date(2010, time.December, 31, 23, 59, 59, 0, time.UTC),
		Byte:           []byte("1234567890"),
		String:         "test update",
		Int:            1234,
		Int64:          5678,
		Float64:        1234.56,
		Bool:           false,
		NullString:     sql.NullString{String: "test1234", Valid: true},
		NullInt:        sql.NullInt64{Int64: 234, Valid: true},
		NullFloat:      sql.NullFloat64{Float64: 234.12, Valid: true},
		NullBool:       sql.NullBool{Bool: true, Valid: true},
	}

	if err = s.Save(input); err != nil {
		t.Fatalf("Failed save (update) with error `%v`", err.Error())
	}

	if err = s.Find(&result, 2); err != nil {
		t.Fatalf("Unable to find modified record `%v`", err.Error())
	}

	//assert row equals
	assertEqualField(false, result.Bool, "Bool mismatches %c != %c")
	assertEqualField(input.NullString, result.NullString, "NullString mismatches %v != %v")
	assertEqualField(input.NullInt, result.NullInt, "NullInt mismatches %v != %v")
	assertEqualField(input.NullFloat, result.NullFloat, "NullFloat mismatches %v != %v")
	assertEqualField(input.NullBool, result.NullBool, "NullBool mismatches %v != %v")

	//*** insert tests ***************************************
	input = &testAllTypeStructure{
		Id:             0,
		TestCustomType: 3,
		Time:           time.Date(2010, time.December, 31, 23, 59, 59, 0, time.UTC),
		Byte:           []byte("1234567890"),
		String:         "test update",
		Int:            1234,
		Int64:          5678,
		Float64:        1234.56,
		Bool:           false,
		NullString:     sql.NullString{String: "", Valid: false},
		NullInt:        sql.NullInt64{Int64: 0, Valid: false},
		NullFloat:      sql.NullFloat64{Float64: 0, Valid: false},
		NullBool:       sql.NullBool{Bool: false, Valid: false},
	}

	//save item
	if err = s.Save(input); err != nil {
		t.Fatalf("Failed save (insert) with error `%v`", err.Error())
	}

	//fetch item we just uupdated and compare if the items are set as expected
	if err = s.Find(&result, 3); err != nil {
		t.Fatalf("Unable to find inserted record `%v`", err.Error())
	}

	//assert row equals
	assertEqualField(3, result.Id, "Id mismatches %d != %d")
	assertEqualField(testCustomType(3), result.TestCustomType, "TestCustomType mismatches %d != %d")
	assertEqualField(input.Time, result.Time, "Time mismatches %s != %s")
	if bytes.Equal([]byte("1234567890"), result.Byte) != true {
		t.Errorf("Byte mismatches %v != %v", input.Byte, result.Byte)
	}
	assertEqualField("test update", result.String, "String mismatches %s != %s")
	assertEqualField(1234, result.Int, "Int mismatches %d != %d")
	assertEqualField(int64(5678), result.Int64, "Int64 mismatches %d != %d")
	assertEqualField(1234.56, result.Float64, "Float64 mismatches %f != %f")
	assertEqualField(input.Bool, result.Bool, "Bool mismatches %c != %c")
	assertEqualField(input.NullString, result.NullString, "NullString mismatches %v != %v")
	assertEqualField(input.NullInt, result.NullInt, "NullInt mismatches %v != %v")
	assertEqualField(input.NullFloat, result.NullFloat, "NullFloat mismatches %v != %v")
	assertEqualField(input.NullBool, result.NullBool, "NullBool mismatches %v != %v")

	//save with null values (only check the null value rows)
	input = &testAllTypeStructure{
		Id:             0,
		TestCustomType: 4,
		Time:           time.Date(2010, time.December, 31, 23, 59, 59, 0, time.UTC),
		Byte:           []byte("1234567890"),
		String:         "test update",
		Int:            1234,
		Int64:          5678,
		Float64:        1234.56,
		Bool:           false,
		NullString:     sql.NullString{String: "test1234", Valid: true},
		NullInt:        sql.NullInt64{Int64: 234, Valid: true},
		NullFloat:      sql.NullFloat64{Float64: 234.12, Valid: true},
		NullBool:       sql.NullBool{Bool: true, Valid: true},
	}

	if err = s.Save(input); err != nil {
		t.Fatalf("Failed save (insert) with error `%v`", err.Error())
	}

	if err = s.Find(&result, 4); err != nil {
		t.Fatalf("Unable to find inserted record `%v`", err.Error())
	}

	//assert row equals
	assertEqualField(4, result.Id, "Id mismatches %d != %d")
	assertEqualField(false, result.Bool, "Bool mismatches %c != %c")
	assertEqualField(input.NullString, result.NullString, "NullString mismatches %v != %v")
	assertEqualField(input.NullInt, result.NullInt, "NullInt mismatches %v != %v")
	assertEqualField(input.NullFloat, result.NullFloat, "NullFloat mismatches %v != %v")
	assertEqualField(input.NullBool, result.NullBool, "NullBool mismatches %v != %v")

}

func TestStorm_SaveWrongInput(t *testing.T) {

	var err error
	var expectedError string
	s := newTestStorm()

	//not a pointer
	var input testStructure
	if err = s.Save(input); err == nil {
		t.Fatalf("Expected a error but got none")
	}

	expectedError = `provided input is not a pointer type`
	if err.Error() != expectedError {
		t.Fatalf("Expected error `%v`, but got `%v`", expectedError, err.Error())
	}

	//not a structure pointer
	var inputIntPtr *int
	if err = s.Save(inputIntPtr); err == nil {
		t.Fatalf("Expected a error but got none")
	}

	expectedError = `provided input is not a structure type`
	if err.Error() != expectedError {
		t.Fatalf("Expected error `%v`, but got `%v`", expectedError, err.Error())
	}

	//not registered structure
	type testNonRegisteredStruct struct{}
	if err = s.Save(&testNonRegisteredStruct{}); err == nil {
		t.Fatalf("Expected a error but got none")
	}

	expectedError = "no registered structure for `storm.testNonRegisteredStruct` found"
	if err.Error() != expectedError {
		t.Fatalf("Expected error `%v`, but got `%v`", expectedError, err.Error())
	}
}

func TestStorm_CreateTable(t *testing.T) {
	var (
		err    error
		result int
	)

	s, _ := Open(`sqlite3`, `:memory:`)
	s.RegisterStructure((*testStructure)(nil))

	if result != 0 {
		t.Fatalf("Table does exists, cannot create")
	}

	err = s.CreateTable((*testStructure)(nil))
	if err != nil {
		t.Fatalf("Failure creating new table `%v`", err)
	}

	result, err = assertTableExist("test_structure", s.DB())
	if err != nil {
		t.Fatalf("Error while determing if new table exists `%v`", err)
	}

	if result != 1 {
		t.Fatalf("Table not created")
	}
}

func TestStorm_DropTable(t *testing.T) {
	var (
		err    error
		result int
		s      = newTestStorm()
	)

	//check if table does exists
	result, err = assertTableExist("test_structure", s.DB())
	if err != nil {
		t.Fatalf("Error while determing if table exists `%v`", err)
	}

	if result != 1 {
		t.Fatal("Table does not exist, nothing to drop")
	}

	//drop the table
	err = s.DropTable((*testStructure)(nil))
	if err != nil {
		t.Fatalf("Failure creating new table `%v`", err)
	}

	//check if table does not exists
	result, err = assertTableExist("test_structure", s.DB())
	if err != nil {
		t.Fatalf("Error while determing if table is dropped `%v`", err)
	}

	if result != 0 {
		t.Fatalf("Table is not dropped")
	}
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
	entity := testStructure{Id: 1, Name: "test"}
	tbl, _ := s.db.table(reflect.TypeOf(entity))
	v := reflect.ValueOf(entity)
	sqlQuery, bind := s.db.generateDeleteSQL(v, tbl)

	c.Assert(bind, HasLen, 1)
	c.Assert(bind[0], Equals, 1)
	c.Assert(sqlQuery, Equals, "DELETE FROM `test_structure` WHERE `id` = ?")
}

func (s *stormSuite) TestGenerateInsertSQL(c *C) {
	entity := testStructure{Id: 0, Name: "test"}
	tbl, _ := s.db.table(reflect.TypeOf(entity))
	v := reflect.ValueOf(entity)
	sqlQuery, bind := s.db.generateInsertSQL(v, tbl)

	c.Assert(bind, HasLen, 1)
	c.Assert(bind[0], Equals, "test")
	c.Assert(sqlQuery, Equals, "INSERT INTO `test_structure` (`name`) VALUES (?)")
}

func (s *stormSuite) TestGenerateUpdateSQL(c *C) {
	entity := testStructure{Id: 2, Name: "test"}
	tbl, _ := s.db.table(reflect.TypeOf(entity))
	v := reflect.ValueOf(entity)
	sqlQuery, bind := s.db.generateUpdateSQL(v, tbl)

	c.Assert(bind, HasLen, 2)
	c.Assert(bind[0], Equals, "test")
	c.Assert(bind[1], Equals, 2)
	c.Assert(sqlQuery, Equals, "UPDATE `test_structure` SET `name` = ? WHERE `id` = ?")
}

func (s *stormSuite) TestGenerateCreateTableSQL(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*testAllTypeStructure)(nil)).Elem())
	c.Assert(s.db.generateCreateTableSQL(tbl), Equals, "CREATE TABLE `test_all_type_structure` "+
		"(`id` INTEGER PRIMARY KEY,`test_custom_type` INTEGER,`time` DATETIME,`byte` BLOB,`string` TEXT,`int` INTEGER,`int64` BIGINT,"+
		"`float64` REAL,`bool` BOOL,`null_string` TEXT,`null_int` BIGINT,"+
		"`null_float` REAL,`null_bool` BOOL)")
}

func (s *stormSuite) TestGenerateDropTableSQL(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*testAllTypeStructure)(nil)).Elem())
	c.Assert(s.db.generateDropTableSQL(tbl), Equals, "DROP TABLE `test_all_type_structure`")
}
