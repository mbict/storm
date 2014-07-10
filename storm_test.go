package storm

import (
	"bytes"
	"database/sql"
	"reflect"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

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
	err = s.RegisterStructure((*testAllTypeStructure)(nil), `testAllTypeStructure`)
	if err != nil {
		t.Fatalf("Failed with error : %v", err)
	}

	if tbl, ok = s.tables[typeStruct]; !ok || tbl == nil {
		t.Fatalf("added table information not found")
	}

	//register by element
	s, _ = Open(`sqlite3`, `:memory:`)
	structure := testAllTypeStructure{}
	err = s.RegisterStructure(structure, `testStructure`)
	if err != nil {
		t.Fatalf("Failed with error : %v", err)
	}

	if tbl, ok = s.tables[typeStruct]; !ok || tbl == nil {
		t.Fatalf("added table information not found")
	}

	//register by nil element
	s, _ = Open(`sqlite3`, `:memory:`)
	structurePtr := &testAllTypeStructure{}
	err = s.RegisterStructure(structurePtr, `testStructure`)
	if err != nil {
		t.Fatalf("Failed with error : %v", err)
	}

	if tbl, ok = s.tables[typeStruct]; !ok || tbl == nil {
		t.Fatalf("added table information not found")
	}
}

func TestStorm_RegisterStructureWrongInput(t *testing.T) {

	var (
		s             *Storm
		err           error
		expectedError = `provided input is not a structure type`
	)

	s, _ = Open(`sqlite3`, `:memory:`)

	//register by casted nil type
	err = s.RegisterStructure((*int)(nil), `testStructure`)
	if err == nil || err.Error() != expectedError {
		t.Fatalf("Expected error `%v`, but got `%v`", expectedError, err)
	}

	//register by normal non struct type
	err = s.RegisterStructure((string)("test"), `testStructure`)
	if err == nil || err.Error() != expectedError {
		t.Fatalf("Expected error `%v`, but got `%v`", expectedError, err)
	}

	//duplicate add error
	err = s.RegisterStructure((*testProduct)(nil), `testStructure`)
	if err != nil {
		t.Fatalf("Expected no error , but got `%v`", err)
	}

	expectedError = "duplicate structure, 'storm.testProduct' already exists"
	err = s.RegisterStructure((*testProduct)(nil), `testStructure`)
	if err == nil || err.Error() != expectedError {
		t.Fatalf("Expected error `%v`, but got `%v`", expectedError, err)
	}

}

func TestStorm_Find_Single(t *testing.T) {
	var (
		err   error
		input *testStructure
		s     = newTestStorm()
	)
	s.DB().Exec("INSERT INTO `testStructure` (`id`, `name`) VALUES (1, 'name')")
	s.DB().Exec("INSERT INTO `testStructure` (`id`, `name`) VALUES (2, 'name 2nd')")

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

	_, err = s.DB().Exec("INSERT INTO `testStructure` (`id`, `name`) VALUES (1, 'name')")
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

	res = s.DB().QueryRow("SELECT * FROM `testStructure` WHERE `id` = ?", 1)
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
	_, err = s.DB().Exec("INSERT INTO `testStructure` (`id`, `name`) VALUES (1, 'name')")
	_, err = s.DB().Exec("INSERT INTO `testStructure` (`id`, `name`) VALUES (2, '2nd')")
	if err != nil {
		t.Fatalf("Failure on saving testdate to store `%v`", err)
	}

	input = &testStructure{Id: 1, Name: "test updated"}
	if err = s.Save(input); err != nil {
		t.Fatalf("Failed save (update) with error `%v`", err.Error())
	}

	res = s.DB().QueryRow("SELECT id, name FROM `testStructure` WHERE `id` = ?", 1)
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
	res = s.DB().QueryRow("SELECT id, name FROM `testStructure` WHERE `id` = ?", 3)
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
	if _, err = s.DB().Exec("INSERT INTO `testAllTypeStructure` (`id`,`test_custom_type`,`time`,`byte`,`string`,`int`,`int64`,`float64`,`bool`,`null_string`,`null_int`,`null_float`,`null_bool`) VALUES " +
		"(1, 5, '2010-12-31 23:59:59 +0000 UTC', '1234567890ABCDEFG', 'stringvalue', 99, 999, 99.1234, 'TRUE', 'null_String_value', 99, 99.1234, 'TRUE')"); err != nil {
		t.Fatalf("Failure on saving testdate to store `%v`", err)
	}

	if _, err = s.DB().Exec("INSERT INTO `testAllTypeStructure` (`id`,`test_custom_type`,`time`,`byte`,`string`,`int`,`int64`,`float64`,`bool`,`null_string`,`null_int`,`null_float`,`null_bool`) VALUES " +
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
	s.RegisterStructure((*testStructure)(nil), `testStructure`)

	if result != 0 {
		t.Fatalf("Table does exists, cannot create")
	}

	err = s.CreateTable((*testStructure)(nil))
	if err != nil {
		t.Fatalf("Failure creating new table `%v`", err)
	}

	result, err = assertTableExist("testStructure", s.DB())
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
	result, err = assertTableExist("testStructure", s.DB())
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
	result, err = assertTableExist("testStructure", s.DB())
	if err != nil {
		t.Fatalf("Error while determing if table is dropped `%v`", err)
	}

	if result != 0 {
		t.Fatalf("Table is not dropped")
	}
}

//Test where passtrough
func TestStorm_Where(t *testing.T) {
	var (
		s = newTestStorm()
		q = s.Where("id = ?", 1)
	)

	if q.where[0].Statement != "id = ?" {
		t.Fatalf("Where statement differs in query")
	}

	if len(q.where[0].Bindings) != 1 && q.where[0].Bindings[0].(int) != 1 {
		t.Fatalf("Expected where statement value")
	}
}

//Test order passtrough
func TestStorm_Order(t *testing.T) {
	var (
		s = newTestStorm()
		q = s.Order("test", ASC)
	)

	if q.order[0].Statement != "test" {
		t.Fatalf("Order statement differs in query")
	}

	if q.order[0].Direction != ASC {
		t.Fatalf("Expected order statement value")
	}
}

//Test limit passtrough
func TestStorm_Limit(t *testing.T) {
	var (
		s = newTestStorm()
		q = s.Limit(123)
	)

	if q.limit != 123 {
		t.Fatalf("Expected limit value of 123 but got %d", q.limit)
	}
}

//Test offset passtrough
func TestStorm_Offset(t *testing.T) {
	var (
		s = newTestStorm()
		q = s.Offset(123)
	)

	if q.offset != 123 {
		t.Fatalf("Expected offset value of 123 but got %d", q.offset)
	}
}

//Test Begin
func TestStorm_Begin(t *testing.T) {
	var (
		s  = newTestStorm()
		tx = s.Begin()
	)

	if tx.DB() == s.DB() {
		t.Fatalf("Expected to get a unique connection diffrent from storm db, but both connections match")
	}
}

//--------------------------------------
// SQL helpers
//--------------------------------------
func TestStorm_generateDeleteSql(t *testing.T) {
	s := newTestStorm()
	entity := testStructure{Id: 1, Name: "test"}
	tbl, _ := s.table(reflect.TypeOf(entity))
	v := reflect.ValueOf(entity)

	sqlQuery, bind := s.generateDeleteSQL(v, tbl)

	if len(bind) != 1 {
		t.Fatalf("Expected to get 1 columns to bind but got %v columns back", len(bind))
	}

	if bind[0] != 1 {
		t.Errorf("Expected to get 1 bind value with the value 1 but got value %v", bind[0])
	}

	sqlExpected := "DELETE FROM `testStructure` WHERE `id` = ?"
	if sqlQuery != sqlExpected {
		t.Errorf("Expected to get query \"%v\" but got the query \"%v\"", sqlExpected, sqlQuery)
	}
}

func TestStorm_generateInsertSQL(t *testing.T) {
	s := newTestStorm()
	entity := testStructure{Id: 0, Name: "test"}
	tbl, _ := s.table(reflect.TypeOf(entity))
	v := reflect.ValueOf(entity)

	sqlQuery, bind := s.generateInsertSQL(v, tbl)

	if len(bind) != 1 {
		t.Fatalf("Expected to get 1 columns to bind but got %v columns back", len(bind))
	}

	if bind[0] != "test" {
		t.Errorf("Expected to get 1 bind value with the value `test` but got value %v", bind[0])
	}

	sqlExpected := "INSERT INTO `testStructure` (`name`) VALUES (?)"
	if sqlQuery != sqlExpected {
		t.Errorf("Expected to get query \"%v\" but got the query \"%v\"", sqlExpected, sqlQuery)
	}
}

func TestStorm_generateUpdateSQL(t *testing.T) {
	s := newTestStorm()
	entity := testStructure{Id: 2, Name: "test"}
	tbl, _ := s.table(reflect.TypeOf(entity))
	v := reflect.ValueOf(entity)

	sqlQuery, bind := s.generateUpdateSQL(v, tbl)

	if len(bind) != 2 {
		t.Fatalf("Expected to get 2 columns to bind but got %v columns back", len(bind))
	}

	if bind[0] != "test" {
		t.Errorf("Expected to get 1st bind value with the value `test` but got value %v", bind[0])
	}

	if bind[1] != 2 {
		t.Errorf("Expected to get 2nd bind value with the value `2` but got value %v", bind[1])
	}

	sqlExpected := "UPDATE `testStructure` SET `name` = ? WHERE `id` = ?"
	if sqlQuery != sqlExpected {
		t.Errorf("Expected to get query \"%v\" but got the query \"%v\"", sqlExpected, sqlQuery)
	}
}

func TestStorm_generateCreateTableSQL(t *testing.T) {
	s := newTestStorm()
	tbl, _ := s.table(reflect.TypeOf((*testAllTypeStructure)(nil)).Elem())

	sqlQuery := s.generateCreateTableSQL(tbl)
	sqlExpected := "CREATE TABLE `testAllTypeStructure` " +
		"(`id` INTEGER PRIMARY KEY,`test_custom_type` INTEGER,`time` DATETIME,`byte` BLOB,`string` TEXT,`int` INTEGER,`int64` BIGINT," +
		"`float64` REAL,`bool` BOOL,`null_string` TEXT,`null_int` BIGINT," +
		"`null_float` REAL,`null_bool` BOOL)"
	if sqlQuery != sqlExpected {
		t.Errorf("Expected to get query \"%v\" but got the query \"%v\"", sqlExpected, sqlQuery)
	}
}

func TestStorm_generateDropTableSQL(t *testing.T) {
	s := newTestStorm()
	tbl, _ := s.table(reflect.TypeOf((*testAllTypeStructure)(nil)).Elem())

	sqlQuery := s.generateDropTableSQL(tbl)
	sqlExpected := "DROP TABLE `testAllTypeStructure`"
	if sqlQuery != sqlExpected {
		t.Errorf("Expected to get query \"%v\" but got the query \"%v\"", sqlExpected, sqlQuery)
	}
}
