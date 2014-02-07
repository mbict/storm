package storm2

import (
	"database/sql"
	"reflect"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestStorm_RegisterStructure(t *testing.T) {

	var (
		s          *Storm
		err        error
		typeStruct = reflect.TypeOf(testStructure{})
		tbl        *table
		ok         bool
	)

	//register by casted nil type
	s, _ = Open(`sqlite3`, `:memory:`)
	err = s.RegisterStructure((*testStructure)(nil), `testStructure`)
	if err != nil {
		t.Fatalf("Failed with error : %v", err)
	}

	if tbl, ok = s.tables[typeStruct]; !ok || tbl == nil {
		t.Fatalf("added table information not found")
	}

	//register by element
	s, _ = Open(`sqlite3`, `:memory:`)
	structure := testStructure{}
	err = s.RegisterStructure(structure, `testStructure`)
	if err != nil {
		t.Fatalf("Failed with error : %v", err)
	}

	if tbl, ok = s.tables[typeStruct]; !ok || tbl == nil {
		t.Fatalf("added table information not found")
	}

	//register by nil element
	s, _ = Open(`sqlite3`, `:memory:`)
	structurePtr := &testStructure{}
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
		expectedError string = `Provided input is not a structure type`
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

	expectedError = "Duplicate structure, 'storm2.testProduct' already exists"
	err = s.RegisterStructure((*testProduct)(nil), `testStructure`)
	if err == nil || err.Error() != expectedError {
		t.Fatalf("Expected error `%v`, but got `%v`", expectedError, err)
	}

}

func TestStorm_Find(t *testing.T) {
	var (
		err   error
		input *testStructure = nil
		s                    = newTestStorm()
	)
	s.DB().Exec("INSERT INTO `testStructure` (`id`, `name`) VALUES (1, 'name')")

	//empty result, no match
	if err = s.Find(&input, 999); err != sql.ErrNoRows {
		t.Fatalf("Got wrong error back, expected `%v` but got `%v`", sql.ErrNoRows, err)
	}

	//find by id
	input = nil
	if err = s.Find(&input, 1); err != nil {
		t.Fatalf("Failed getting by id with error `%v`", err)
	}

	if err = assertEntity(input, &testStructure{1, "name"}); err != nil {
		t.Fatalf("Error: %v",err)
	}

	//find by string
	input = nil
	if err = s.Find(&input, `id = 1`); err != nil {
		t.Fatalf("Failed getting by id with error `%v`", err)
	}

	if err = assertEntity(input, &testStructure{1, "name"}); err != nil {
		t.Fatalf("Error: %v",err)
	}

	//find by query bind string (shorthand for query)
	input = nil
	if err = s.Find(&input, `id = ?`, 1); err != nil {
		t.Fatalf("Failed getting by id with error `%v`", err)
	}

	if err = assertEntity(input, &testStructure{1, "name"}); err != nil {
		t.Fatalf("Error: %v",err)
	}

	//find by multiple bind string (shorthand for query)
	input = nil
	if err = s.Find(&input, `id = ? AND name = ?`, 1, `name`); err != nil {
		t.Fatalf("Failed getting by id with error `%v`", err)
	}

	if err = assertEntity(input, &testStructure{1, "name"}); err != nil {
		t.Fatalf("Error: %v",err)
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

	expectedError = `Provided structure is not a pointer type`
	if err.Error() != expectedError {
		t.Fatalf("Expected error `%v`, but got `%v`", expectedError, err.Error())
	}

	//not a structure pointer
	var inputIntPtr *int
	if err = s.Find(inputIntPtr, 1); err == nil {
		t.Fatalf("Expected a error but got none")
	}

	expectedError = `Provided input is not a structure type`
	if err.Error() != expectedError {
		t.Fatalf("Expected error `%v`, but got `%v`", expectedError, err.Error())
	}

	//not registered structure
	type testNonRegisteredStruct struct{}
	if err = s.Find(&testNonRegisteredStruct{}, 1); err == nil {
		t.Fatalf("Expected a error but got none")
	}

	expectedError = "No registered structure for `storm2.testNonRegisteredStruct` found"
	if err.Error() != expectedError {
		t.Fatalf("Expected error `%v`, but got `%v`", expectedError, err.Error())
	}
}

func TestStorm_Delete(t *testing.T) {
	var (
		err   error
		input testStructure = testStructure{1, `name`}
		s                   = newTestStorm()
		res   *sql.Row
	)

	//normal
	_, err = s.DB().Exec("INSERT INTO `testStructure` (`id`, `name`) VALUES (1, 'name')")
	if err != nil {
		t.Fatalf("Failure on saving testdate to store `%v`", err)
	}

	if err = s.Delete(input); err != nil {
		t.Fatalf("Failed delete with error `%v`", err.Error())
	}

	res = s.DB().QueryRow("SELECT id, name FROM `testStructure` WHERE `id` = ?", 1)
	if err = res.Scan( &input.Id, &input.Name ); err != sql.ErrNoRows {
		if err == nil {
			t.Fatalf("Record not deleted")
		}
		t.Fatalf("Expected to get a ErrNoRows but got %v", err)
	}

	//pointer variant
	_, err = s.DB().Exec("INSERT INTO `testStructure` (`id`, `name`) VALUES (1, 'name')")
	if err != nil {
		t.Fatalf("Failure on saving testdate to store `%v`", err)
	}

	if err = s.Delete(&input); err != nil {
		t.Fatalf("Failed delete with error `%v`", err.Error())
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

	expectedError = `Provided input is not a structure type`
	if err == nil || err.Error() != expectedError {
		t.Fatalf("Expected error `%v`, but got `%v`", expectedError, err)
	}

	//not a structure pointer
	var inputIntPtr *int
	if err = s.Delete(inputIntPtr); err == nil {
		t.Fatalf("Expected a error but got none")
	}

	expectedError = `Provided input is not a structure type`
	if err == nil || err.Error() != expectedError {
		t.Fatalf("Expected error `%v`, but got `%v`", expectedError, err)
	}

	//not registered structure
	type testNonRegisteredStruct struct{}
	if err = s.Delete(&testNonRegisteredStruct{}); err == nil {
		t.Fatalf("Expected a error but got none")
	}

	expectedError = "No registered structure for `storm2.testNonRegisteredStruct` found"
	if err == nil || err.Error() != expectedError {
		t.Fatalf("Expected error `%v`, but got `%v`", expectedError, err)
	}
}


func TestStorm_Save(t *testing.T) {

	var (
		err   error
		input *testStructure
		s     = newTestStorm()
		res   *sql.Row
	)

	//update a existing entity
	_, err = s.DB().Exec("INSERT INTO `testStructure` (`id`, `name`) VALUES (1, 'name')")
	_, err = s.DB().Exec("INSERT INTO `testStructure` (`id`, `name`) VALUES (2, '2nd')")
	if err != nil {
		t.Fatalf("Failure on saving testdate to store `%v`", err)
	}

	input = &testStructure{1, `test updated`}
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

	//insert a new entity
	input = &testStructure{0, "test insert"}
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

	if err = assertEntity(input, &testStructure{3, "test insert"}); err != nil {
		t.Fatalf(err.Error())
	}
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

	expectedError = `Provided structure is not a pointer type`
	if err.Error() != expectedError {
		t.Fatalf("Expected error `%v`, but got `%v`", expectedError, err.Error())
	}

	//not a structure pointer
	var inputIntPtr *int
	if err = s.Save(inputIntPtr); err == nil {
		t.Fatalf("Expected a error but got none")
	}

	expectedError = `Provided input is not a structure type`
	if err.Error() != expectedError {
		t.Fatalf("Expected error `%v`, but got `%v`", expectedError, err.Error())
	}

	//not registered structure
	type testNonRegisteredStruct struct{}
	if err = s.Save(&testNonRegisteredStruct{}); err == nil {
		t.Fatalf("Expected a error but got none")
	}

	expectedError = "No registered structure for `storm2.testNonRegisteredStruct` found"
	if err.Error() != expectedError {
		t.Fatalf("Expected error `%v`, but got `%v`", expectedError, err.Error())
	}
}

/*
func TestStorm_CreateTable(t *testing.T) {
	var (
		err    error
		result int = 0
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
		result int = 0
		s          = newTestStorm()
	)

	//check if table does exists
	result, err = assertTableExist("testStructure", s.DB())
	if err != nil {
		t.Fatalf("Error while determing if table exists `%v`", err)
	}

	if result != 1 {
		t.Fatalf("Table does not exist, nothing to drop", err)
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
		s  = newTestStorm()
		q  = s.Where("id = ?", 1)
		v  []interface{}
		ok bool
	)

	if v, ok = q.where["id = ?"]; ok != true {
		t.Fatalf("Where statement not found in query")
	}

	if len(v) != 1 && v[0].(int) != 1 {
		t.Fatalf("Expected where statement value")
	}
}

//Test order passtrough
func TestStorm_Order(t *testing.T) {
	var (
		s  = newTestStorm()
		q  = s.Order("test", ASC)
		v  SortDirection
		ok bool
	)

	if v, ok = q.order["test"]; ok != true {
		t.Fatalf("Order statement not found in query")
	}

	if v != ASC {
		t.Fatalf("Expected order statement value")
	}
}

//Test limit passtrough
func TestStorm_Limit(t *testing.T) {
	var (
		s  = newTestStorm()
		q  = s.Limit(123)
	)

	if q.limit != 123 {
		t.Fatalf("Expected limit value of 123 but got %d", q.limit)
	}
}

//Test offset passtrough
func TestStorm_Offset(t *testing.T) {
	var (
		s  = newTestStorm()
		q  = s.Offset(123)
	)

	if q.offset != 123 {
		t.Fatalf("Expected offset value of 123 but got %d", q.offset)
	}
}

//--------------------------------------
// SQL helpers
//--------------------------------------
*/
func TestStorm_generateDeleteSql(t *testing.T) {
	s := newTestStorm()
	entity := testStructure{1, "test"}
	tbl, _ := s.getTable(reflect.TypeOf(entity))
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
	entity := testStructure{0, "test"}
	tbl, _ := s.getTable(reflect.TypeOf(entity))
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
	entity := testStructure{2, "test"}
	tbl, _ := s.getTable(reflect.TypeOf(entity))
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

/*
func TestStorm_generateCreateTableSQL(t *testing.T) {
	s := newTestStorm()
	tbl, _ := s.getTable(reflect.TypeOf((*testStructure)(nil)).Elem())

	sqlQuery := s.generateCreateTableSQL(tbl)
	sqlExpected := "CREATE TABLE `testStructure` (`id` integer,`name` text)"
	if sqlQuery != sqlExpected {
		t.Errorf("Expected to get query \"%v\" but got the query \"%v\"", sqlExpected, sqlQuery)
	}
}


func TestStorm_generateDropTableSQL(t *testing.T) {
	s := newTestStorm()
	tbl, _ := s.getTable(reflect.TypeOf((*testStructure)(nil)).Elem())

	sqlQuery := s.generateDropTableSQL(tbl)
	sqlExpected := "DROP TABLE `testStructure`"
	if sqlQuery != sqlExpected {
		t.Errorf("Expected to get query \"%v\" but got the query \"%v\"", sqlExpected, sqlQuery)
	}
}
*/
