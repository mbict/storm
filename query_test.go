package storm

import (
	"database/sql"
	"reflect"
	"testing"
)

func TestQuery_First(t *testing.T) {
	var (
		err      error
		input    testStructure
		inputPtr *testStructure
		s        = newTestStorm()
	)

	s.DB().Exec("INSERT INTO `test_structure` (`id`, `name`) VALUES (1, 'name')")
	s.DB().Exec("INSERT INTO `test_structure` (`id`, `name`) VALUES (2, 'name 2')")

	//empty result, no match
	if err = s.Query().Where("id = ?", 999).First(&input); err != sql.ErrNoRows {
		t.Fatalf("Got wrong error back, expected `%v` but got `%v`", sql.ErrNoRows, err)
	}

	//empty result, no match PTR
	if err = s.Query().Where("id = ?", 999).First(&inputPtr); err != sql.ErrNoRows {
		t.Fatalf("Got wrong error back, expected `%v` but got `%v`", sql.ErrNoRows, err)
	}

	//find by id
	if err = s.Query().Where("id = ?", 1).First(&input); err != nil {
		t.Fatalf("Failed getting by id with error `%v`", err.Error())
	}

	//check if the right item is returned
	if err = assertEntity(&input, &testStructure{Id: 1, Name: "name"}); err != nil {
		t.Fatalf(err.Error())
	}

	//find by id Ptr and assign
	inputPtr = nil
	if err = s.Query().Where("id = ?", 1).First(&inputPtr); err != nil {
		t.Fatalf("Failed getting by id with error `%v`", err.Error())
	}

	//check if the right item is returned
	if err = assertEntity(inputPtr, &testStructure{Id: 1, Name: "name"}); err != nil {
		t.Fatalf(err.Error())
	}

	//check if callback OnInit is called
	if inputPtr.onInitInvoked != true {
		t.Errorf("OnInit function not invoked")
	}
}

func TestQuery_Find_Single(t *testing.T) {
	var (
		err      error
		input    testStructure
		inputPtr *testStructure
		s        = newTestStorm()
	)

	s.DB().Exec("INSERT INTO `test_structure` (`id`, `name`) VALUES (1, 'name')")
	s.DB().Exec("INSERT INTO `test_structure` (`id`, `name`) VALUES (2, 'name 2')")

	//empty result, no match
	if err = s.Query().Where("id = ?", 999).Find(&input); err != sql.ErrNoRows {
		t.Fatalf("Got wrong error back, expected `%v` but got `%v`", sql.ErrNoRows, err)
	}

	//empty result, no match PTR
	if err = s.Query().Where("id = ?", 999).Find(&inputPtr); err != sql.ErrNoRows {
		t.Fatalf("Got wrong error back, expected `%v` but got `%v`", sql.ErrNoRows, err)
	}

	//find by id inline where
	q := s.Query()
	if err = q.Find(&input, 1); err != nil {
		t.Fatalf("Failed getting by id with error `%v`", err.Error())
	}

	//check if the right item is returned
	if err = assertEntity(&input, &testStructure{Id: 1, Name: "name"}); err != nil {
		t.Fatalf(err.Error())
	}

	//find by id inline where are not added to the current query context when set inline
	if err = q.Find(&input, 2); err != nil {
		t.Fatalf("Failed getting by id with error `%v`", err.Error())
	}

	//check if the right item is returned
	if err = assertEntity(&input, &testStructure{Id: 2, Name: "name 2"}); err != nil {
		t.Fatalf(err.Error())
	}

	//find by id
	if err = s.Query().Where("id = ?", 1).Find(&input); err != nil {
		t.Fatalf("Failed getting by id with error `%v`", err.Error())
	}

	//check if the right item is returned
	if err = assertEntity(&input, &testStructure{Id: 1, Name: "name"}); err != nil {
		t.Fatalf(err.Error())
	}

	//find by id Ptr and assign inline where
	inputPtr = nil
	if err = s.Query().Find(&inputPtr, 2); err != nil {
		t.Fatalf("Failed getting by id with error `%v`", err.Error())
	}

	//check if the right item is returned
	if err = assertEntity(inputPtr, &testStructure{Id: 2, Name: "name 2"}); err != nil {
		t.Fatalf(err.Error())
	}

	//find by id Ptr and assign
	inputPtr = nil
	if err = s.Query().Where("id = ?", 1).Find(&inputPtr); err != nil {
		t.Fatalf("Failed getting by id with error `%v`", err.Error())
	}

	//check if the right item is returned
	if err = assertEntity(inputPtr, &testStructure{Id: 1, Name: "name"}); err != nil {
		t.Fatalf(err.Error())
	}

	//check if callback OnInit is called
	if inputPtr.onInitInvoked != true {
		t.Errorf("OnInit function not invoked")
	}
	
	//make sure when we recycle a pointer its reset to a zero value
}

//where and inline find with related object
func TestQuery_Find_Single_WhereRelParentRecord(t *testing.T) {
	var (
		err        error
		inputPtr   *testRelatedStructure
		testRecord = &testStructure{Id: 2, Name: "name 2"}
		s          = newTestStorm()
	)

	s.DB().Exec("INSERT INTO `test_structure` (`id`, `name`) VALUES (1, 'name')")
	s.DB().Exec("INSERT INTO `test_structure` (`id`, `name`) VALUES (2, 'name 2')")
	s.DB().Exec("INSERT INTO `test_related_structure` (`id`, `test_structure_id`, `name`) VALUES (1, 1, 'name 1')")
	s.DB().Exec("INSERT INTO `test_related_structure` (`id`, `test_structure_id`, `name`) VALUES (2, 1, 'name 2')")
	s.DB().Exec("INSERT INTO `test_related_structure` (`id`, `test_structure_id`, `name`) VALUES (3, 2, 'name 3')")

	//where with string condition
	if err = s.Query().Where("test_structure_id = ?", testRecord).Find(&inputPtr); err != nil {
		t.Fatalf("Got unexpected error back got `%v`", err)
	}

	//check if the right item is returned
	if err = assertRelatedEntity(inputPtr, &testRelatedStructure{Id: 3, TestStructureId: 2, Name: "name 3"}); err != nil {
		t.Fatalf(err.Error())
	}

	//inline where find
	inputPtr = nil
	if err = s.Query().Find(&inputPtr, testRecord); err != nil {
		t.Fatalf("Got unexpected error back got `%v`", err)
	}

	//check if the right item is returned
	if err = assertRelatedEntity(inputPtr, &testRelatedStructure{Id: 3, TestStructureId: 2, Name: "name 3"}); err != nil {
		t.Fatalf(err.Error())
	}
}

func TestQuery_Find_Slice(t *testing.T) {
	var (
		err      error
		inputPtr []*testStructure
		input    []testStructure
		s        = newTestStorm()
	)

	s.DB().Exec("INSERT INTO `test_structure` (`id`, `name`) VALUES (1, 'name')")
	s.DB().Exec("INSERT INTO `test_structure` (`id`, `name`) VALUES (2, 'name 2')")
	s.DB().Exec("INSERT INTO `test_structure` (`id`, `name`) VALUES (3, 'name 3')")
	s.DB().Exec("INSERT INTO `test_structure` (`id`, `name`) VALUES (4, 'name 4')")

	//empty result, no match PTR
	inputPtr = nil
	if err = s.Query().Where("id > ?", 999).Find(&inputPtr); err != sql.ErrNoRows {
		t.Fatalf("Got wrong error back, expected `%v` but got `%v`", sql.ErrNoRows, err)
	}

	if inputPtr != nil {
		t.Fatalf("Not a nil record returned while we expected a nil record")
	}

	//empty result, no match
	if err = s.Query().Where("id > ?", 999).Find(&input); err != sql.ErrNoRows {
		t.Fatalf("Got wrong error back, expected `%v` but got `%v`", sql.ErrNoRows, err)
	}

	if input != nil {
		t.Fatalf("Not a nil record returned while we expected a nil record")
	}

	//find by id PTR
	if err = s.Query().Where("id > ?", 1).Find(&inputPtr); err != nil {
		t.Fatalf("Failed getting by id with error `%v`", err)
	}

	if len(inputPtr) != 3 {
		t.Fatalf("Expected to get %d records back but got %d", 3, len(inputPtr))
	}

	//find by id PTR and where statement inline
	q := s.Query()
	if err = q.Find(&inputPtr, 1); err != nil {
		t.Fatalf("Failed getting by id with error `%v`", err)
	}

	if len(inputPtr) != 1 {
		t.Fatalf("Expected to get %d records back but got %d", 1, len(inputPtr))
	}

	//check if the right item is returned
	if err = assertEntity(inputPtr[0], &testStructure{Id: 1, Name: "name"}); err != nil {
		t.Fatalf(err.Error())
	}

	//find by inline statmement previous inline should not be added to current query context
	if err = q.Find(&inputPtr, 2); err != nil {
		t.Fatalf("Failed getting by id with error `%v`", err)
	}

	if len(inputPtr) != 1 {
		t.Fatalf("Expected to get %d records back but got %d", 1, len(inputPtr))
	}

	//check if the right item is returned
	if err = assertEntity(inputPtr[0], &testStructure{Id: 2, Name: "name 2"}); err != nil {
		t.Fatalf(err.Error())
	}

	//check if slice count is reset, and not appended (bug)
	inputPtr = []*testStructure{&testStructure{}}
	if err = s.Query().Where("id > ?", 1).Find(&inputPtr); err != nil {
		t.Fatalf("Failed getting by id with error `%v`", err)
	}

	if len(inputPtr) != 3 {
		t.Fatalf("Expected to have %d records inslice but got %d items is slice", 3, len(inputPtr))
	}

	//find by id and where statement inline
	input = nil
	if err = s.Query().Find(&input, 1); err != nil {
		t.Fatalf("Failed getting by id with error `%v`", err)
	}

	if len(input) != 1 {
		t.Fatalf("Expected to get %d records back but got %d", 1, len(input))
	}

	//find by id
	input = nil
	if err = s.Query().Where("id > ?", 1).Find(&input); err != nil {
		t.Fatalf("Failed getting by id with error `%v`", err)
	}

	if len(input) != 3 {
		t.Fatalf("Expected to get %d records back but got %d", 3, len(input))
	}

	//check if callback OnInit is called
	if input[0].onInitInvoked != true || input[1].onInitInvoked != true || input[2].onInitInvoked != true {
		t.Errorf("OnInit function not invoked")
	}

	//check if slice count is reset, and not appended (bug)
	input = []testStructure{testStructure{}}
	if err = s.Query().Where("id > ?", 1).Find(&input); err != nil {
		t.Fatalf("Failed getting by id with error `%v`", err)
	}

	if len(input) != 3 {
		t.Fatalf("Expected to have %d records inslice but got %d items is slice", 3, len(input))
	}
	
	//BUG: make sure if we recycle a pointer its initialized to zero
	if err = s.Find(&input, `id = ?`, 999); err != sql.ErrNoRows {
		t.Fatalf("Failed getting by id with error `%v`", err)
	}

	if len(input) != 0 {
		t.Fatalf("Expected to have no records inslice but got %d items is slice", len(input))
	}
}

//where and inline find with related object
func TestQuery_Find_Slice_WhereRelParentRecord(t *testing.T) {
	var (
		err        error
		inputPtr   []*testRelatedStructure
		testRecord = &testStructure{Id: 2, Name: "name 2"}
		s          = newTestStorm()
	)

	s.DB().Exec("INSERT INTO `test_structure` (`id`, `name`) VALUES (1, 'name')")
	s.DB().Exec("INSERT INTO `test_structure` (`id`, `name`) VALUES (2, 'name 2')")
	s.DB().Exec("INSERT INTO `test_related_structure` (`id`, `test_structure_id`, `name`) VALUES (1, 1, 'name 1')")
	s.DB().Exec("INSERT INTO `test_related_structure` (`id`, `test_structure_id`, `name`) VALUES (2, 1, 'name 2')")
	s.DB().Exec("INSERT INTO `test_related_structure` (`id`, `test_structure_id`, `name`) VALUES (3, 2, 'name 3')")
	s.DB().Exec("INSERT INTO `test_related_structure` (`id`, `test_structure_id`, `name`) VALUES (4, 2, 'name 4')")

	//where with string condition
	if err = s.Query().Where("test_structure_id = ?", testRecord).Find(&inputPtr); err != nil {
		t.Fatalf("Got unexpected error back got `%v`", err)
	}

	if len(inputPtr) != 2 {
		t.Fatalf("Expected %d results but got %d results back", 2, len(inputPtr))
	}

	//check if the right items are returned
	if err = assertRelatedEntity(inputPtr[0], &testRelatedStructure{Id: 3, TestStructureId: 2, Name: "name 3"}); err != nil {
		t.Fatalf(err.Error())
	}

	if err = assertRelatedEntity(inputPtr[1], &testRelatedStructure{Id: 4, TestStructureId: 2, Name: "name 4"}); err != nil {
		t.Fatalf(err.Error())
	}

	//inline where find
	inputPtr = nil
	if err = s.Query().Find(&inputPtr, testRecord); err != nil {
		t.Fatalf("Got unexpected error back got `%v`", err)
	}

	if len(inputPtr) != 2 {
		t.Fatalf("Expected %d results but got %d results back", 2, len(inputPtr))
	}

}

func TestQuery_Count(t *testing.T) {
	var (
		err error
		cnt int64
		s   = newTestStorm()
	)

	s.DB().Exec("INSERT INTO `test_structure` (`id`, `name`) VALUES (1, 'name')")
	s.DB().Exec("INSERT INTO `test_structure` (`id`, `name`) VALUES (2, 'name 2')")
	s.DB().Exec("INSERT INTO `test_structure` (`id`, `name`) VALUES (3, 'name 3')")
	s.DB().Exec("INSERT INTO `test_structure` (`id`, `name`) VALUES (4, 'name 4')")

	//empty result, no match PTR
	if cnt, err = s.Query().Where("id > ?", 999).Count((*testStructure)(nil)); err != nil {
		t.Fatalf("Got wrong error back, expected `%v` but got `%v`", sql.ErrNoRows, err)
	}

	if cnt != 0 {
		t.Fatalf("Expected a 0 count but got %d", cnt)
	}

	//find by id PTR
	if cnt, err = s.Query().Where("id > ?", 1).Count((*testStructure)(nil)); err != nil {
		t.Fatalf("Failed getting by id with error `%v`", err)
	}

	if cnt != 3 {
		t.Fatalf("Expected a 3 count but got %d", cnt)
	}
}

//helper tests
func TestQuery_generateSelect(t *testing.T) {

	s := newTestStorm()
	q := s.Query()
	tbl, _ := s.table(reflect.TypeOf((*testStructure)(nil)).Elem())

	//simple empty select
	sqlQuery, bind := q.generateSelectSQL(tbl)

	if len(bind) != 0 {
		t.Errorf("Expected to get 0 columns to bind but got %v columns back", len(bind))
	}

	sqlExpected := "SELECT `id`, `name` FROM `test_structure`"
	if sqlQuery != sqlExpected {
		t.Errorf("Expected to get query \"%v\" but got the query \"%v\"", sqlExpected, sqlQuery)
	}

	//where/limit/offset/order/order test
	q = s.Query()
	q.Where("id = ?", 1).
		Where("name = ?", "test").
		Limit(10).
		Offset(5).
		Order("id", ASC).
		Order("name", DESC)

	sql, bind := q.generateSelectSQL(tbl)

	if len(bind) != 2 {
		t.Errorf("Expected to get 2 columns to bind but got %v columns back", len(bind))
	}

	sqlExpected = "SELECT `id`, `name` FROM `test_structure` WHERE id = ? AND name = ? ORDER BY `id` ASC, `name` DESC LIMIT 10 OFFSET 5"
	if sql != sqlExpected {
		t.Errorf("Expected to get query \"%v\" but got the query \"%v\"", sqlExpected, sql)
	}
}

func TestQuery_generateCount(t *testing.T) {

	s := newTestStorm()
	q := s.Query()
	tbl, _ := s.table(reflect.TypeOf((*testStructure)(nil)).Elem())

	//simple empty select
	sqlQuery, bind := q.generateCountSQL(tbl)

	if len(bind) != 0 {
		t.Errorf("Expected to get 0 columns to bind but got %v columns back", len(bind))
	}

	sqlExpected := "SELECT COUNT(*) FROM `test_structure`"
	if sqlQuery != sqlExpected {
		t.Errorf("Expected to get query \"%v\" but got the query \"%v\"", sqlExpected, sqlQuery)
	}

	//where/limit/offset/order/order test
	q = s.Query()
	q.Where("id = ?", 1).
		Where("name = ?", "test").
		Limit(10).
		Offset(5).
		Order("id", ASC).
		Order("name", DESC)

	sql, bind := q.generateCountSQL(tbl)

	if len(bind) != 2 {
		t.Errorf("Expected to get 2 columns to bind but got %v columns back", len(bind))
	}

	sqlExpected = "SELECT COUNT(*) FROM `test_structure` WHERE id = ? AND name = ?"
	if sql != sqlExpected {
		t.Errorf("Expected to get query \"%v\" but got the query \"%v\"", sqlExpected, sql)
	}
}
