package storm

import (
	"database/sql"
	"testing"
)

//Test where passtrough
func TestTransaction_Where(t *testing.T) {
	var (
		s  = newTestStorm()
		tx = s.Begin()
		q  = tx.Where("id = ?", 1)
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
func TestTransaction_Order(t *testing.T) {
	var (
		s  = newTestStorm()
		tx = s.Begin()
		q  = tx.Order("test", ASC)
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
func TestTransaction_Limit(t *testing.T) {
	var (
		s  = newTestStorm()
		tx = s.Begin()
		q  = tx.Limit(123)
	)

	if q.limit != 123 {
		t.Fatalf("Expected limit value of 123 but got %d", q.limit)
	}
}

//Test offset passtrough
func TestTransaction_Offset(t *testing.T) {
	var (
		s  = newTestStorm()
		tx = s.Begin()
		q  = tx.Offset(123)
	)

	if q.offset != 123 {
		t.Fatalf("Expected offset value of 123 but got %d", q.offset)
	}
}

func TestTransaction_Save(t *testing.T) {

	var (
		err   error
		input *testStructure
		s     = newTestStormFile()
		res   *sql.Row
		tx1   = s.Begin()
	)

	//update a existing entity
	_, err = s.DB().Exec("INSERT INTO `testStructure` (`id`, `name`) VALUES (1, 'name')")
	_, err = s.DB().Exec("INSERT INTO `testStructure` (`id`, `name`) VALUES (2, '2nd')")

	if err != nil {
		t.Fatalf("Failure on saving testdate to store `%v`", err)
	}

	input = &testStructure{Id: 1, Name: `test updated`}
	if err = tx1.Save(input); err != nil {
		t.Fatalf("Failed save (update) with error `%v`", err.Error())
	}

	res = tx1.DB().QueryRow("SELECT id, name FROM `testStructure` WHERE `id` = ?", 1)
	if err = res.Scan(&input.Id, &input.Name); err != nil {
		t.Fatalf("Expected to get a row back but got error %v", err)
	}

	if input.Name != "test updated" {
		t.Fatalf("Entity data not updated")
	}

	//check if not modified in other connection (non transactional)
	res = s.DB().QueryRow("SELECT id, name FROM `testStructure` WHERE `id` = ?", 1)
	if err = res.Scan(&input.Id, &input.Name); err != nil {
		t.Fatalf("Expected to get a row back but got error %v", err)
	}

	if input.Name != "name" {
		t.Fatalf("Entity not only in transaction changed")
	}

	//insert a new entity
	input = &testStructure{Id: 0, Name: "test insert"}
	if err = tx1.Save(input); err != nil {
		t.Fatalf("Failed save (insert) with error `%v`", err.Error())
	}

	if input.Id == 0 {
		t.Fatalf("Entity pk id not set")
	}

	if input.Id != 3 {
		t.Fatalf("Expected to get entity PK 3 but got %v", input.Id)
	}

	//query for entity
	res = tx1.DB().QueryRow("SELECT id, name FROM `testStructure` WHERE `id` = ?", 3)
	if err = res.Scan(&input.Id, &input.Name); err != nil {
		t.Fatalf("Expected to get a row back but got error %v", err)
	}

	if err = assertEntity(input, &testStructure{Id: 3, Name: "test insert"}); err != nil {
		t.Fatalf(err.Error())
	}

	res = s.Begin().DB().QueryRow("SELECT id, name FROM `testStructure` WHERE `id` = ?", 3)
	if err = res.Scan(&input.Id, &input.Name); err != sql.ErrNoRows {
		t.Fatalf("Expected to get no rows back but got %v", err)
	}

	//check if not modified in other connection (non transactional)
	res = s.DB().QueryRow("SELECT id, name FROM `testStructure` WHERE `id` = ?", 3)
	if err = res.Scan(&input.Id, &input.Name); err != sql.ErrNoRows {
		t.Fatalf("Expected to get no rows back but got error %v or a record back", err)
	}

	//cleanup
	tx1.tx.Rollback()
}

func TestTransaction_Find(t *testing.T) {
	var (
		err   error
		input *testStructure = nil
		s                    = newTestStormFile()
		tx1   *Transaction   = s.Begin()
	)
	s.DB().Exec("INSERT INTO `testStructure` (`id`, `name`) VALUES (1, 'name')")
	tx1.DB().Exec("INSERT INTO `testStructure` (`id`, `name`) VALUES (2, 'name 2nd')")

	//find by id (transaction)
	input = nil
	if err = tx1.Find(&input, 1); err != nil {
		t.Fatalf("Failed getting by id with error `%v`", err)
	}

	//find by id
	input = nil
	if err = tx1.Find(&input, 2); err != nil {
		t.Fatalf("Failed getting by id with error `%v`", err)
	}

	//find by id (transaction)
	input = nil
	if err = s.Find(&input, 2); err != sql.ErrNoRows {
		t.Fatalf("Expected to get no results back but got error `%v`", err)
	}

	//cleanup
	tx1.tx.Rollback()
}

func TestTransaction_Delete(t *testing.T) {
	var (
		err   error
		input *testStructure = &testStructure{Id: 2, Name: "name delete"}
		s                    = newTestStormFile()
		tx1   *Transaction   = s.Begin()
		res   *sql.Row
	)
	s.DB().Exec("INSERT INTO `testStructure` (`id`, `name`) VALUES (1, 'name')")
	s.DB().Exec("INSERT INTO `testStructure` (`id`, `name`) VALUES (2, 'name delete')")

	//normal
	if err = tx1.Delete(input); err != nil {
		t.Fatalf("Failed delete with error `%v`", err.Error())
	}

	res = tx1.DB().QueryRow("SELECT id, name FROM `testStructure` WHERE `id` = ?", 2)
	if err = res.Scan(&input.Id, &input.Name); err != sql.ErrNoRows {
		if err == nil {
			t.Fatalf("Record not deleted")
		}
		t.Fatalf("Expected to get a ErrNoRows but got %v", err)
	}

	res = s.DB().QueryRow("SELECT id, name FROM `testStructure` WHERE `id` = ?", 2)
	if err = res.Scan(&input.Id, &input.Name); err != nil {
		t.Fatalf("Expected to get a row but got error %v", err)
	}
}

func TestTransaction_Commit(t *testing.T) {

	var (
		err   error
		input *testStructure = &testStructure{}
		s                    = newTestStormFile()
		res   *sql.Row
		tx1   = s.Begin()
	)

	//update a existing entity
	_, err = tx1.DB().Exec("INSERT INTO `testStructure` (`id`, `name`) VALUES (1, 'name')")
	if err != nil {
		t.Fatalf("Failure on saving testdate to store `%v`", err)
	}

	err = tx1.Commit()
	if err != nil {
		t.Fatalf("Error while commit got error `%v`", err)
	}

	res = s.DB().QueryRow("SELECT id, name FROM `testStructure` WHERE `id` = ?", 1)
	if err = res.Scan(&input.Id, &input.Name); err != nil {
		t.Fatalf("Expected to get a row back but got error %v", err)
	}

	if err = assertEntity(input, &testStructure{Id: 1, Name: "name"}); err != nil {
		t.Fatalf("Entity mismatch : %v", err)
	}
}

func TestTransaction_Rollback(t *testing.T) {

	var (
		err   error
		input *testStructure = &testStructure{}
		s                    = newTestStormFile()
		res   *sql.Row
		tx1   = s.Begin()
	)

	//update a existing entity
	_, err = tx1.DB().Exec("INSERT INTO `testStructure` (`id`, `name`) VALUES (1, 'name')")
	if err != nil {
		t.Fatalf("Failure on saving testdate to store `%v`", err)
	}

	err = tx1.Rollback()
	if err != nil {
		t.Fatalf("Error while commit got error `%v`", err)
	}

	res = s.DB().QueryRow("SELECT id, name FROM `testStructure` WHERE `id` = ?", 1)
	if err = res.Scan(&input.Id, &input.Name); err != sql.ErrNoRows {
		t.Fatalf("Expected to get a error back no rows found but got something else %v", err)
	}
}
