package storm

import (
	"database/sql"
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	. "gopkg.in/check.v1"
)

//*** test suite setup ***/
type transactionSuite struct {
	db       *Storm
	tx       *Transaction
	tempName string
}

var _ = Suite(&transactionSuite{})

func (s *transactionSuite) SetUpSuite(c *C) {
	//create temporary table (for transactions we need a physical database, sql lite doesnt support memory transactions)
	tmp, err := ioutil.TempFile("", "storm_test.sqlite_")
	c.Assert(err, IsNil)
	tmp.Close()
	s.tempName = tmp.Name()

	s.db, err = Open(`sqlite3`, `file:`+s.tempName+`?mode=rwc`)
	c.Assert(s.db, NotNil)
	c.Assert(err, IsNil)

	c.Assert(s.db.RegisterStructure((*Person)(nil)), IsNil)

	s.db.SetMaxIdleConns(10)
	s.db.SetMaxOpenConns(10)
}

func (s *transactionSuite) SetUpTest(c *C) {
	s.db.DB().Exec("DROP TABLE `person`")
	_, err := s.db.DB().Exec("CREATE TABLE `person` (`id` INTEGER PRIMARY KEY, `name` TEXT, `address_id` INTEGER, `optional_address_id` INTEGER)")
	c.Assert(err, IsNil)

	//c.Assert(err, IsNil)
	s.tx = s.db.Begin()
}

func (s *transactionSuite) TearDownTest(c *C) {
	s.tx.Rollback()
}

func (s *transactionSuite) TearDownSuite(c *C) {
	s.db.Close()

	//remove database
	os.Remove(s.tempName)
}

/*** tests ***/

//test if we get the storm instance back
func (s *transactionSuite) TestStorm(c *C) {
	c.Assert(s.tx.Storm(), Equals, s.db)
}

func (s *transactionSuite) TestTable(c *C) {
	tbl, ok := s.tx.table(reflect.TypeOf((*Person)(nil)).Elem())
	c.Assert(ok, Equals, true)
	c.Assert(tbl, NotNil)
	c.Assert(tbl.tableName, Equals, "person")

	type notRegisteredStruct struct{}
	_, ok = s.tx.table(reflect.TypeOf((*notRegisteredStruct)(nil)).Elem())
	c.Assert(ok, Equals, false)
}

func (s *transactionSuite) TestTableByName(c *C) {
	expectedTbl, ok := s.tx.table(reflect.TypeOf((*Person)(nil)).Elem())
	c.Assert(ok, Equals, true)

	tbl, ok := s.tx.tableByName("person")
	c.Assert(ok, Equals, true)
	c.Assert(tbl, Equals, expectedTbl)

	tbl, ok = s.tx.tableByName("tableNoExistie")
	c.Assert(ok, Equals, false)
	c.Assert(tbl, IsNil)
}

//Test where passtrough
func (s *transactionSuite) TestWhere(c *C) {
	q := s.tx.Where("id = ?", 1)
	c.Assert(q.where, HasLen, 1)
	c.Assert(q.where[0].Statement, Equals, "id = ?")
	c.Assert(q.where[0].Bindings, HasLen, 1)
	c.Assert(q.where[0].Bindings[0].(int), Equals, int(1))
}

//Test order passtrough
func (s *transactionSuite) TestOrder(c *C) {
	q := s.tx.Order("test", ASC)
	c.Assert(q.order, HasLen, 1)
	c.Assert(q.order[0].Statement, Equals, "test")
	c.Assert(q.order[0].Direction, Equals, ASC)
}

//Test limit passtrough
func (s *transactionSuite) TestLimit(c *C) {
	c.Assert(s.tx.Limit(123).limit, Equals, 123)
}

//Test offset passtrough
func (s *transactionSuite) TestOffset(c *C) {
	c.Assert(s.tx.Offset(123).offset, Equals, 123)
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
	_, err = s.DB().Exec("INSERT INTO `test_structure` (`id`, `name`) VALUES (1, 'name')")
	_, err = s.DB().Exec("INSERT INTO `test_structure` (`id`, `name`) VALUES (2, '2nd')")

	if err != nil {
		t.Fatalf("Failure on saving testdate to store `%v`", err)
	}

	input = &testStructure{Id: 1, Name: `test updated`}
	if err = tx1.Save(input); err != nil {
		t.Fatalf("Failed save (update) with error `%v`", err.Error())
	}

	res = tx1.DB().QueryRow("SELECT id, name FROM `test_structure` WHERE `id` = ?", 1)
	if err = res.Scan(&input.Id, &input.Name); err != nil {
		t.Fatalf("Expected to get a row back but got error %v", err)
	}

	if input.Name != "test updated" {
		t.Fatalf("Entity data not updated")
	}

	//check if not modified in other connection (non transactional)
	res = s.DB().QueryRow("SELECT id, name FROM `test_structure` WHERE `id` = ?", 1)
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
	res = tx1.DB().QueryRow("SELECT id, name FROM `test_structure` WHERE `id` = ?", 3)
	if err = res.Scan(&input.Id, &input.Name); err != nil {
		t.Fatalf("Expected to get a row back but got error %v", err)
	}

	if err = assertEntity(input, &testStructure{Id: 3, Name: "test insert"}); err != nil {
		t.Fatalf(err.Error())
	}

	res = s.Begin().DB().QueryRow("SELECT id, name FROM `test_structure` WHERE `id` = ?", 3)
	if err = res.Scan(&input.Id, &input.Name); err != sql.ErrNoRows {
		t.Fatalf("Expected to get no rows back but got %v", err)
	}

	//check if not modified in other connection (non transactional)
	res = s.DB().QueryRow("SELECT id, name FROM `test_structure` WHERE `id` = ?", 3)
	if err = res.Scan(&input.Id, &input.Name); err != sql.ErrNoRows {
		t.Fatalf("Expected to get no rows back but got error %v or a record back", err)
	}

	//cleanup
	tx1.tx.Rollback()
}

func TestTransaction_Find_Single(t *testing.T) {
	var (
		err   error
		input *testStructure
		s     = newTestStormFile()
		tx1   = s.Begin()
	)
	s.DB().Exec("INSERT INTO `test_structure` (`id`, `name`) VALUES (1, 'name')")
	tx1.DB().Exec("INSERT INTO `test_structure` (`id`, `name`) VALUES (2, 'name 2nd')")
	tx1.DB().Exec("INSERT INTO `test_structure` (`id`, `name`) VALUES (3, 'name 3nd')")

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

func TestTransaction_Find_Slice(t *testing.T) {
	var (
		err   error
		input []*testStructure
		s     = newTestStormFile()
		tx1   = s.Begin()
	)
	s.DB().Exec("INSERT INTO `test_structure` (`id`, `name`) VALUES (1, 'name')")
	tx1.DB().Exec("INSERT INTO `test_structure` (`id`, `name`) VALUES (2, 'name 2nd')")
	tx1.DB().Exec("INSERT INTO `test_structure` (`id`, `name`) VALUES (3, 'name 3nd')")

	//find by id (transaction)
	input = nil
	if err = tx1.Find(&input, 1); err != nil {
		t.Fatalf("Failed getting by id with error `%v`", err)
	}

	if len(input) != 1 {
		t.Fatalf("Expected to get %d record back but got %d", 1, len(input))
	}

	//find by id
	input = nil
	if err = tx1.Find(&input, 2); err != nil {
		t.Fatalf("Failed getting by id with error `%v`", err)
	}

	if len(input) != 1 {
		t.Fatalf("Expected to get %d record back but got %d", 1, len(input))
	}

	//get all (transaction)
	input = nil
	if err = tx1.Find(&input); err != nil {
		t.Fatalf("Failed getting by id with error `%v`", err)
	}

	if len(input) != 3 {
		t.Fatalf("Expected to get %d record back but got %d", 3, len(input))
	}

	//find by id (transaction)
	input = nil
	if err = s.Find(&input, 2); err != sql.ErrNoRows {
		t.Fatalf("Expected to get no results back but got error `%v`", err)
	}

	//get all
	input = nil
	if err = s.Find(&input); err != nil {
		t.Fatalf("Failed getting by id with error `%v`", err)
	}

	if len(input) != 1 {
		t.Fatalf("Expected to get %d record back but got %d", 1, len(input))
	}

	//cleanup
	tx1.tx.Rollback()
}

func TestTransaction_Delete(t *testing.T) {
	var (
		err   error
		input = &testStructure{Id: 2, Name: "name delete"}
		s     = newTestStormFile()
		tx1   = s.Begin()
		res   *sql.Row
	)
	s.DB().Exec("INSERT INTO `test_structure` (`id`, `name`) VALUES (1, 'name')")
	s.DB().Exec("INSERT INTO `test_structure` (`id`, `name`) VALUES (2, 'name delete')")

	//normal
	if err = tx1.Delete(input); err != nil {
		t.Fatalf("Failed delete with error `%v`", err.Error())
	}

	res = tx1.DB().QueryRow("SELECT id, name FROM `test_structure` WHERE `id` = ?", 2)
	if err = res.Scan(&input.Id, &input.Name); err != sql.ErrNoRows {
		if err == nil {
			t.Fatalf("Record not deleted")
		}
		t.Fatalf("Expected to get a ErrNoRows but got %v", err)
	}

	res = s.DB().QueryRow("SELECT id, name FROM `test_structure` WHERE `id` = ?", 2)
	if err = res.Scan(&input.Id, &input.Name); err != nil {
		t.Fatalf("Expected to get a row but got error %v", err)
	}
}

func (s *transactionSuite) TestCommit(c *C) {
	person := &Person{Id: 0, Name: "test"}
	personCommit := &Person{}
	c.Assert(s.tx.Save(&person), IsNil) //insert new
	c.Assert(s.tx.Commit(), IsNil)
	c.Assert(s.db.Find(&personCommit, 1), IsNil)
	c.Assert(personCommit.Id, Equals, person.Id)
	c.Assert(personCommit.Name, Equals, person.Name)
}

func (s *transactionSuite) TestRollback(c *C) {
	person := &Person{Id: 0, Name: "test"}
	c.Assert(s.db.Find(&person, 1), Equals, sql.ErrNoRows)

	c.Assert(s.tx.Save(&person), IsNil) //insert new
	c.Assert(s.tx.Rollback(), IsNil)
	s.db.Find(&person, 1)
	c.Assert(s.db.Find(&person, 1), Equals, sql.ErrNoRows)
}
