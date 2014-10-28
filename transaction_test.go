package storm

import (
	"database/sql"
	"io/ioutil"
	"os"
	"reflect"

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

func (s *transactionSuite) TestSave_Insert(c *C) {
	insert := &Person{Name: "first"}
	var compare *Person

	c.Assert(s.tx.Save(&insert), IsNil)
	c.Assert(s.tx.Find(&compare, insert.Id), IsNil)
	c.Assert(compare.Name, Equals, insert.Name)

	//should not be found
	c.Assert(s.db.Find(&compare, insert.Id), Equals, sql.ErrNoRows)
}

func (s *transactionSuite) TestSave_Update(c *C) {
	first := &Person{Name: "first"}

	c.Assert(s.db.Save(&first), IsNil)
	c.Assert(s.db.Save(&Person{Name: "2nd"}), IsNil)

	updated := &Person{Id: first.Id, Name: `test updated`}
	c.Assert(s.tx.Save(&updated), IsNil)

	var compare *Person

	//current transaction new value
	c.Assert(s.tx.Find(&compare, first.Id), IsNil)
	c.Assert(compare.Name, Equals, updated.Name)

	//should be old value
	c.Assert(s.db.Find(&compare, first.Id), IsNil)
	c.Assert(compare.Name, Equals, first.Name)
}

//simple test for the passtrough transaction
func (s *transactionSuite) TestQuery(c *C) {
	var compare *Person
	row1 := &Person{Name: "first"}
	row2 := &Person{Name: "2nd"}

	c.Assert(s.db.Save(&row1), IsNil)
	c.Assert(s.tx.Save(&row2), IsNil)

	q := s.tx.Query()

	c.Assert(q.Find(&compare, row1.Id), IsNil)
	c.Assert(q.Find(&compare, row2.Id), IsNil)
}

//simple test for the passtrough transaction
func (s *transactionSuite) TestFind(c *C) {
	var compare *Person
	row1 := &Person{Name: "first"}
	row2 := &Person{Name: "2nd"}

	c.Assert(s.db.Save(&row1), IsNil)
	c.Assert(s.tx.Save(&row2), IsNil)

	c.Assert(s.tx.Find(&compare, row1.Id), IsNil)
	c.Assert(s.tx.Find(&compare, row2.Id), IsNil)

	c.Assert(s.db.Find(&compare, row1.Id), IsNil)
	c.Assert(s.db.Find(&compare, row2.Id), Equals, sql.ErrNoRows)
}

func (s *transactionSuite) TestFind_Slice(c *C) {

	var compare []*Person
	row1 := &Person{Name: "first"}
	row2 := &Person{Name: "2nd"}

	c.Assert(s.db.Save(&row1), IsNil)
	c.Assert(s.tx.Save(&row2), IsNil)

	c.Assert(s.tx.Find(&compare), IsNil)
	c.Assert(compare, HasLen, 2)

	c.Assert(s.db.Find(&compare), IsNil)
	c.Assert(compare, HasLen, 1)
}

func (s *transactionSuite) TestDelete(c *C) {
	insert := &Person{Name: "first"}
	var stub *Person
	c.Assert(s.db.Save(&insert), IsNil)
	c.Assert(s.tx.Find(&stub, insert.Id), IsNil) //find in transaction
	c.Assert(s.tx.Delete(insert), IsNil)
	c.Assert(s.tx.Find(&stub, insert.Id), Equals, sql.ErrNoRows) //find in transaction
	c.Assert(s.db.Find(&stub, insert.Id), IsNil)                 //main
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
