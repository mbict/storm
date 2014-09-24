package storm

import (
	"database/sql"
	"fmt"
	"reflect"

	. "gopkg.in/check.v1"
)

type querySuite struct {
	db *Storm
}

var _ = Suite(&querySuite{})

func (s *querySuite) SetUpSuite(c *C) {

	var err error
	s.db, err = Open(`sqlite3`, `:memory:`)
	c.Assert(s.db, NotNil)
	c.Assert(err, IsNil)

	s.db.RegisterStructure((*testStructure)(nil))
	s.db.RegisterStructure((*testRelatedStructure)(nil))
	s.db.SetMaxIdleConns(10)
	s.db.SetMaxOpenConns(10)

	s.db.DB().Exec("CREATE TABLE `test_structure` (`id` INTEGER PRIMARY KEY, `name` TEXT)")
	s.db.DB().Exec("INSERT INTO `test_structure` (`id`, `name`) VALUES (1, 'name')")
	s.db.DB().Exec("INSERT INTO `test_structure` (`id`, `name`) VALUES (2, 'name 2')")
	s.db.DB().Exec("INSERT INTO `test_structure` (`id`, `name`) VALUES (3, 'name 3')")
	s.db.DB().Exec("INSERT INTO `test_structure` (`id`, `name`) VALUES (4, 'name 4')")

	s.db.DB().Exec("CREATE TABLE `test_related_structure` (`id` INTEGER PRIMARY KEY, test_structure_id INTEGER, `name` TEXT)")
	s.db.DB().Exec("INSERT INTO `test_related_structure` (`id`, `test_structure_id`, `name`) VALUES (1, 1, 'name 1')")
	s.db.DB().Exec("INSERT INTO `test_related_structure` (`id`, `test_structure_id`, `name`) VALUES (2, 1, 'name 2')")
	s.db.DB().Exec("INSERT INTO `test_related_structure` (`id`, `test_structure_id`, `name`) VALUES (3, 2, 'name 3')")
	s.db.DB().Exec("INSERT INTO `test_related_structure` (`id`, `test_structure_id`, `name`) VALUES (4, 2, 'name 4')")
}

/*** tests ***/
func (s *querySuite) TestFirst(c *C) {
	var (
		input    testStructure
		inputPtr *testStructure
	)

	c.Assert(s.db.Query().Where("id = ?", 999).First(&input), Equals, sql.ErrNoRows)    //no result
	c.Assert(s.db.Query().Where("id = ?", 999).First(&inputPtr), Equals, sql.ErrNoRows) //no result ptr

	//find
	c.Assert(s.db.Query().Where("id = ?", 1).First(&input), IsNil)
	c.Assert(input.Id, Equals, 1)
	c.Assert(input.Name, Equals, "name")

	//find by Ptr
	inputPtr = nil
	c.Assert(s.db.Query().Where("id = ?", 1).First(&inputPtr), IsNil)
	c.Assert(inputPtr, NotNil)
	c.Assert(inputPtr.Id, Equals, 1)
	c.Assert(inputPtr.Name, Equals, "name")

	//check if callback OnInit is called
	c.Assert(inputPtr.onInitInvoked, Equals, true)
}

func (s *querySuite) TestFindSingle(c *C) {
	var (
		input    testStructure
		inputPtr *testStructure
	)

	c.Assert(s.db.Query().Where("id = ?", 999).Find(&input), Equals, sql.ErrNoRows)    //empty result, no match
	c.Assert(s.db.Query().Where("id = ?", 999).Find(&inputPtr), Equals, sql.ErrNoRows) //empty result, no match PTR

	q := s.db.Query()

	//find by id inline where
	c.Assert(q.Find(&input, 1), IsNil)
	c.Assert(input.Id, Equals, 1)
	c.Assert(input.Name, Equals, "name")

	//find by id inline where are not added to the current query context when set inline
	c.Assert(q.Find(&input, 2), IsNil)
	c.Assert(input.Id, Equals, 2)
	c.Assert(input.Name, Equals, "name 2")

	//find by id
	c.Assert(s.db.Query().Where("id = ?", 1).Find(&input), IsNil)
	c.Assert(input.Id, Equals, 1)
	c.Assert(input.Name, Equals, "name")

	//find by id Ptr and assign inline where
	c.Assert(s.db.Query().Where("id = ?", 2).Find(&inputPtr), IsNil)
	c.Assert(inputPtr, NotNil)
	c.Assert(inputPtr.Id, Equals, 2)
	c.Assert(inputPtr.Name, Equals, "name 2")

	//check if callback OnInit is called
	c.Assert(inputPtr.onInitInvoked, Equals, true)
}

func (s *querySuite) TestFindSingle_WhereRelParentRecord(c *C) {
	var inputPtr *testRelatedStructure

	//where with string condition
	c.Assert(s.db.Query().Where("test_structure_id = ?", &testStructure{Id: 2, Name: "name 2"}).Find(&inputPtr), IsNil)
	c.Assert(inputPtr, DeepEquals, &testRelatedStructure{Id: 3, TestStructureId: 2, Name: "name 3"})

	//inline where find
	inputPtr = nil
	c.Assert(s.db.Query().Find(&inputPtr, &testStructure{Id: 2, Name: "name 2"}), IsNil)
	c.Assert(inputPtr, DeepEquals, &testRelatedStructure{Id: 3, TestStructureId: 2, Name: "name 3"})
}

func (s *querySuite) TestFirstFindSlice(c *C) {
	var (
		inputPtr []*testStructure
		input    []testStructure
	)
	//empty result, no match PTR
	inputPtr = nil
	c.Assert(s.db.Query().Where("id > ?", 999).Find(&inputPtr), Equals, sql.ErrNoRows)
	c.Assert(inputPtr, IsNil)

	//empty result, no match
	c.Assert(s.db.Query().Where("id > ?", 999).Find(&input), Equals, sql.ErrNoRows)
	c.Assert(input, IsNil)

	//find by id PTR
	c.Assert(s.db.Query().Where("id > ?", 1).Find(&inputPtr), IsNil)
	c.Assert(inputPtr, HasLen, 3)

	//find by id PTR and where statement inline
	q := s.db.Query()
	c.Assert(q.Find(&inputPtr, 1), IsNil)
	c.Assert(inputPtr, HasLen, 1)
	c.Assert(inputPtr[0].Id, Equals, 1)
	c.Assert(inputPtr[0].Name, Equals, "name")

	//find by inline statmement previous inline should not be added to current query context
	c.Assert(q.Find(&inputPtr, 2), IsNil)
	c.Assert(inputPtr, HasLen, 1)
	c.Assert(inputPtr[0], DeepEquals, &testStructure{Id: 2, Name: "name 2"})

	//check if slice count is reset, and not appended (bug)
	inputPtr = []*testStructure{&testStructure{}}
	c.Assert(s.db.Query().Where("id > ?", 1).Find(&inputPtr), IsNil)
	c.Assert(inputPtr, HasLen, 3)

	//find by id and where statement inline
	input = nil
	c.Assert(s.db.Query().Find(&input, 1), IsNil)
	c.Assert(input, HasLen, 1)

	//find by id
	input = nil
	c.Assert(s.db.Query().Where("id > ?", 1).Find(&input), IsNil)
	c.Assert(input, HasLen, 3)

	//check if callback OnInit is called
	c.Assert(input[0].onInitInvoked, Equals, true)
	c.Assert(input[1].onInitInvoked, Equals, true)
	c.Assert(input[2].onInitInvoked, Equals, true)

	//check if slice count is reset, and not appended (bug)
	input = []testStructure{testStructure{}}
	c.Assert(s.db.Query().Where("id > ?", 1).Find(&input), IsNil)
	c.Assert(input, HasLen, 3)

	//BUG: make sure if we recycle a pointer its initialized to zero
	c.Assert(s.db.Find(&input, `id = ?`, 999), IsNil)
	c.Assert(input, HasLen, 0)

}

//where and inline find with related objec (auto id inject)
func (s *querySuite) TestFindSlice_WhereRelParentRecord(c *C) {
	var inputPtr []*testRelatedStructure

	c.Assert(s.db.Query().Where("test_structure_id = ?", &testStructure{Id: 2, Name: "name 2"}).Find(&inputPtr), IsNil)
	c.Assert(inputPtr, HasLen, 2)
	c.Assert(inputPtr[0], DeepEquals, &testRelatedStructure{Id: 3, TestStructureId: 2, Name: "name 3"})
	c.Assert(inputPtr[1], DeepEquals, &testRelatedStructure{Id: 4, TestStructureId: 2, Name: "name 4"})

	//inline where find
	inputPtr = nil
	c.Assert(s.db.Query().Find(&inputPtr, &testStructure{Id: 2, Name: "name 2"}), IsNil)
	c.Assert(inputPtr, HasLen, 2)
}

func (s *querySuite) TestCount(c *C) {
	//2 results
	cnt, err := s.db.Query().Where("id > ?", 2).Count((*testStructure)(nil))
	c.Assert(err, IsNil)
	c.Assert(cnt, Equals, int64(2))

	//no results
	cnt, err = s.db.Query().Where("id > ?", 999).Count((*testStructure)(nil))
	c.Assert(err, IsNil)
	c.Assert(cnt, Equals, int64(0))
}

//helper tests
func (s *querySuite) TestGenerateSelect(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*testStructure)(nil)).Elem())

	sql, bind := s.db.Query().generateSelectSQL(tbl)
	fmt.Println(sql)
	c.Assert(bind, HasLen, 0)
	c.Assert(sql, Equals, "SELECT `test_structure`.`id`, `test_structure`.`name` FROM `test_structure`")

	//where/limit/offset/order/order test
	sql, bind = s.db.Query().Where("id = ?", 1).
		Where("name = ?", "test").
		Limit(10).
		Offset(5).
		Order("id", ASC).
		Order("name", DESC).generateSelectSQL(tbl)
	c.Assert(bind, HasLen, 2)
	c.Assert(sql, Equals, "SELECT `test_structure`.`id`, `test_structure`.`name` FROM `test_structure` WHERE `test_structure`.`id` = ? AND `test_structure`.`name` = ? ORDER BY `test_structure`.`id` ASC, `test_structure`.`name` DESC LIMIT 10 OFFSET 5")
}

func (s *querySuite) TestGenerateSelect_AutoJoin(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*testRelatedStructure)(nil)).Elem())

	//where/limit/offset/order/order test
	sql, bind := s.db.Query().Where("id = ?", 1).
		Where("name = ?", "test").
		Where("testStructure.name = ?", "test").
		Order("id", ASC).
		Order("name", DESC).generateSelectSQL(tbl)
	c.Assert(bind, HasLen, 3)
	c.Assert(sql, Equals, "SELECT `test_related_structure`.`id`, `test_related_structure`.`test_structure_id`, `test_related_structure`.`name` FROM `test_related_structure` JOIN `test_structure` ON `test_structure`.`id` = `test_related_structure`.`test_structure_id` WHERE `test_related_structure`.id = ? AND `test_related_structure`.name = ? AND `test_structure`.name = ? ORDER BY `test_related_structure`.`id` ASC, `test_related_structure`.`name` DESC LIMIT 10 OFFSET 5")
}

func (s *querySuite) TestGenerateCount(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*testStructure)(nil)).Elem())

	sql, bind := s.db.Query().generateCountSQL(tbl)
	c.Assert(bind, HasLen, 0)
	c.Assert(sql, Equals, "SELECT COUNT(`test_structure`.*) FROM `test_structure`")

	//where/limit/offset/order/order test
	sql, bind = s.db.Query().Where("id = ?", 1).
		Where("name = ?", "test").
		Limit(10).
		Offset(5).
		Order("id", ASC).
		Order("name", DESC).generateCountSQL(tbl)
	c.Assert(bind, HasLen, 2)
	c.Assert(sql, Equals, "SELECT COUNT(`test_structure`.*) FROM `test_structure` WHERE `test_structure`.`id` = ? AND `test_structure`.`name` = ?")
}

func (s *querySuite) TestFormatAndResolveStatement(c *C) {
	tbl, _ := s.db.tableByName("test_structure")
	tblRelated, _ := s.db.tableByName("test_related_structure")

	statement, tables := s.db.Query().formatAndResolveStatement("a = ?", tbl)
	c.Assert(statement, Equals, "a = ?")
	c.Assert(tables, HasLen, 0)

	statement, tables = s.db.Query().formatAndResolveStatement("id = ?", tbl)
	c.Assert(statement, Equals, "`test_structure`.`id` = ?")
	c.Assert(tables, HasLen, 0)

	statement, tables = s.db.Query().formatAndResolveStatement("id = testRelatedStructure.testStructureId", tbl)
	c.Assert(statement, Equals, "`test_structure`.`id` = `test_related_structure`.`test_structure_id`")
	c.Assert(tables, HasLen, 1)
	c.Assert(tables[0], Equals, tblRelated)

	statement, tables = s.db.Query().formatAndResolveStatement("(id) = (test_related_structure.test_structure_id)", tbl)
	c.Assert(statement, Equals, "(`test_structure`.`id`) = (`test_related_structure`.`test_structure_id`)")
	c.Assert(tables, HasLen, 1)
	c.Assert(tables[0], Equals, tblRelated)

	statement, tables = s.db.Query().formatAndResolveStatement("(testStructure.id IN testRelatedStructure.testStructureId)", tbl)
	c.Assert(statement, Equals, "(`test_structure`.`id` IN `test_related_structure`.`test_structure_id`)")
	c.Assert(tables, HasLen, 1)
	c.Assert(tables[0], Equals, tblRelated)

	statement, tables = s.db.Query().formatAndResolveStatement("MIN(testStructure.id) > 'id' AND MAX( testRelatedStructure.testStructureId ) IN 1234", tblRelated)
	c.Assert(statement, Equals, "MIN(`test_structure`.`id`) > 'id' AND MAX( `test_related_structure`.`test_structure_id` ) IN 1234")
	c.Assert(tables, HasLen, 1)
	c.Assert(tables[0], Equals, tbl)
}
