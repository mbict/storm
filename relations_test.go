package storm

import (
	"reflect"

	. "gopkg.in/check.v1"
)

/*** suite setup ***/
type relationSuite struct {
	db   *Storm
	dbTx *Transaction
}

var _ = Suite(&relationSuite{})

func (s *relationSuite) SetUpSuite(c *C) {
	var err error
	s.db, err = Open(`sqlite3`, `:memory:`)
	c.Assert(s.db, NotNil)
	c.Assert(err, IsNil)

	s.db.RegisterStructure((*TestProduct)(nil))
	s.db.RegisterStructure((*TestProductTag)(nil))
	s.db.RegisterStructure((*TestTag)(nil))
	s.db.SetMaxIdleConns(2)
	s.db.SetMaxOpenConns(2)

	//begin transaction
	s.dbTx = s.db.Begin()

	s.dbTx.DB().Exec("CREATE TABLE `test_product` (`id` INTEGER PRIMARY KEY, `name`, `price` REAL, TEXT, `tag_id` INTEGER, `tag_ptr_id` INTEGER)")
	s.dbTx.DB().Exec("CREATE TABLE `test_tag` (`id` INTEGER PRIMARY KEY, `tag` TEXT)")
	s.dbTx.DB().Exec("CREATE TABLE `test_product_tag` (`id` INTEGER PRIMARY KEY, `test_product_id` INTEGER, `tag` TEXT)")
	s.dbTx.DB().Exec("CREATE TABLE `test_product_test_tag` (`test_product_id` INTEGER, `test_tag_id` INTEGER)")

	s.dbTx.DB().Exec("INSERT INTO `test_tag` (`id`, `tag`) VALUES (1, 'tag 1')")
	s.dbTx.DB().Exec("INSERT INTO `test_tag` (`id`, `tag`) VALUES (2, 'tag 2')")
	s.dbTx.DB().Exec("INSERT INTO `test_tag` (`id`, `tag`) VALUES (3, 'tag 3')")

	s.dbTx.DB().Exec("INSERT INTO `test_product_tag` (`id`, `test_product_id`, `tag`) VALUES (1, 1, 'tag 1')")
	s.dbTx.DB().Exec("INSERT INTO `test_product_tag` (`id`, `test_product_id`, `tag`) VALUES (2, 1, 'tag 2')")
	s.dbTx.DB().Exec("INSERT INTO `test_product_tag` (`id`, `test_product_id`, `tag`) VALUES (3, 2, 'tag 3')")

	s.dbTx.DB().Exec("INSERT INTO `test_product` (`id`, `name`, `price`, `tag_id`, `tag_ptr_id`) VALUES (1, 'name', 11.2, 1, 2)")
	s.dbTx.DB().Exec("INSERT INTO `test_product` (`id`, `name`, `price`, `tag_id`, `tag_ptr_id`) VALUES (2, '2nd', 22.3, 2, NULL)")
	s.dbTx.DB().Exec("INSERT INTO `test_product` (`id`, `name`, `price`, `tag_id`, `tag_ptr_id`) VALUES (3, '3th', 0, 0, NULL)")

	s.dbTx.DB().Exec("INSERT INTO `test_product_tag_test_tag` (`test_product_id`, `test_tag_id`) VALUES (1, 1)")
	s.dbTx.DB().Exec("INSERT INTO `test_product_tag_test_tag` (`test_product_id`, `test_tag_id`) VALUES (1, 2)")
	s.dbTx.DB().Exec("INSERT INTO `test_product_tag_test_tag` (`test_product_id`, `test_tag_id`) VALUES (2, 3)")

	//s.db.Log(log.New(os.Stdout, "[storm-relation] ", 0))
}

/*** tests ***/
func (s *relationSuite) TestRegisterStructureResolveRelations(c *C) {
	tbl, _ := s.db.tables[reflect.TypeOf(TestProduct{})]

	//one to one
	c.Assert(tbl.relations, HasLen, 6)
	c.Assert(tbl.relations[0].relTable, IsNil)
	c.Assert(tbl.relations[0].relColumn, NotNil)
	c.Assert(tbl.relations[0].relColumn.columnName, Equals, "tag_id")

	c.Assert(tbl.relations[1].relTable, IsNil)
	c.Assert(tbl.relations[1].relColumn, NotNil)
	c.Assert(tbl.relations[1].relColumn.columnName, Equals, "tag_ptr_id")

	//one to many
	c.Assert(tbl.relations[2].relColumn, NotNil)
	c.Assert(tbl.relations[2].relTable, NotNil)
	c.Assert(tbl.relations[2].relTable.tableName, Equals, "test_product_tag")
	c.Assert(tbl.relations[2].relColumn.columnName, Equals, "test_product_id")

	c.Assert(tbl.relations[3].relColumn, NotNil)
	c.Assert(tbl.relations[3].relTable, NotNil)
	c.Assert(tbl.relations[3].relTable.tableName, Equals, "test_product_tag")
	c.Assert(tbl.relations[3].relColumn.columnName, Equals, "test_product_id")

	//many to many
	/* todo: not implemented yet
	c.Assert(tbl.relations[4].relColumn, IsNil)
	c.Assert(tbl.relations[4].relTable, NotNil)
	c.Assert(tbl.relations[4].relTable.tableName, Equals, "test_product_tag")

	c.Assert(tbl.relations[3].relColumn, NotNil)
	c.Assert(tbl.relations[3].relTable, NotNil)
	c.Assert(tbl.relations[3].relTable.tableName, Equals, "test_product_tag")
	*/
}
