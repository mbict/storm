package storm

import (
	"database/sql"
	"reflect"

	_ "github.com/mattn/go-sqlite3"
	. "gopkg.in/check.v1"
)

type Person struct {
	Id                int
	Name              string
	Address           *Address
	AddressId         int
	OptionalAddress   *Address
	OptionalAddressId sql.NullInt64
	Telephones        []*Telephone
}

type Address struct {
	Id        int
	Line1     string
	Line2     string
	Country   *Country
	CountryId int
}

type Country struct {
	Id   int
	Name string
}

type Telephone struct {
	Id     int
	Number int
}

type query1Suite struct {
	db *Storm
}

var _ = Suite(&query1Suite{})

func (s *query1Suite) SetUpSuite(c *C) {

	var err error
	s.db, err = Open(`sqlite3`, `:memory:`)
	c.Assert(s.db, NotNil)
	c.Assert(err, IsNil)

	s.db.RegisterStructure((*Person)(nil))
	s.db.RegisterStructure((*Address)(nil))
	s.db.RegisterStructure((*Country)(nil))
	s.db.RegisterStructure((*Telephone)(nil))
	s.db.SetMaxIdleConns(10)
	s.db.SetMaxOpenConns(10)

	/*
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
	*/
}

/*** tests ***/
func (s *query1Suite) TestQuery(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*Person)(nil)).Elem())
	sql, bind, err := s.db.Query().
		generateSelectSQL2(tbl)

	c.Assert(err, IsNil)
	c.Assert(bind, HasLen, 0)
	c.Assert(sql, Equals, "SELECT `person`.`id`, `person`.`name`, `person`.`address_id`, `person`.`optional_address_id` FROM `person`")
}

func (s *query1Suite) TestQueryWhere(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*Person)(nil)).Elem())
	sql, bind, err := s.db.Query().
		Where("id = ?", 1).
		generateSelectSQL2(tbl)

	c.Assert(err, IsNil)
	c.Assert(bind, HasLen, 1)
	c.Assert(sql, Equals, "SELECT `person`.`id`, `person`.`name`, `person`.`address_id`, `person`.`optional_address_id` FROM `person` WHERE `person`.`id` = ?")
}

//simple 1 level
func (s *query1Suite) TestWhereAutoJoin(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*Person)(nil)).Elem())
	sql, bind, err := s.db.Query().
		Where("optional_address.line1 = ?", 2).
		generateSelectSQL2(tbl)

	c.Assert(err, IsNil)
	c.Assert(bind, HasLen, 1)
	c.Assert(sql, Equals, "SELECT `person`.`id`, `person`.`name`, `person`.`address_id`, `person`.`optional_address_id` FROM `person` "+
		"JOIN address AS person_optional_address ON person.optional_address_id = person_optional_address.id "+
		"WHERE `person_optional_address`.`line1` = ?")
}

//join 2 levels deep
func (s *query1Suite) TestWhereAutoJoinDeep(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*Person)(nil)).Elem())
	sql, bind, err := s.db.Query().
		Where("OptionalAddress.Country.id = ?", 1).
		generateSelectSQL2(tbl)

	c.Assert(err, IsNil)
	c.Assert(bind, HasLen, 1)
	c.Assert(sql, Equals, "SELECT `person`.`id`, `person`.`name`, `person`.`address_id`, `person`.`optional_address_id` FROM `person` "+
		"JOIN address AS person_optional_address ON person.optional_address_id = person_optional_address.id "+
		"JOIN country AS person_optional_address_country ON person_optional_address.country_id = person_optional_address_country.id "+
		"WHERE `person_optional_address_country`.`id` = ?")
}

//joining multiple tables no duplicates
func (s *query1Suite) TestWhereAutoJoinComplex(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*Person)(nil)).Elem())
	sql, bind, err := s.db.Query().
		Where("id = ?", 1).
		Where("person.name = ?", "test").
		Where("Address.Country.id = ?", 2).
		Where("optional_address.line1 = ?", 2).
		Where("OptionalAddress.Country.id = ?", 1).
		Where("OptionalAddress.Country.id = ?", 2).
		Where("Address.line1 = ?", 2).
		generateSelectSQL2(tbl)

	c.Assert(err, IsNil)
	c.Assert(bind, HasLen, 7)
	c.Assert(sql, Equals, "SELECT `person`.`id`, `person`.`name`, `person`.`address_id`, `person`.`optional_address_id` FROM `person` "+
		"JOIN address AS person_address ON person.address_id = person_address.id "+
		"JOIN country AS person_address_country ON person_address.country_id = person_address_country.id "+
		"JOIN address AS person_optional_address ON person.optional_address_id = person_optional_address.id "+
		"JOIN country AS person_optional_address_country ON person_optional_address.country_id = person_optional_address_country.id "+
		"WHERE `person`.`id` = ? AND `person`.`name` = ? AND `person_address_country`.`id` = ? AND `person_optional_address`.`line1` = ? AND "+
		"`person_optional_address_country`.`id` = ? AND `person_optional_address_country`.`id` = ? AND `person_address`.`line1` = ?")
}

//auto join to parent record (tries to find a related structure)
func (s *query1Suite) TestWhereAutoJoinReverseToParent(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*Country)(nil)).Elem())
	sql, bind, err := s.db.Query().
		Where("id = ?", 1).
		Where("country.name = ?", "test").
		Where("Address.line1 = ?", 2).
		generateSelectSQL2(tbl)

	c.Assert(err, IsNil)
	c.Assert(bind, HasLen, 3)
	c.Assert(sql, Equals, "SELECT `country`.`id`, `country`.`name` FROM `country` "+
		"JOIN address AS country_address_country ON country.id = country_address_country.country_id "+
		"WHERE `country`.`id` = ? AND `country`.`name` = ? AND `country_address_country`.`line1` = ? "+
		"GROUP BY `country`.`id`")
}

//auto join to parent record (tries to find a related structure) willl only bind on the first occurnce
//in this case it will only bind on Address and not on OptionalAddress
func (s *query1Suite) TestWhereAutoJoinReverseToParentFirstOccurence(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*Address)(nil)).Elem())
	sql, bind, err := s.db.Query().
		Where("id = ?", 1).
		Where("person.name = ?", "piet").
		generateSelectSQL2(tbl)

	c.Assert(err, IsNil)
	c.Assert(bind, HasLen, 2)
	c.Assert(sql, Equals, "SELECT `address`.`id`, `address`.`line1`, `address`.`line2`, `address`.`country_id` FROM `address` "+
		"JOIN person AS address_person_address ON address.id = address_person_address.address_id "+
		"WHERE `address`.`id` = ? AND `address_person_address`.`name` = ? "+
		"GROUP BY `address`.`id`")
}

func (s *query1Suite) TestWhereAutoJoinReverseToParentHint(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*Address)(nil)).Elem())
	sql, bind, err := s.db.Query().
		Where("id = ?", 1).
		Where("person[optional_address].name = ?", "piet").
		generateSelectSQL2(tbl)

	c.Assert(err, IsNil)
	c.Assert(bind, HasLen, 2)
	c.Assert(sql, Equals, "SELECT `address`.`id`, `address`.`line1`, `address`.`line2`, `address`.`country_id` FROM `address` "+
		"JOIN person AS address_person_optional_address ON address.id = address_person_optional_address.optional_address_id "+
		"WHERE `address`.`id` = ? AND `address_person_optional_address`.`name` = ? "+
		"GROUP BY `address`.`id`")
}

func (s *query1Suite) TestWhereAutoJoinErrorTableResolve(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*Person)(nil)).Elem())
	_, _, err := s.db.Query().
		Where("OptionalAddress.UnknownTable.id = ?", 1).
		generateSelectSQL2(tbl)

	c.Assert(err, NotNil)
}

func (s *query1Suite) TestWhereAutoJoinErrorColumnResolve(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*Person)(nil)).Elem())
	_, _, err := s.db.Query().
		Where("OptionalAddress.notexistingcolumn = ?", 1).
		generateSelectSQL2(tbl)

	c.Assert(err, NotNil)
	//c.Assert(err, ErrorMatches, )
}

/*
func (s *query1Suite) TestDepends(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*Person)(nil)).Elem())
	sql, _ := s.db.Query().
		DependentColumns("OptionalAddress", "Telephones", "Address.Country").
		generateSelectSQL2(tbl)

	c.Assert(sql, Equals, "SELECT")
}

func (s *query1Suite) TestDependsWhereJoin(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*Person)(nil)).Elem())
	sql, bind := s.db.Query().
		DependentColumns("OptionalAddress", "Telephones", "Address.Country", "OptionalAddress.Country").
		Where("OptionalAddress.line1 = ?", 2).
		Where("address.line1 = ?", 2).
		generateSelectSQL2(tbl)

	c.Assert(bind, HasLen, 2)
	c.Assert(sql, Equals, "SELECT")
}
*/
