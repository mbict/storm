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
	Id       int
	PersonId int
	Number   int
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

	s.db.DB().Exec("CREATE TABLE `person` (`id` INTEGER PRIMARY KEY, `name` TEXT, `address_id` INTEGER, `optional_address_id` INTEGER)")
	s.db.DB().Exec("CREATE TABLE `address` (`id` INTEGER PRIMARY KEY, `line1` TEXT, `line2` TEXT, `country_id` INTEGER)")
	s.db.DB().Exec("CREATE TABLE `country` (`id` INTEGER PRIMARY KEY, `name` TEXT)")
	s.db.DB().Exec("CREATE TABLE `telephone` (`id` INTEGER PRIMARY KEY, `number` TEXT)")

	s.db.DB().Exec("INSERT INTO `person` (`id`, `name`) VALUES (1, 'name')")
	s.db.DB().Exec("INSERT INTO `test_structure` (`id`, `name`) VALUES (2, 'name 2')")
	s.db.DB().Exec("INSERT INTO `test_structure` (`id`, `name`) VALUES (3, 'name 3')")
	s.db.DB().Exec("INSERT INTO `test_structure` (`id`, `name`) VALUES (4, 'name 4')")

	s.db.DB().Exec("CREATE TABLE `test_related_structure` (`id` INTEGER PRIMARY KEY, test_structure_id INTEGER, `name` TEXT)")
	s.db.DB().Exec("INSERT INTO `test_related_structure` (`id`, `test_structure_id`, `name`) VALUES (1, 1, 'name 1')")
	s.db.DB().Exec("INSERT INTO `test_related_structure` (`id`, `test_structure_id`, `name`) VALUES (2, 1, 'name 2')")
	s.db.DB().Exec("INSERT INTO `test_related_structure` (`id`, `test_structure_id`, `name`) VALUES (3, 2, 'name 3')")
	s.db.DB().Exec("INSERT INTO `test_related_structure` (`id`, `test_structure_id`, `name`) VALUES (4, 2, 'name 4')")

}

/**************************************************************************
 * Tests Count
 **************************************************************************/

/**************************************************************************
 * Tests First
 **************************************************************************/

/**************************************************************************
 * Tests Find (single)
 **************************************************************************/

/**************************************************************************
 * Tests Find (slice)
 **************************************************************************/

/**************************************************************************
 * Tests generateSelectSQL (helper)
 **************************************************************************/

func (s *query1Suite) TestQueryGenerateSelectSQL(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*Person)(nil)).Elem())
	sql, bind, err := s.db.Query().
		generateSelectSQL(tbl)

	c.Assert(err, IsNil)
	c.Assert(bind, HasLen, 0)
	c.Assert(sql, Equals, "SELECT `person`.`id`, `person`.`name`, `person`.`address_id`, `person`.`optional_address_id` FROM `person`")
}

//select, order by,where, limit and offset syntax check
func (s *query1Suite) Test_GenerateSelectSQL_Where(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*Person)(nil)).Elem())
	sql, bind, err := s.db.Query().
		Order("id", DESC).
		Limit(123).
		Offset(112).
		Where("id = ?", 1).
		generateSelectSQL(tbl)

	c.Assert(err, IsNil)
	c.Assert(bind, HasLen, 1)
	c.Assert(sql, Equals, "SELECT `person`.`id`, `person`.`name`, `person`.`address_id`, `person`.`optional_address_id` FROM `person` "+
		"WHERE `person`.`id` = ? "+
		"ORDER BY `person`.`id` DESC LIMIT 123 OFFSET 112")
}

//simple 1 level
func (s *query1Suite) Test_GenerateSelectSQL_WhereAutoJoin(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*Person)(nil)).Elem())
	sql, bind, err := s.db.Query().
		Where("optional_address.line1 = ?", 2).
		generateSelectSQL(tbl)

	c.Assert(err, IsNil)
	c.Assert(bind, HasLen, 1)
	c.Assert(sql, Equals, "SELECT `person`.`id`, `person`.`name`, `person`.`address_id`, `person`.`optional_address_id` FROM `person` "+
		"JOIN address AS person_optional_address ON person.optional_address_id = person_optional_address.id "+
		"WHERE `person_optional_address`.`line1` = ?")
}

//join 2 levels deep
func (s *query1Suite) Test_GenerateSelectSQL_WhereAutoJoinDeep(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*Person)(nil)).Elem())
	sql, bind, err := s.db.Query().
		Where("OptionalAddress.Country.id = ?", 1).
		generateSelectSQL(tbl)

	c.Assert(err, IsNil)
	c.Assert(bind, HasLen, 1)
	c.Assert(sql, Equals, "SELECT `person`.`id`, `person`.`name`, `person`.`address_id`, `person`.`optional_address_id` FROM `person` "+
		"JOIN address AS person_optional_address ON person.optional_address_id = person_optional_address.id "+
		"JOIN country AS person_optional_address_country ON person_optional_address.country_id = person_optional_address_country.id "+
		"WHERE `person_optional_address_country`.`id` = ?")
}

//auto join trough order by
func (s *query1Suite) Test_GenerateSelectSQL_WhereAutoJoinOrderBy(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*Person)(nil)).Elem())
	sql, bind, err := s.db.Query().
		Order("optional_address.line1", ASC).
		generateSelectSQL(tbl)

	c.Assert(err, IsNil)
	c.Assert(bind, HasLen, 0)
	c.Assert(sql, Equals, "SELECT `person`.`id`, `person`.`name`, `person`.`address_id`, `person`.`optional_address_id` FROM `person` "+
		"JOIN address AS person_optional_address ON person.optional_address_id = person_optional_address.id "+
		"ORDER BY `person_optional_address`.`line1` ASC")
}

//joining multiple tables (test no duplicate joins)
func (s *query1Suite) Test_GenerateSelectSQL_WhereAutoJoinComplex(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*Person)(nil)).Elem())
	sql, bind, err := s.db.Query().
		Where("id = ?", 1).
		Where("person.name = ?", "test").
		Where("Address.Country.id = ?", 2).
		Where("optional_address.line1 = ?", 2).
		Where("OptionalAddress.Country.id = ?", 1).
		Where("OptionalAddress.Country.id = ?", 2).
		Where("Address.line1 = ?", 2).
		generateSelectSQL(tbl)

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

//joining with a many to one table
func (s *query1Suite) Test_GenerateSelectSQL_WhereAutoJoinMany(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*Person)(nil)).Elem())
	sql, bind, err := s.db.Query().
		Where("telephones.number = ?", 1).
		Where("Telephones.Id IN (?,?,?)", 1, 2, 3).
		generateSelectSQL(tbl)

	c.Assert(err, IsNil)
	c.Assert(bind, HasLen, 4)
	c.Assert(sql, Equals, "SELECT `person`.`id`, `person`.`name`, `person`.`address_id`, `person`.`optional_address_id` FROM `person` "+
		"JOIN telephone AS person_telephones ON person.id = person_telephones.person_id "+
		"WHERE `person_telephones`.`number` = ? AND `person_telephones`.`id` IN (?,?,?) "+
		"GROUP BY `person`.`id`")
}

//auto join to parent record (tries to find a related structure)
func (s *query1Suite) Test_GenerateSelectSQL_WhereAutoJoinReverseToParent(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*Country)(nil)).Elem())
	sql, bind, err := s.db.Query().
		Where("id = ?", 1).
		Where("country.name = ?", "test").
		Where("Address.line1 = ?", 2).
		generateSelectSQL(tbl)

	c.Assert(err, IsNil)
	c.Assert(bind, HasLen, 3)
	c.Assert(sql, Equals, "SELECT `country`.`id`, `country`.`name` FROM `country` "+
		"JOIN address AS country_address_country ON country.id = country_address_country.country_id "+
		"WHERE `country`.`id` = ? AND `country`.`name` = ? AND `country_address_country`.`line1` = ? "+
		"GROUP BY `country`.`id`")
}

//auto join to parent record (tries to find a related structure) will only bind on the first occurnce
//in this case it will only bind on Address and not on OptionalAddress
func (s *query1Suite) Test_GenerateSelectSQL_WhereAutoJoinReverseToParentFirstOccurence(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*Address)(nil)).Elem())
	sql, bind, err := s.db.Query().
		Where("id = ?", 1).
		Where("person.name = ?", "piet").
		generateSelectSQL(tbl)

	c.Assert(err, IsNil)
	c.Assert(bind, HasLen, 2)
	c.Assert(sql, Equals, "SELECT `address`.`id`, `address`.`line1`, `address`.`line2`, `address`.`country_id` FROM `address` "+
		"JOIN person AS address_person_address ON address.id = address_person_address.address_id "+
		"WHERE `address`.`id` = ? AND `address_person_address`.`name` = ? "+
		"GROUP BY `address`.`id`")
}

//parent hinting support if multiple columns of the same type exists in the parent
func (s *query1Suite) Test_GenerateSelectSQL_WhereAutoJoinReverseToParentHint(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*Address)(nil)).Elem())
	sql, bind, err := s.db.Query().
		Where("id = ?", 1).
		Where("person[optional_address].name = ?", "piet").
		generateSelectSQL(tbl)

	c.Assert(err, IsNil)
	c.Assert(bind, HasLen, 2)
	c.Assert(sql, Equals, "SELECT `address`.`id`, `address`.`line1`, `address`.`line2`, `address`.`country_id` FROM `address` "+
		"JOIN person AS address_person_optional_address ON address.id = address_person_optional_address.optional_address_id "+
		"WHERE `address`.`id` = ? AND `address_person_optional_address`.`name` = ? "+
		"GROUP BY `address`.`id`")
}

func (s *query1Suite) Test_GenerateSelectSQL_WhereAutoJoinErrorTableResolve(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*Person)(nil)).Elem())
	_, _, err := s.db.Query().
		Where("OptionalAddress.UnknownTable.id = ?", 1).
		generateSelectSQL(tbl)

	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, "Cannot resolve table `UnknownTable` in statement `OptionalAddress.UnknownTable.id`")
}

func (s *query1Suite) Test_GenerateSelectSQL_WhereAutoJoinErrorColumnResolve(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*Person)(nil)).Elem())
	_, _, err := s.db.Query().
		Where("OptionalAddress.notexistingcolumn = ?", 1).
		generateSelectSQL(tbl)

	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, "Cannot find column `notexistingcolumn` found in table `address` used in statement `OptionalAddress.notexistingcolumn`")
}

/**************************************************************************
 * Tests generateCountSQL (helper)
 **************************************************************************/
func (s *query1Suite) TestQueryGenerateCountSQL(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*Person)(nil)).Elem())
	sql, bind, err := s.db.Query().
		generateCountSQL(tbl)

	c.Assert(err, IsNil)
	c.Assert(bind, HasLen, 0)
	c.Assert(sql, Equals, "SELECT COUNT(*) FROM `person`")
}

//select, order by,where, limit and offset syntax check
func (s *query1Suite) Test_GenerateCountSQL_Where(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*Person)(nil)).Elem())
	sql, bind, err := s.db.Query().
		Order("id", DESC).
		Limit(123).
		Offset(112).
		Where("id = ?", 1).
		generateCountSQL(tbl)

	c.Assert(err, IsNil)
	c.Assert(bind, HasLen, 1)
	c.Assert(sql, Equals, "SELECT COUNT(*) FROM `person` WHERE `person`.`id` = ?")
}

//simple 1 level
func (s *query1Suite) Test_GenerateCountSQL_WhereAutoJoin(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*Person)(nil)).Elem())
	sql, bind, err := s.db.Query().
		Where("optional_address.line1 = ?", 2).
		generateCountSQL(tbl)

	c.Assert(err, IsNil)
	c.Assert(bind, HasLen, 1)
	c.Assert(sql, Equals, "SELECT COUNT(*) FROM `person` "+
		"JOIN address AS person_optional_address ON person.optional_address_id = person_optional_address.id "+
		"WHERE `person_optional_address`.`line1` = ?")
}

//join 2 levels deep
func (s *query1Suite) Test_GenerateCountSQL_WhereAutoJoinDeep(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*Person)(nil)).Elem())
	sql, bind, err := s.db.Query().
		Where("OptionalAddress.Country.id = ?", 1).
		generateCountSQL(tbl)

	c.Assert(err, IsNil)
	c.Assert(bind, HasLen, 1)
	c.Assert(sql, Equals, "SELECT COUNT(*) FROM `person` "+
		"JOIN address AS person_optional_address ON person.optional_address_id = person_optional_address.id "+
		"JOIN country AS person_optional_address_country ON person_optional_address.country_id = person_optional_address_country.id "+
		"WHERE `person_optional_address_country`.`id` = ?")
}

//auto join trough order by, but no order by stement
func (s *query1Suite) Test_GenerateCountSQL_WhereAutoJoinOrderBy(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*Person)(nil)).Elem())
	sql, bind, err := s.db.Query().
		Order("optional_address.line1", ASC).
		generateCountSQL(tbl)

	c.Assert(err, IsNil)
	c.Assert(bind, HasLen, 0)
	c.Assert(sql, Equals, "SELECT COUNT(*) FROM `person` "+
		"JOIN address AS person_optional_address ON person.optional_address_id = person_optional_address.id")
}

//joining multiple tables (test no duplicate joins)
func (s *query1Suite) Test_GenerateCountSQL_WhereAutoJoinComplex(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*Person)(nil)).Elem())
	sql, bind, err := s.db.Query().
		Where("id = ?", 1).
		Where("person.name = ?", "test").
		Where("Address.Country.id = ?", 2).
		Where("optional_address.line1 = ?", 2).
		Where("OptionalAddress.Country.id = ?", 1).
		Where("OptionalAddress.Country.id = ?", 2).
		Where("Address.line1 = ?", 2).
		generateCountSQL(tbl)

	c.Assert(err, IsNil)
	c.Assert(bind, HasLen, 7)
	c.Assert(sql, Equals, "SELECT COUNT(*) FROM `person` "+
		"JOIN address AS person_address ON person.address_id = person_address.id "+
		"JOIN country AS person_address_country ON person_address.country_id = person_address_country.id "+
		"JOIN address AS person_optional_address ON person.optional_address_id = person_optional_address.id "+
		"JOIN country AS person_optional_address_country ON person_optional_address.country_id = person_optional_address_country.id "+
		"WHERE `person`.`id` = ? AND `person`.`name` = ? AND `person_address_country`.`id` = ? AND `person_optional_address`.`line1` = ? AND "+
		"`person_optional_address_country`.`id` = ? AND `person_optional_address_country`.`id` = ? AND `person_address`.`line1` = ?")
}

//joining with a many to one table
func (s *query1Suite) Test_GenerateCountSQL_WhereAutoJoinMany(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*Person)(nil)).Elem())
	sql, bind, err := s.db.Query().
		Where("telephones.number = ?", 1).
		Where("Telephones.Id IN (?,?,?)", 1, 2, 3).
		generateCountSQL(tbl)

	c.Assert(err, IsNil)
	c.Assert(bind, HasLen, 4)
	c.Assert(sql, Equals, "SELECT COUNT(*) FROM `person` "+
		"JOIN telephone AS person_telephones ON person.id = person_telephones.person_id "+
		"WHERE `person_telephones`.`number` = ? AND `person_telephones`.`id` IN (?,?,?) "+
		"GROUP BY `person`.`id`")
}

//auto join to parent record (tries to find a related structure)
func (s *query1Suite) Test_GenerateCountSQL_WhereAutoJoinReverseToParent(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*Country)(nil)).Elem())
	sql, bind, err := s.db.Query().
		Where("id = ?", 1).
		Where("country.name = ?", "test").
		Where("Address.line1 = ?", 2).
		generateCountSQL(tbl)

	c.Assert(err, IsNil)
	c.Assert(bind, HasLen, 3)
	c.Assert(sql, Equals, "SELECT COUNT(*) FROM `country` "+
		"JOIN address AS country_address_country ON country.id = country_address_country.country_id "+
		"WHERE `country`.`id` = ? AND `country`.`name` = ? AND `country_address_country`.`line1` = ? "+
		"GROUP BY `country`.`id`")
}

//auto join to parent record (tries to find a related structure) willl only bind on the first occurnce
//in this case it will only bind on Address and not on OptionalAddress
func (s *query1Suite) Test_GenerateCountSQL_WhereAutoJoinReverseToParentFirstOccurence(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*Address)(nil)).Elem())
	sql, bind, err := s.db.Query().
		Where("id = ?", 1).
		Where("person.name = ?", "piet").
		generateCountSQL(tbl)

	c.Assert(err, IsNil)
	c.Assert(bind, HasLen, 2)
	c.Assert(sql, Equals, "SELECT COUNT(*) FROM `address` "+
		"JOIN person AS address_person_address ON address.id = address_person_address.address_id "+
		"WHERE `address`.`id` = ? AND `address_person_address`.`name` = ? "+
		"GROUP BY `address`.`id`")
}

func (s *query1Suite) Test_GenerateCountSQL_WhereAutoJoinReverseToParentHint(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*Address)(nil)).Elem())
	sql, bind, err := s.db.Query().
		Where("id = ?", 1).
		Where("person[optional_address].name = ?", "piet").
		generateCountSQL(tbl)

	c.Assert(err, IsNil)
	c.Assert(bind, HasLen, 2)
	c.Assert(sql, Equals, "SELECT COUNT(*) FROM `address` "+
		"JOIN person AS address_person_optional_address ON address.id = address_person_optional_address.optional_address_id "+
		"WHERE `address`.`id` = ? AND `address_person_optional_address`.`name` = ? "+
		"GROUP BY `address`.`id`")
}

func (s *query1Suite) Test_GenerateCountSQL_WhereAutoJoinErrorTableResolve(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*Person)(nil)).Elem())
	_, _, err := s.db.Query().
		Where("OptionalAddress.UnknownTable.id = ?", 1).
		generateCountSQL(tbl)

	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, "Cannot resolve table `UnknownTable` in statement `OptionalAddress.UnknownTable.id`")
}

func (s *query1Suite) Test_GenerateCountSQL_WhereAutoJoinErrorColumnResolve(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*Person)(nil)).Elem())
	_, _, err := s.db.Query().
		Where("OptionalAddress.notexistingcolumn = ?", 1).
		generateCountSQL(tbl)

	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, "Cannot find column `notexistingcolumn` found in table `address` used in statement `OptionalAddress.notexistingcolumn`")
}

/**************************************************************************
 * Tests formatAndResolveStatement (helper)
 **************************************************************************/
func (s *query1Suite) TestFormatAndResolveStatement(c *C) {
	personTbl, _ := s.db.tableByName("person")
	//addressTbl, _ := s.db.tableByName("address")

	//no table prefix
	statement, joins, tables, err := s.db.Query().formatAndResolveStatement(personTbl, "id = ?")
	c.Assert(err, IsNil)
	c.Assert(statement, HasLen, 1)
	c.Assert(statement[0], Equals, "`person`.`id` = ?")
	c.Assert(joins, Equals, "")
	c.Assert(tables, HasLen, 0)

	//hardcoded string condition, integer and float condition, glued statements
	statement, joins, tables, err = s.db.Query().formatAndResolveStatement(personTbl, "id = 'id' AND id = 123 AND id = 12.34 AND id=Id AND 1=id AND id=1")
	c.Assert(err, IsNil)
	c.Assert(statement, HasLen, 1)
	c.Assert(statement[0], Equals, "`person`.`id` = 'id' AND `person`.`id` = 123 AND `person`.`id` = 12.34 AND `person`.`id`=`person`.`id` AND 1=`person`.`id` AND `person`.`id`=1")
	c.Assert(joins, Equals, "")
	c.Assert(tables, HasLen, 0)

	//table prefix
	statement, joins, tables, err = s.db.Query().formatAndResolveStatement(personTbl, "Person.id = ?")
	c.Assert(err, IsNil)
	c.Assert(statement, HasLen, 1)
	c.Assert(statement[0], Equals, "`person`.`id` = ?")
	c.Assert(joins, Equals, "")
	c.Assert(tables, HasLen, 0)

	//with auto join
	statement, joins, tables, err = s.db.Query().formatAndResolveStatement(personTbl, "address_id = address.id")
	c.Assert(err, IsNil)
	c.Assert(statement, HasLen, 1)
	c.Assert(statement[0], Equals, "`person`.`address_id` = `person_address`.`id`")
	c.Assert(joins, Equals, " JOIN address AS person_address ON person.address_id = person_address.id")
	//c.Assert(tables, HasLen, 1)
	//c.Assert(tables[0], Equals, tblRelated)

	//check brackets
	statement, joins, tables, err = s.db.Query().formatAndResolveStatement(personTbl, "(address_id) = (address.id)")
	c.Assert(err, IsNil)
	c.Assert(statement, HasLen, 1)
	c.Assert(statement[0], Equals, "(`person`.`address_id`) = (`person_address`.`id`)")
	c.Assert(joins, Equals, " JOIN address AS person_address ON person.address_id = person_address.id")
	//c.Assert(tables, HasLen, 1)
	//c.Assert(tables[0], Equals, tblRelated)

	//check all in backets
	statement, joins, tables, err = s.db.Query().formatAndResolveStatement(personTbl, "(address.id IN person.optionalAddressId)")
	c.Assert(err, IsNil)
	c.Assert(statement, HasLen, 1)
	c.Assert(statement[0], Equals, "(`person_address`.`id` IN `person`.`optional_address_id`)")
	//c.Assert(tables, HasLen, 1)
	//c.Assert(tables[0], Equals, tblRelated)

	//check brackets functional brackets
	statement, joins, tables, err = s.db.Query().formatAndResolveStatement(personTbl, "MIN(address.id) > 'id' AND MAX( person.Id ) IN (1234, 12.34, id)")
	c.Assert(err, IsNil)
	c.Assert(statement, HasLen, 1)
	c.Assert(statement[0], Equals, "MIN(`person_address`.`id`) > 'id' AND MAX( `person`.`id` ) IN (1234, 12.34, `person`.`id`)")
	c.Assert(joins, Equals, " JOIN address AS person_address ON person.address_id = person_address.id")
	//c.Assert(tables, HasLen, 1)
	//c.Assert(tables[0], Equals, tbl)

	//test multiple return, double join
	statement, joins, tables, err = s.db.Query().formatAndResolveStatement(personTbl, "address.id = 1", "address.country.id = ?")
	c.Assert(err, IsNil)
	c.Assert(statement, HasLen, 2)
	c.Assert(statement[0], Equals, "`person_address`.`id` = 1")
	c.Assert(statement[1], Equals, "`person_address_country`.`id` = ?")
	c.Assert(joins, Equals, " JOIN address AS person_address ON person.address_id = person_address.id JOIN country AS person_address_country ON person_address.country_id = person_address_country.id")
	//c.Assert(tables, HasLen, 1)
	//c.Assert(tables[0], Equals, tbl)

	//test join on multiple
	statement, joins, tables, err = s.db.Query().formatAndResolveStatement(personTbl, "telephones.number = '11223344'")
	c.Assert(err, IsNil)
	c.Assert(statement, HasLen, 1)
	c.Assert(statement[0], Equals, "`person_telephones`.`number` = '11223344'")
	c.Assert(joins, Equals, " JOIN telephone AS person_telephones ON person.id = person_telephones.person_id")
	//c.Assert(tables, HasLen, 1)
	//c.Assert(tables[0], Equals, tbl)

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
