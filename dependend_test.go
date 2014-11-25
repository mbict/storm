package storm

import (
	"database/sql"
	"io/ioutil"
	"os"
	"reflect"
	"strings"

	. "gopkg.in/check.v1"
)

//*** test suite setup ***/
type dependendSuite struct {
	db       *Storm
	tempName string
}

var _ = Suite(&dependendSuite{})

func (s *dependendSuite) SetUpSuite(c *C) {
	//create temporary table (for transactions we need a physical database, sql lite doesnt support memory transactions)
	tmp, err := ioutil.TempFile("", "storm_test.sqlite_")
	c.Assert(err, IsNil)
	tmp.Close()
	s.tempName = tmp.Name()

	s.db, err = Open(`sqlite3`, `file:`+s.tempName+`?mode=rwc`)
	c.Assert(s.db, NotNil)
	c.Assert(err, IsNil)

	s.db.RegisterStructure((*Person)(nil))
	s.db.RegisterStructure((*Address)(nil))
	s.db.RegisterStructure((*Country)(nil))
	s.db.RegisterStructure((*Telephone)(nil))
	s.db.RegisterStructure((*ParentPerson)(nil))

	s.db.SetMaxIdleConns(10)
	s.db.SetMaxOpenConns(10)

	assertExec := func(res sql.Result, err error) {
		c.Assert(err, IsNil)
	}

	//TABLES
	assertExec(s.db.DB().Exec("CREATE TABLE `parent_person` (`id` INTEGER PRIMARY KEY, `person_id` INTEGER)"))
	assertExec(s.db.DB().Exec("CREATE TABLE `person` (`id` INTEGER PRIMARY KEY, `name` TEXT, `address_id` INTEGER, `optional_address_id` INTEGER)"))
	assertExec(s.db.DB().Exec("CREATE TABLE `address` (`id` INTEGER PRIMARY KEY, `line1` TEXT, `line2` TEXT, `country_id` INTEGER)"))
	assertExec(s.db.DB().Exec("CREATE TABLE `country` (`id` INTEGER PRIMARY KEY, `name` TEXT)"))
	assertExec(s.db.DB().Exec("CREATE TABLE `telephone` (`id` INTEGER PRIMARY KEY, `person_id` INTEGER, `number` TEXT)"))

	//TEST DATA
	assertExec(s.db.DB().Exec("INSERT INTO `person` (`id`, `name`, `address_id`, `optional_address_id`) VALUES (1, 'person 1', 1, 2)"))
	assertExec(s.db.DB().Exec("INSERT INTO `person` (`id`, `name`, `address_id`, `optional_address_id`) VALUES (2, 'person 2', 3, 4)"))
	assertExec(s.db.DB().Exec("INSERT INTO `person` (`id`, `name`, `address_id`, `optional_address_id`) VALUES (3, 'person 3', 5, 1)"))
	assertExec(s.db.DB().Exec("INSERT INTO `person` (`id`, `name`, `address_id`, `optional_address_id`) VALUES (4, 'person 4', 2, 2)"))

	assertExec(s.db.DB().Exec("INSERT INTO `parent_person` (`id`, `person_id`) VALUES (1, 2)"))
	assertExec(s.db.DB().Exec("INSERT INTO `parent_person` (`id`, `person_id`) VALUES (2, 4)"))

	assertExec(s.db.DB().Exec("INSERT INTO `address` (`id`, `line1`, `line2`, `country_id`) VALUES (1, 'address 1 line 1', 'address 1 line 2', 1)"))
	assertExec(s.db.DB().Exec("INSERT INTO `address` (`id`, `line1`, `line2`, `country_id`) VALUES (2, 'address 2 line 1', 'address 2 line 2', 2)"))
	assertExec(s.db.DB().Exec("INSERT INTO `address` (`id`, `line1`, `line2`, `country_id`) VALUES (3, 'address 3 line 1', 'address 3 line 2', 3)"))
	assertExec(s.db.DB().Exec("INSERT INTO `address` (`id`, `line1`, `line2`, `country_id`) VALUES (4, 'address 4 line 1', 'address 4 line 2', 4)"))
	assertExec(s.db.DB().Exec("INSERT INTO `address` (`id`, `line1`, `line2`, `country_id`) VALUES (5, 'address 5 line 1', 'address 5 line 2', 1)"))

	assertExec(s.db.DB().Exec("INSERT INTO `country` (`id`, `name`) VALUES (1, 'nl')"))
	assertExec(s.db.DB().Exec("INSERT INTO `country` (`id`, `name`) VALUES (2, 'usa')"))
	assertExec(s.db.DB().Exec("INSERT INTO `country` (`id`, `name`) VALUES (3, 'de')"))
	assertExec(s.db.DB().Exec("INSERT INTO `country` (`id`, `name`) VALUES (4, 'fr')"))

	assertExec(s.db.DB().Exec("INSERT INTO `telephone` (`id`, `person_id`, `number`) VALUES (1, 1, '111-11-1111')"))
	assertExec(s.db.DB().Exec("INSERT INTO `telephone` (`id`, `person_id`, `number`) VALUES (2, 1, '111-22-1111')"))
	assertExec(s.db.DB().Exec("INSERT INTO `telephone` (`id`, `person_id`, `number`) VALUES (3, 1, '111-33-1111')"))
	assertExec(s.db.DB().Exec("INSERT INTO `telephone` (`id`, `person_id`, `number`) VALUES (4, 1, '111-44-1111')"))
	assertExec(s.db.DB().Exec("INSERT INTO `telephone` (`id`, `person_id`, `number`) VALUES (5, 3, '333-11-1111')"))
	assertExec(s.db.DB().Exec("INSERT INTO `telephone` (`id`, `person_id`, `number`) VALUES (6, 4, '444-11-1111')"))
	assertExec(s.db.DB().Exec("INSERT INTO `telephone` (`id`, `person_id`, `number`) VALUES (7, 4, '444-22-1111')"))
}

func (s *dependendSuite) TearDownSuite(c *C) {
	s.db.Close()

	//remove database
	os.Remove(s.tempName)
}

func findRelationByName(tbl *table, name string) *relation {
	for i, _ := range tbl.relations {
		if strings.EqualFold(tbl.relations[i].name, name) {
			return tbl.relations[i]
		}
	}
	return nil
}

/*** tests ***/
//depends
func (s *dependendSuite) TestDependentColumns(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*Person)(nil)).Elem())
	sql, _, remainingDepends, scanObjects, _ := s.db.Query().
		DependentColumns("OptionalAddress", "Telephones", "Address").
		generateSelectSQL(tbl)

	c.Assert(sql, Equals, "SELECT "+
		"`person`.`id`, `person`.`name`, `person`.`address_id`, `person`.`optional_address_id`, "+
		"`person_address`.`id`, `person_address`.`line1`, `person_address`.`line2`, `person_address`.`country_id` "+
		"FROM `person` AS `person` "+
		"JOIN address AS person_address ON person.address_id = person_address.id")

	c.Assert(remainingDepends, HasLen, 2)
	c.Assert(remainingDepends, DeepEquals, []depends{
		depends{index: [][]int{[]int{4}}, dependentColumns: []string{}, rel: findRelationByName(tbl, "optional_address")},
		depends{index: [][]int{[]int{6}}, dependentColumns: []string{}, rel: findRelationByName(tbl, "telephones")},
	})
	c.Assert(scanObjects, HasLen, 1)
}

func (s *dependendSuite) TestDependentColumns_Where(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*Person)(nil)).Elem())
	sql, _, remainingDepends, scanObjects, _ := s.db.Query().
		DependentColumns("OptionalAddress", "Telephones", "Address").
		Where("OptionalAddress.id = ?", 2).
		generateSelectSQL(tbl)

	c.Assert(sql, Equals, "SELECT "+
		"`person`.`id`, `person`.`name`, `person`.`address_id`, `person`.`optional_address_id`, "+
		"`person_optional_address`.`id`, `person_optional_address`.`line1`, `person_optional_address`.`line2`, `person_optional_address`.`country_id`, "+
		"`person_address`.`id`, `person_address`.`line1`, `person_address`.`line2`, `person_address`.`country_id` "+
		"FROM `person` AS `person` "+
		"JOIN address AS person_optional_address ON person.optional_address_id = person_optional_address.id "+
		"JOIN address AS person_address ON person.address_id = person_address.id "+
		"WHERE `person_optional_address`.`id` = ?")

	c.Assert(remainingDepends, HasLen, 1)
	c.Assert(remainingDepends, DeepEquals, []depends{
		depends{index: [][]int{[]int{6}}, dependentColumns: []string{}, rel: findRelationByName(tbl, "telephones")},
	})
	c.Assert(scanObjects, HasLen, 2)
}

func (s *dependendSuite) TestDependentColumns_JoinDeep(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*Person)(nil)).Elem())
	sql, _, remainingDepends, scanObjects, _ := s.db.Query().
		DependentColumns("OptionalAddress.Country", "OptionalAddress", "OptionalAddress.Test.Test", "OptionalAddress.Country.Test", "Telephones", "Address.Country").
		generateSelectSQL(tbl)

	c.Assert(sql, Equals, "SELECT "+
		"`person`.`id`, `person`.`name`, `person`.`address_id`, `person`.`optional_address_id`, "+
		"`person_address`.`id`, `person_address`.`line1`, `person_address`.`line2`, `person_address`.`country_id`, "+
		"`person_address_country`.`id`, `person_address_country`.`name` "+
		"FROM `person` AS `person` "+
		"JOIN address AS person_address ON person.address_id = person_address.id "+
		"JOIN country AS person_address_country ON person_address.country_id = person_address_country.id")

	c.Assert(remainingDepends, HasLen, 2)
	c.Assert(remainingDepends, DeepEquals, []depends{
		depends{index: [][]int{[]int{4}}, dependentColumns: []string{"Country", "Test.Test", "Country.Test"}, rel: findRelationByName(tbl, "optional_address")},
		depends{index: [][]int{[]int{6}}, dependentColumns: []string{}, rel: findRelationByName(tbl, "telephones")},
	})
	c.Assert(scanObjects, HasLen, 2)
}

func (s *dependendSuite) TestDependentColumns_WhereDeep(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*Person)(nil)).Elem())
	sql, _, remainingDepends, scanObjects, _ := s.db.Query().
		DependentColumns("OptionalAddress.Country", "Telephones", "Address.Country").
		Where("OptionalAddress.country.name = ?", "nl").
		generateSelectSQL(tbl)

	c.Assert(sql, Equals, "SELECT "+
		"`person`.`id`, `person`.`name`, `person`.`address_id`, `person`.`optional_address_id`, "+
		"`person_optional_address`.`id`, `person_optional_address`.`line1`, `person_optional_address`.`line2`, `person_optional_address`.`country_id`, "+
		"`person_optional_address_country`.`id`, `person_optional_address_country`.`name`, "+
		"`person_address`.`id`, `person_address`.`line1`, `person_address`.`line2`, `person_address`.`country_id`, "+
		"`person_address_country`.`id`, `person_address_country`.`name` "+
		"FROM `person` AS `person` "+
		"JOIN address AS person_optional_address ON person.optional_address_id = person_optional_address.id "+
		"JOIN country AS person_optional_address_country ON person_optional_address.country_id = person_optional_address_country.id "+
		"JOIN address AS person_address ON person.address_id = person_address.id "+
		"JOIN country AS person_address_country ON person_address.country_id = person_address_country.id "+
		"WHERE `person_optional_address_country`.`name` = ?")

	c.Assert(remainingDepends, HasLen, 1)
	c.Assert(remainingDepends, DeepEquals, []depends{
		depends{index: [][]int{[]int{6}}, dependentColumns: []string{}, rel: findRelationByName(tbl, "telephones")},
	})
	c.Assert(scanObjects, HasLen, 4)
}

func (s *dependendSuite) TestDependentColumns_LevelDeepOptional(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*ParentPerson)(nil)).Elem())
	tblPerson, _ := s.db.table(reflect.TypeOf((*Person)(nil)).Elem())
	sql, _, remainingDepends, scanObjects, _ := s.db.Query().
		DependentColumns("Person.OptionalAddress.Country", "Person.Address.Country").
		generateSelectSQL(tbl)

	c.Assert(sql, Equals, "SELECT "+
		"`parent_person`.`id`, `parent_person`.`person_id`, "+
		"`parent_person_person`.`id`, `parent_person_person`.`name`, `parent_person_person`.`address_id`, `parent_person_person`.`optional_address_id`, "+
		"`parent_person_person_address`.`id`, `parent_person_person_address`.`line1`, `parent_person_person_address`.`line2`, `parent_person_person_address`.`country_id`, "+
		"`parent_person_person_address_country`.`id`, `parent_person_person_address_country`.`name` "+
		"FROM `parent_person` AS `parent_person` "+
		"JOIN person AS parent_person_person ON parent_person.person_id = parent_person_person.id "+
		"JOIN address AS parent_person_person_address ON parent_person_person.address_id = parent_person_person_address.id "+
		"JOIN country AS parent_person_person_address_country ON parent_person_person_address.country_id = parent_person_person_address_country.id")

	c.Assert(remainingDepends, HasLen, 1)
	c.Assert(remainingDepends, DeepEquals, []depends{
		depends{index: [][]int{[]int{1}, []int{4}}, dependentColumns: []string{"Country"}, rel: findRelationByName(tblPerson, "optional_address")},
	})
	c.Assert(scanObjects, HasLen, 3)
}

/*******************************************
 * Find slice
 *******************************************/
func (s *dependendSuite) TestFind_DependentColumns(c *C) {
	var persons []Person
	err := s.db.Query().
		DependentColumns("OptionalAddress", "Telephones", "Address").
		Find(&persons)

	c.Assert(err, IsNil)
	c.Assert(persons, HasLen, 4)

	c.Assert(persons[0].Id, Equals, 1)
	c.Assert(persons[0].Address, NotNil)
	c.Assert(persons[0].Address.Id, Equals, 1)
	c.Assert(persons[0].Address.Country, IsNil)
	c.Assert(persons[0].OptionalAddress, NotNil)
	c.Assert(persons[0].OptionalAddress.Id, Equals, 2)
	c.Assert(persons[0].OptionalAddress.Country, IsNil)
	c.Assert(persons[0].Telephones, HasLen, 4)

	c.Assert(persons[1].Id, Equals, 2)
	c.Assert(persons[1].Address, NotNil)
	c.Assert(persons[1].Address.Id, Equals, 3)
	c.Assert(persons[1].Address.Country, IsNil)
	c.Assert(persons[1].OptionalAddress, NotNil)
	c.Assert(persons[1].OptionalAddress.Id, Equals, 4)
	c.Assert(persons[1].OptionalAddress.Country, IsNil)
	c.Assert(persons[1].Telephones, HasLen, 0)

	c.Assert(persons[2].Id, Equals, 3)
	c.Assert(persons[2].Address, NotNil)
	c.Assert(persons[2].Address.Id, Equals, 5)
	c.Assert(persons[2].Address.Country, IsNil)
	c.Assert(persons[2].OptionalAddress, NotNil)
	c.Assert(persons[2].OptionalAddress.Id, Equals, 1)
	c.Assert(persons[2].OptionalAddress.Country, IsNil)
	c.Assert(persons[2].Telephones, HasLen, 1)

	c.Assert(persons[3].Id, Equals, 4)
	c.Assert(persons[3].Address, NotNil)
	c.Assert(persons[3].Address.Id, Equals, 2)
	c.Assert(persons[3].Address.Country, IsNil)
	c.Assert(persons[3].OptionalAddress, NotNil)
	c.Assert(persons[3].OptionalAddress.Id, Equals, 2)
	c.Assert(persons[3].OptionalAddress.Country, IsNil)
	c.Assert(persons[3].Telephones, HasLen, 2)
}

func (s *dependendSuite) TestFind_DependentColumns_Where(c *C) {
	var persons []Person
	err := s.db.Query().
		DependentColumns("OptionalAddress", "Telephones", "Address").
		Where("OptionalAddress.id = ?", 2).
		Find(&persons)

	c.Assert(err, IsNil)
	c.Assert(persons, HasLen, 2)

	c.Assert(persons[0].Id, Equals, 1)
	c.Assert(persons[0].Address, NotNil)
	c.Assert(persons[0].Address.Id, Equals, 1)
	c.Assert(persons[0].Address.Country, IsNil)
	c.Assert(persons[0].OptionalAddress, NotNil)
	c.Assert(persons[0].OptionalAddress.Id, Equals, 2)
	c.Assert(persons[0].OptionalAddress.Country, IsNil)
	c.Assert(persons[0].Telephones, HasLen, 4)

	c.Assert(persons[1].Id, Equals, 4)
	c.Assert(persons[1].Address, NotNil)
	c.Assert(persons[1].Address.Id, Equals, 2)
	c.Assert(persons[1].Address.Country, IsNil)
	c.Assert(persons[1].OptionalAddress, NotNil)
	c.Assert(persons[1].OptionalAddress.Id, Equals, 2)
	c.Assert(persons[1].OptionalAddress.Country, IsNil)
	c.Assert(persons[1].Telephones, HasLen, 2)
}

func (s *dependendSuite) TestFind_DependentColumns_JoinDeep(c *C) {
	var persons []Person
	err := s.db.Query().
		DependentColumns("OptionalAddress.Country", "OptionalAddress", "OptionalAddress.Test.Test", "OptionalAddress.Country.Test", "Telephones", "Address.Country").
		Where("id IN (?,?)", 1, 2).
		Find(&persons)

	c.Assert(err, IsNil)
	c.Assert(persons, HasLen, 2)

	c.Assert(persons[0].Id, Equals, 1)
	c.Assert(persons[0].Address, NotNil)
	c.Assert(persons[0].Address.Id, Equals, 1)
	c.Assert(persons[0].Address.Country, NotNil)
	c.Assert(persons[0].Address.Country.Id, Equals, 1)
	c.Assert(persons[0].OptionalAddress, NotNil)
	c.Assert(persons[0].OptionalAddress.Id, Equals, 2)
	c.Assert(persons[0].OptionalAddress.Country, NotNil)
	c.Assert(persons[0].OptionalAddress.Country.Id, Equals, 2)
	c.Assert(persons[0].Telephones, HasLen, 4)

	c.Assert(persons[1].Id, Equals, 2)
	c.Assert(persons[1].Address, NotNil)
	c.Assert(persons[1].Address.Id, Equals, 3)
	c.Assert(persons[1].Address.Country, NotNil)
	c.Assert(persons[1].Address.Country.Id, Equals, 3)
	c.Assert(persons[1].OptionalAddress, NotNil)
	c.Assert(persons[1].OptionalAddress.Id, Equals, 4)
	c.Assert(persons[1].OptionalAddress.Country, NotNil)
	c.Assert(persons[1].OptionalAddress.Country.Id, Equals, 4)
	c.Assert(persons[1].Telephones, HasLen, 0)
}

func (s *dependendSuite) TestFind_DependentColumns_WhereDeep(c *C) {
	var persons []Person
	err := s.db.Query().
		DependentColumns("OptionalAddress.Country", "OptionalAddress", "Telephones", "Address.Country").
		Where("OptionalAddress.country.name = ?", "usa").
		Find(&persons)

	c.Assert(err, IsNil)
	c.Assert(persons, HasLen, 2)
	c.Assert(persons[0].Id, Equals, 1)
	c.Assert(persons[0].Address, NotNil)
	c.Assert(persons[0].Address.Id, Equals, 1)
	c.Assert(persons[0].Address.Country, NotNil)
	c.Assert(persons[0].Address.Country.Id, Equals, 1)
	c.Assert(persons[0].OptionalAddress, NotNil)
	c.Assert(persons[0].OptionalAddress.Id, Equals, 2)
	c.Assert(persons[0].OptionalAddress.Country, NotNil)
	c.Assert(persons[0].OptionalAddress.Country.Id, Equals, 2)
	c.Assert(persons[0].Telephones, HasLen, 4)

	c.Assert(persons[1].Id, Equals, 4)
	c.Assert(persons[1].Address, NotNil)
	c.Assert(persons[1].Address.Id, Equals, 2)
	c.Assert(persons[1].Address.Country, NotNil)
	c.Assert(persons[1].Address.Country.Id, Equals, 2)
	c.Assert(persons[1].OptionalAddress, NotNil)
	c.Assert(persons[1].OptionalAddress.Id, Equals, 2)
	c.Assert(persons[1].OptionalAddress.Country, NotNil)
	c.Assert(persons[1].OptionalAddress.Country.Id, Equals, 2)
	c.Assert(persons[1].Telephones, HasLen, 2)
}

func (s *dependendSuite) TestFind_DependentColumns_LevelDeepOptional(c *C) {
	var parentPersons []ParentPerson
	err := s.db.Query().
		DependentColumns("Person.OptionalAddress.Country", "Person.Address.Country", "Person.Telephones").
		Where("person.id = ?", 4).
		Find(&parentPersons)

	c.Assert(err, IsNil)
	c.Assert(parentPersons, HasLen, 1)

	c.Assert(parentPersons[0].Person, NotNil)
	c.Assert(parentPersons[0].Person.Id, Equals, 4)
	c.Assert(parentPersons[0].Person.Address, NotNil)
	c.Assert(parentPersons[0].Person.Address.Id, Equals, 2)
	c.Assert(parentPersons[0].Person.Address.Country, NotNil)
	c.Assert(parentPersons[0].Person.Address.Country.Id, Equals, 2)
	c.Assert(parentPersons[0].Person.OptionalAddress, NotNil)
	c.Assert(parentPersons[0].Person.OptionalAddress.Id, Equals, 2)
	c.Assert(parentPersons[0].Person.OptionalAddress.Country, NotNil)
	c.Assert(parentPersons[0].Person.OptionalAddress.Country.Id, Equals, 2)
	c.Assert(parentPersons[0].Person.Telephones, HasLen, 2)
}

/*******************************************
 * First
 *******************************************/
func (s *dependendSuite) TestFirst_DependentColumns(c *C) {
	var person Person
	err := s.db.Query().
		DependentColumns("OptionalAddress", "Telephones", "Address").
		Where("id = ?", 4).
		First(&person)

	c.Assert(err, IsNil)
	c.Assert(person.Id, Equals, 4)
	c.Assert(person.Address, NotNil)
	c.Assert(person.Address.Id, Equals, 2)
	c.Assert(person.Address.Country, IsNil)
	c.Assert(person.OptionalAddress, NotNil)
	c.Assert(person.OptionalAddress.Id, Equals, 2)
	c.Assert(person.OptionalAddress.Country, IsNil)
	c.Assert(person.Telephones, HasLen, 2)
}

func (s *dependendSuite) TestFirst_DependentColumns_Where(c *C) {
	var person Person
	err := s.db.Query().
		DependentColumns("OptionalAddress", "Telephones", "Address").
		Where("OptionalAddress.id = ?", 4).
		First(&person)

	c.Assert(err, IsNil)
	c.Assert(person.Id, Equals, 2)
	c.Assert(person.Address, NotNil)
	c.Assert(person.Address.Id, Equals, 3)
	c.Assert(person.Address.Country, IsNil)
	c.Assert(person.OptionalAddress, NotNil)
	c.Assert(person.OptionalAddress.Id, Equals, 4)
	c.Assert(person.OptionalAddress.Country, IsNil)
	c.Assert(person.Telephones, HasLen, 0)
}

func (s *dependendSuite) TestFirst_DependentColumns_JoinDeep(c *C) {
	var person Person
	err := s.db.Query().
		DependentColumns("OptionalAddress.Country", "OptionalAddress", "OptionalAddress.Test.Test", "OptionalAddress.Country.Test", "Telephones", "Address.Country").
		Where("id = ?", 1).
		Find(&person)

	c.Assert(err, IsNil)
	c.Assert(person.Id, Equals, 1)
	c.Assert(person.Address, NotNil)
	c.Assert(person.Address.Id, Equals, 1)
	c.Assert(person.Address.Country, NotNil)
	c.Assert(person.Address.Country.Id, Equals, 1)
	c.Assert(person.OptionalAddress, NotNil)
	c.Assert(person.OptionalAddress.Id, Equals, 2)
	c.Assert(person.OptionalAddress.Country, NotNil)
	c.Assert(person.OptionalAddress.Country.Id, Equals, 2)
	c.Assert(person.Telephones, HasLen, 4)
}

func (s *dependendSuite) TestFirst_DependentColumns_WhereDeep(c *C) {
	var person Person
	err := s.db.Query().
		DependentColumns("OptionalAddress.Country", "OptionalAddress", "Telephones", "Address.Country").
		Where("OptionalAddress.country.name = ?", "fr").
		Find(&person)

	c.Assert(err, IsNil)
	c.Assert(person.Id, Equals, 2)
	c.Assert(person.Address, NotNil)
	c.Assert(person.Address.Id, Equals, 3)
	c.Assert(person.Address.Country, NotNil)
	c.Assert(person.Address.Country.Id, Equals, 3)
	c.Assert(person.OptionalAddress, NotNil)
	c.Assert(person.OptionalAddress.Id, Equals, 4)
	c.Assert(person.OptionalAddress.Country, NotNil)
	c.Assert(person.OptionalAddress.Country.Id, Equals, 4)
	c.Assert(person.Telephones, HasLen, 0)
}

func (s *dependendSuite) TestFirst_DependentColumns_LevelDeepOptional(c *C) {
	var parentPerson ParentPerson
	err := s.db.Query().
		DependentColumns("Person.OptionalAddress.Country", "Person.Address.Country", "Person.Telephones").
		Where("person.id = ?", 4).
		First(&parentPerson)

	c.Assert(err, IsNil)
	c.Assert(parentPerson.Person, NotNil)
	c.Assert(parentPerson.Person.Id, Equals, 4)
	c.Assert(parentPerson.Person.Address, NotNil)
	c.Assert(parentPerson.Person.Address.Id, Equals, 2)
	c.Assert(parentPerson.Person.Address.Country, NotNil)
	c.Assert(parentPerson.Person.Address.Country.Id, Equals, 2)
	c.Assert(parentPerson.Person.OptionalAddress, NotNil)
	c.Assert(parentPerson.Person.OptionalAddress.Id, Equals, 2)
	c.Assert(parentPerson.Person.OptionalAddress.Country, NotNil)
	c.Assert(parentPerson.Person.OptionalAddress.Country.Id, Equals, 2)
	c.Assert(parentPerson.Person.Telephones, HasLen, 2)
}

/***
 * Test dependend function
 */

func (s *dependendSuite) TestDependent(c *C) {
	var person *Person

	//get enity and fetch dependent
	c.Assert(s.db.Query().Where("id = ?", 1).First(&person), IsNil)
	c.Assert(s.db.Dependent(&person, "OptionalAddress", "Telephones", "Address"), IsNil)

	c.Assert(person.Address, NotNil)
	c.Assert(person.Address.Id, Equals, 1)
	c.Assert(person.Address.Country, IsNil)
	c.Assert(person.OptionalAddress, NotNil)
	c.Assert(person.OptionalAddress.Id, Equals, 2)
	c.Assert(person.OptionalAddress.Country, IsNil)
	c.Assert(person.Telephones, HasLen, 4)
}

func (s *dependendSuite) TestDependent_Deep(c *C) {
	var person *Person

	//get enity and fetch dependent
	c.Assert(s.db.Query().Where("id = ?", 1).First(&person), IsNil)
	c.Assert(s.db.Dependent(&person, "OptionalAddress.Country", "Telephones", "Address.Country"), IsNil)

	c.Assert(person.Address, NotNil)
	c.Assert(person.Address.Id, Equals, 1)
	c.Assert(person.Address.Country, NotNil)
	c.Assert(person.Address.Country.Id, Equals, 1)
	c.Assert(person.OptionalAddress, NotNil)
	c.Assert(person.OptionalAddress.Id, Equals, 2)
	c.Assert(person.OptionalAddress.Country, NotNil)
	c.Assert(person.OptionalAddress.Country.Id, Equals, 2)
	c.Assert(person.Telephones, HasLen, 4)
}

func (s *dependendSuite) TestDependent_Grouped(c *C) {
	var person *Person

	//get enity and fetch dependent
	c.Assert(s.db.Query().Where("id = ?", 1).First(&person), IsNil)
	c.Assert(s.db.Dependent(&person, "OptionalAddress.Country", "OptionalAddress", "Telephones", "Address", "Address.Country"), IsNil)

	c.Assert(person.Address, NotNil)
	c.Assert(person.Address.Id, Equals, 1)
	c.Assert(person.Address.Country, NotNil)
	c.Assert(person.Address.Country.Id, Equals, 1)
	c.Assert(person.OptionalAddress, NotNil)
	c.Assert(person.OptionalAddress.Id, Equals, 2)
	c.Assert(person.OptionalAddress.Country, NotNil)
	c.Assert(person.OptionalAddress.Country.Id, Equals, 2)
	c.Assert(person.Telephones, HasLen, 4)
}

func (s *dependendSuite) TestDependentColumns_WrongInput(c *C) {
	var person *Person
	c.Assert(s.db.Dependent(&person, "Tag", "TagPtr", "Tags", "TagsPtr", "ManyTags", "ManyTagsPtr"), ErrorMatches, "Cannot get dependent fields on nil struct")
}
