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

	//test invoke params
	onInsertInvoked      bool
	onPostInserteInvoked bool
	onUpdateInvoked      bool
	onPostUpdateInvoked  bool
	onDeleteInvoked      bool
	onPostDeleteInvoked  bool
	onInitInvoked        bool
}

//all posibile callbacks
func (person *Person) OnInsert()     { person.onInsertInvoked = true }
func (person *Person) OnPostInsert() { person.onPostInserteInvoked = true }
func (person *Person) OnUpdate()     { person.onUpdateInvoked = true }
func (person *Person) OnPostUpdate() { person.onPostUpdateInvoked = true }
func (person *Person) OnDelete()     { person.onDeleteInvoked = true }
func (person *Person) OnPostDelete() { person.onPostDeleteInvoked = true }
func (person *Person) OnInit()       { person.onInitInvoked = true }

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

type querySuite struct {
	db *Storm
}

var _ = Suite(&querySuite{})

func (s *querySuite) SetUpSuite(c *C) {

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

	assertExec := func(res sql.Result, err error) {
		c.Assert(err, IsNil)
	}

	//TABLES
	assertExec(s.db.DB().Exec("CREATE TABLE `person` (`id` INTEGER PRIMARY KEY, `name` TEXT, `address_id` INTEGER, `optional_address_id` INTEGER)"))
	assertExec(s.db.DB().Exec("CREATE TABLE `address` (`id` INTEGER PRIMARY KEY, `line1` TEXT, `line2` TEXT, `country_id` INTEGER)"))
	assertExec(s.db.DB().Exec("CREATE TABLE `country` (`id` INTEGER PRIMARY KEY, `name` TEXT)"))
	assertExec(s.db.DB().Exec("CREATE TABLE `telephone` (`id` INTEGER PRIMARY KEY, `person_id` INTEGER, `number` TEXT)"))

	//TEST DATA
	assertExec(s.db.DB().Exec("INSERT INTO `person` (`id`, `name`, `address_id`, `optional_address_id`) VALUES (1, 'person 1', 1, 2)"))
	assertExec(s.db.DB().Exec("INSERT INTO `person` (`id`, `name`, `address_id`, `optional_address_id`) VALUES (2, 'person 2', 3, 4)"))
	assertExec(s.db.DB().Exec("INSERT INTO `person` (`id`, `name`, `address_id`, `optional_address_id`) VALUES (3, 'person 3', 5, 1)"))
	assertExec(s.db.DB().Exec("INSERT INTO `person` (`id`, `name`, `address_id`, `optional_address_id`) VALUES (4, 'person 4', 2, 2)"))

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

/**************************************************************************
 * Tests Count
 **************************************************************************/
func (s *querySuite) Test_Count(c *C) {
	cnt, err := s.db.Query().Count((*Person)(nil))

	c.Assert(err, IsNil)
	c.Assert(cnt, Equals, int64(4))
}

func (s *querySuite) Test_Count_NoResult(c *C) {
	cnt, err := s.db.Query().
		Where("id = -1").
		Count((*Person)(nil))
	c.Assert(err, Equals, nil)
	c.Assert(cnt, Equals, int64(0))
}

//select, order by,where, limit and offset syntax check
func (s *querySuite) Test_Count_Where(c *C) {
	cnt, err := s.db.Query().
		Order("id", DESC).
		Limit(123).
		Offset(112).
		Where("id IN (?,?,?)", 1, 3, 4).
		Count((*Person)(nil))

	c.Assert(err, IsNil)
	c.Assert(cnt, Equals, int64(3))
}

//simple 1 level
func (s *querySuite) Test_Count_WhereAutoJoin(c *C) {
	cnt, err := s.db.Query().
		Where("optional_address.line1 = ?", "address 2 line 1").
		Count((*Person)(nil))

	c.Assert(err, IsNil)
	c.Assert(cnt, Equals, int64(2))
}

//join 2 levels deep
func (s *querySuite) Test_Count_WhereAutoJoinDeep(c *C) {
	cnt, err := s.db.Query().
		Where("OptionalAddress.Country.id = ?", 2).
		Count((*Person)(nil))

	c.Assert(err, IsNil)
	c.Assert(cnt, Equals, int64(2))
}

//auto join trough order by, but no order by stement
func (s *querySuite) Test_Count_WhereAutoJoinOrderBy(c *C) {
	cnt, err := s.db.Query().
		Order("optional_address.line1", ASC).
		Count((*Person)(nil))

	c.Assert(err, IsNil)
	c.Assert(cnt, Equals, int64(4))
}

//joining multiple tables (test no duplicate joins)
func (s *querySuite) Test_Count_WhereAutoJoinComplex(c *C) {
	cnt, err := s.db.Query().
		Where("id = ?", 1).
		Where("person.name = ?", "person 1").
		Where("Address.Country.id = ?", 1).
		Where("optional_address.line1 = ?", "address 2 line 1").
		Where("OptionalAddress.Country.id = ?", 2).
		Where("Address.line2 = ?", "address 1 line 2").
		Count((*Person)(nil))

	c.Assert(err, IsNil)
	c.Assert(cnt, Equals, int64(1))
}

//joining with a many to one table (count distinct id)
func (s *querySuite) Test_Count_WhereAutoJoinMany(c *C) {
	cnt, err := s.db.Query().
		Where("telephones.number IN	(?, ?, ?)", "111-11-1111", "111-33-1111", "444-11-1111"). //will match 3 (id: 1, 3, 6)
		Where("Telephones.Id IN (?,?,?,?)", 1, 2, 3, 6).
		Count((*Person)(nil))

	//only 2 unique persons
	c.Assert(err, IsNil)
	c.Assert(cnt, Equals, int64(2))
}

//auto join to parent record (tries to find a related structure)
func (s *querySuite) Test_Count_WhereAutoJoinReverseToParent(c *C) {
	cnt, err := s.db.Query().
		Where("Address.line2 IN (?,?,?)", "address 1 line 2", "address 2 line 2", "address 5 line 2").
		Count((*Country)(nil))

	c.Assert(err, IsNil)
	c.Assert(cnt, Equals, int64(2)) //should have count 2 address 1 & 2 count as 1 + address 2
}

//auto join to parent record (tries to find a related structure) willl only bind on the first occurnce
//in this case it will only bind on Address and not on OptionalAddress
func (s *querySuite) Test_Count_WhereAutoJoinReverseToParentFirstOccurence(c *C) {
	cnt, err := s.db.Query().
		Where("person.name IN (?,?,?)", "person 1", "person 2", "person 4").
		Count((*Address)(nil))

	c.Assert(err, IsNil)
	c.Assert(cnt, Equals, int64(3))
}

func (s *querySuite) Test_Count_WhereAutoJoinReverseToParentHint(c *C) {
	cnt, err := s.db.Query().
		Where("line1 = ?", "address 4 line 1").
		Where("person[optional_address].name = ?", "person 2").
		Count((*Address)(nil))

	c.Assert(err, IsNil)
	c.Assert(cnt, Equals, int64(1))
}

func (s *querySuite) Test_Count_WhereAutoJoinErrorTableResolve(c *C) {
	cnt, err := s.db.Query().
		Where("OptionalAddress.UnknownTable.id = ?", 1).
		Count((*Person)(nil))

	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, "Cannot resolve table `UnknownTable` in statement `OptionalAddress.UnknownTable.id`")
	c.Assert(cnt, Equals, int64(0))
}

func (s *querySuite) Test_Count_WhereAutoJoinErrorColumnResolve(c *C) {
	cnt, err := s.db.Query().
		Where("OptionalAddress.notexistingcolumn = ?", 1).
		Count((*Person)(nil))

	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, "Cannot find column `notexistingcolumn` found in table `address` used in statement `OptionalAddress.notexistingcolumn`")
	c.Assert(cnt, Equals, int64(0))
}

func (s *querySuite) Test_Count_ErrorStruct(c *C) {
	cnt, err := s.db.Query().
		Where("OptionalAddress.notexistingcolumn = ?", 1).
		Count((*Person)(nil))

	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, "Cannot find column `notexistingcolumn` found in table `address` used in statement `OptionalAddress.notexistingcolumn`")
	c.Assert(cnt, Equals, int64(0))
}

func (s *querySuite) Test_Count_ErrorNotRegistered(c *C) {
	type testNotRegistered struct{}
	_, err := s.db.Query().Count((*testNotRegistered)(nil))

	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, "no registered structure for `storm.testNotRegistered` found")
}

func (s *querySuite) Test_Count_ErrorNotAStruct(c *C) {
	var notastruct int
	_, err := s.db.Query().Count(notastruct)

	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, "provided input is not a structure type")
}

/**************************************************************************
 * Tests First
 **************************************************************************/
func (s *querySuite) Test_First(c *C) {
	var person *Person
	err := s.db.Query().First(&person)

	c.Assert(err, IsNil)
	c.Assert(person, DeepEquals, &Person{
		Id:                1,
		Name:              "person 1",
		Address:           nil,
		AddressId:         1,
		OptionalAddress:   nil,
		OptionalAddressId: sql.NullInt64{Int64: 2, Valid: true},
		Telephones:        nil,
		onInitInvoked:     true})
}

func (s *querySuite) Test_First_WhereObject(c *C) {
	var person *Person
	address := Address{Id: 3}
	err := s.db.Query().
		Where("address.id = ?", address).
		First(&person)

	c.Assert(err, IsNil)
	c.Assert(person, DeepEquals, &Person{
		Id:                2,
		Name:              "person 2",
		Address:           nil,
		AddressId:         3,
		OptionalAddress:   nil,
		OptionalAddressId: sql.NullInt64{Int64: 4, Valid: true},
		Telephones:        nil,
		onInitInvoked:     true})
}

func (s *querySuite) Test_First_WhereObjectPtr(c *C) {
	var person *Person
	address := &Address{Id: 3}
	err := s.db.Query().
		Where("address.id = ?", address).
		First(&person)

	c.Assert(err, IsNil)
	c.Assert(person, DeepEquals, &Person{
		Id:                2,
		Name:              "person 2",
		Address:           nil,
		AddressId:         3,
		OptionalAddress:   nil,
		OptionalAddressId: sql.NullInt64{Int64: 4, Valid: true},
		Telephones:        nil,
		onInitInvoked:     true})
}

func (s *querySuite) Test_First_NonPointer(c *C) {
	var person Person
	err := s.db.Query().First(&person)

	c.Assert(err, IsNil)
	c.Assert(person, DeepEquals, Person{
		Id:                1,
		Name:              "person 1",
		Address:           nil,
		AddressId:         1,
		OptionalAddress:   nil,
		OptionalAddressId: sql.NullInt64{Int64: 2, Valid: true},
		Telephones:        nil,
		onInitInvoked:     true})
}

func (s *querySuite) Test_First_NoResult(c *C) {
	var person *Person
	err := s.db.Query().
		Where("id < -1").
		First(&person)
	c.Assert(err, Equals, sql.ErrNoRows)
}

//select, order by,where, limit and offset syntax check
func (s *querySuite) Test_First_Where(c *C) {
	var person *Person
	err := s.db.Query().
		Order("id", DESC).
		Limit(123).
		Offset(2).
		Where("id IN (?,?,?)", 1, 3, 4).
		First(&person)

	c.Assert(err, IsNil)
	c.Assert(person, DeepEquals, &Person{
		Id:                1,
		Name:              "person 1",
		Address:           nil,
		AddressId:         1,
		OptionalAddress:   nil,
		OptionalAddressId: sql.NullInt64{Int64: 2, Valid: true},
		Telephones:        nil,
		onInitInvoked:     true})
}

//simple 1 level
func (s *querySuite) Test_First_WhereAutoJoin(c *C) {
	var person *Person
	err := s.db.Query().
		Where("optional_address.line1 = ?", "address 2 line 1").
		First(&person)

	c.Assert(err, IsNil)
	c.Assert(person, DeepEquals, &Person{
		Id:                1,
		Name:              "person 1",
		Address:           nil,
		AddressId:         1,
		OptionalAddress:   nil,
		OptionalAddressId: sql.NullInt64{Int64: 2, Valid: true},
		Telephones:        nil,
		onInitInvoked:     true})
}

//join 2 levels deep
func (s *querySuite) Test_First_WhereAutoJoinDeep(c *C) {
	var person *Person
	err := s.db.Query().
		Where("OptionalAddress.Country.id = ?", 2).
		First(&person)

	c.Assert(err, IsNil)
	c.Assert(person, DeepEquals, &Person{
		Id:                1,
		Name:              "person 1",
		Address:           nil,
		AddressId:         1,
		OptionalAddress:   nil,
		OptionalAddressId: sql.NullInt64{Int64: 2, Valid: true},
		Telephones:        nil,
		onInitInvoked:     true})
}

//auto join trough order by, but no order by stement
func (s *querySuite) Test_First_WhereAutoJoinOrderBy(c *C) {
	var person *Person
	err := s.db.Query().
		Order("optional_address.line1", ASC).
		First(&person)

	c.Assert(err, IsNil)
	c.Assert(person, DeepEquals, &Person{
		Id:                3,
		Name:              "person 3",
		Address:           nil,
		AddressId:         5,
		OptionalAddress:   nil,
		OptionalAddressId: sql.NullInt64{Int64: 1, Valid: true},
		Telephones:        nil,
		onInitInvoked:     true})
}

//joining multiple tables (test no duplicate joins)
func (s *querySuite) Test_First_WhereAutoJoinComplex(c *C) {
	var person *Person
	err := s.db.Query().
		Where("id = ?", 1).
		Where("person.name = ?", "person 1").
		Where("Address.Country.id = ?", 1).
		Where("optional_address.line1 = ?", "address 2 line 1").
		Where("OptionalAddress.Country.id = ?", 2).
		Where("Address.line2 = ?", "address 1 line 2").
		First(&person)

	c.Assert(err, IsNil)
	c.Assert(person, DeepEquals, &Person{
		Id:                1,
		Name:              "person 1",
		Address:           nil,
		AddressId:         1,
		OptionalAddress:   nil,
		OptionalAddressId: sql.NullInt64{Int64: 2, Valid: true},
		Telephones:        nil,
		onInitInvoked:     true})
}

//joining with a many to one table (count distinct id)
func (s *querySuite) Test_First_WhereAutoJoinMany(c *C) {
	var person *Person
	err := s.db.Query().
		Where("telephones.number IN	(?, ?, ?)", "111-11-1111", "111-33-1111", "444-11-1111"). //will match 3 (id: 1, 3, 6)
		Where("Telephones.Id IN (?,?,?,?)", 1, 2, 3, 6).
		First(&person)

	//only 2 unique persons
	c.Assert(err, IsNil)
	c.Assert(person, DeepEquals, &Person{
		Id:                1,
		Name:              "person 1",
		Address:           nil,
		AddressId:         1,
		OptionalAddress:   nil,
		OptionalAddressId: sql.NullInt64{Int64: 2, Valid: true},
		Telephones:        nil,
		onInitInvoked:     true})
}

//auto join to parent record (tries to find a related structure)
func (s *querySuite) Test_First_WhereAutoJoinReverseToParent(c *C) {
	var country *Country
	err := s.db.Query().
		Where("Address.line2 IN (?,?,?)", "address 1 line 2", "address 2 line 2", "address 5 line 2").
		First(&country)

	c.Assert(err, IsNil)
	c.Assert(country, DeepEquals, &Country{Id: 1, Name: "nl"})
}

//auto join to parent record (tries to find a related structure) willl only bind on the first occurnce
//in this case it will only bind on Address and not on OptionalAddress
func (s *querySuite) Test_First_WhereAutoJoinReverseToParentFirstOccurence(c *C) {
	var address *Address
	err := s.db.Query().
		Where("person.name IN (?,?,?)", "person 1", "person 2", "person 4").
		First(&address)

	c.Assert(err, IsNil)
	c.Assert(address, DeepEquals, &Address{
		Id:        1,
		Line1:     "address 1 line 1",
		Line2:     "address 1 line 2",
		Country:   nil,
		CountryId: 1})
}

func (s *querySuite) Test_First_WhereAutoJoinReverseToParentHint(c *C) {
	var address *Address
	err := s.db.Query().
		Where("line1 = ?", "address 4 line 1").
		Where("person[optional_address].name = ?", "person 2").
		First(&address)

	c.Assert(err, IsNil)
	c.Assert(address, DeepEquals, &Address{
		Id:        4,
		Line1:     "address 4 line 1",
		Line2:     "address 4 line 2",
		Country:   nil,
		CountryId: 4})
}

func (s *querySuite) Test_First_WhereAutoJoinErrorTableResolve(c *C) {
	var person *Person
	err := s.db.Query().
		Where("OptionalAddress.UnknownTable.id = ?", 1).
		First(&person)

	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, "Cannot resolve table `UnknownTable` in statement `OptionalAddress.UnknownTable.id`")
}

func (s *querySuite) Test_First_WhereAutoJoinErrorColumnResolve(c *C) {
	var person *Person
	err := s.db.Query().
		Where("OptionalAddress.notexistingcolumn = ?", 1).
		First(&person)

	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, "Cannot find column `notexistingcolumn` found in table `address` used in statement `OptionalAddress.notexistingcolumn`")
}

func (s *querySuite) Test_First_ErrorNotAStructure(c *C) {
	var notastruct int
	err := s.db.Query().First(&notastruct)

	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, "provided input is not a structure type")
}

func (s *querySuite) Test_First_ErrorNotRegistred(c *C) {
	type notRegisteredStruct struct{}
	var person *notRegisteredStruct
	err := s.db.Query().First(&person)

	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, "no registered structure for `storm.notRegisteredStruct` found")
}

func (s *querySuite) Test_First_ErrorNotByReference(c *C) {
	var person Person
	err := s.db.Query().First(person)

	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, "provided input is not by reference")
}

/**************************************************************************
 * Tests Find (single)
 **************************************************************************/
func (s *querySuite) Test_Find_Single(c *C) {
	var person *Person
	err := s.db.Query().Find(&person)

	c.Assert(err, IsNil)
	c.Assert(person, DeepEquals, &Person{
		Id:                1,
		Name:              "person 1",
		Address:           nil,
		AddressId:         1,
		OptionalAddress:   nil,
		OptionalAddressId: sql.NullInt64{Int64: 2, Valid: true},
		Telephones:        nil,
		onInitInvoked:     true})
}

func (s *querySuite) Test_Find_Single_NonPointer(c *C) {
	var person Person
	err := s.db.Query().Find(&person)

	c.Assert(err, IsNil)
	c.Assert(person, DeepEquals, Person{
		Id:                1,
		Name:              "person 1",
		Address:           nil,
		AddressId:         1,
		OptionalAddress:   nil,
		OptionalAddressId: sql.NullInt64{Int64: 2, Valid: true},
		Telephones:        nil,
		onInitInvoked:     true})
}

func (s *querySuite) Test_Find_Single_NoResults(c *C) {
	var person *Person
	err := s.db.Query().
		Where("id < -1").
		Find(&person)

	c.Assert(err, Equals, sql.ErrNoRows)
}

//select, order by,where, limit and offset syntax check
func (s *querySuite) Test_Find_Single_Where(c *C) {
	var person *Person
	err := s.db.Query().
		Order("id", DESC).
		Limit(123).
		Offset(2).
		Where("id IN (?,?,?)", 1, 3, 4).
		Find(&person)

	c.Assert(err, IsNil)
	c.Assert(person, DeepEquals, &Person{
		Id:                1,
		Name:              "person 1",
		Address:           nil,
		AddressId:         1,
		OptionalAddress:   nil,
		OptionalAddressId: sql.NullInt64{Int64: 2, Valid: true},
		Telephones:        nil,
		onInitInvoked:     true})
}

func (s *querySuite) Test_Find_Single_Where_Inline(c *C) {
	var person *Person
	err := s.db.Query().
		Find(&person, 2)

	c.Assert(err, IsNil)
	c.Assert(person, DeepEquals, &Person{
		Id:                2,
		Name:              "person 2",
		Address:           nil,
		AddressId:         3,
		OptionalAddress:   nil,
		OptionalAddressId: sql.NullInt64{Int64: 4, Valid: true},
		Telephones:        nil,
		onInitInvoked:     true})
}

//use object for inline where
func (s *querySuite) Test_Find_Single_Where_InlineStatementObject(c *C) {
	var person *Person
	address := Address{Id: 3}
	err := s.db.Query().
		Find(&person, "address.id = ?", address)

	c.Assert(err, IsNil)
	c.Assert(person, DeepEquals, &Person{
		Id:                2,
		Name:              "person 2",
		Address:           nil,
		AddressId:         3,
		OptionalAddress:   nil,
		OptionalAddressId: sql.NullInt64{Int64: 4, Valid: true},
		Telephones:        nil,
		onInitInvoked:     true})
}

func (s *querySuite) Test_Find_Single_Where_InlineStatementObjectPtr(c *C) {
	var person *Person
	address := &Address{Id: 3}
	err := s.db.Query().
		Find(&person, "address.id = ?", address)

	c.Assert(err, IsNil)
	c.Assert(person, DeepEquals, &Person{
		Id:                2,
		Name:              "person 2",
		Address:           nil,
		AddressId:         3,
		OptionalAddress:   nil,
		OptionalAddressId: sql.NullInt64{Int64: 4, Valid: true},
		Telephones:        nil,
		onInitInvoked:     true})
}

func (s *querySuite) Test_Find_Single_Where_InlineStatement(c *C) {
	var person *Person
	err := s.db.Query().
		Find(&person, "id = ?", 2)

	c.Assert(err, IsNil)
	c.Assert(person, DeepEquals, &Person{
		Id:                2,
		Name:              "person 2",
		Address:           nil,
		AddressId:         3,
		OptionalAddress:   nil,
		OptionalAddressId: sql.NullInt64{Int64: 4, Valid: true},
		Telephones:        nil,
		onInitInvoked:     true})
}

//inline statements should not alter the origianal query
func (s *querySuite) Test_Find_Single_Where_InlineStatementNotPersistent(c *C) {
	var person *Person
	q := s.db.Query()
	err := q.Find(&person, "id = ?", 2)

	c.Assert(err, IsNil)
	c.Assert(person, DeepEquals, &Person{
		Id:                2,
		Name:              "person 2",
		Address:           nil,
		AddressId:         3,
		OptionalAddress:   nil,
		OptionalAddressId: sql.NullInt64{Int64: 4, Valid: true},
		Telephones:        nil,
		onInitInvoked:     true})

	err = q.Find(&person, "id = ?", 1)

	c.Assert(err, IsNil)
	c.Assert(person, DeepEquals, &Person{
		Id:                1,
		Name:              "person 1",
		Address:           nil,
		AddressId:         1,
		OptionalAddress:   nil,
		OptionalAddressId: sql.NullInt64{Int64: 2, Valid: true},
		Telephones:        nil,
		onInitInvoked:     true})
}

func (s *querySuite) Test_Find_Single_Where_InlineAutoJoin(c *C) {
	var person *Person
	err := s.db.Query().
		Find(&person, "optional_address.line1 = ?", "address 4 line 1")

	c.Assert(err, IsNil)
	c.Assert(person, DeepEquals, &Person{
		Id:                2,
		Name:              "person 2",
		Address:           nil,
		AddressId:         3,
		OptionalAddress:   nil,
		OptionalAddressId: sql.NullInt64{Int64: 4, Valid: true},
		Telephones:        nil,
		onInitInvoked:     true})
}

//simple 1 level
func (s *querySuite) Test_Find_Single_WhereAutoJoin(c *C) {
	var person *Person
	err := s.db.Query().
		Where("optional_address.line1 = ?", "address 2 line 1").
		Find(&person)

	c.Assert(err, IsNil)
	c.Assert(person, DeepEquals, &Person{
		Id:                1,
		Name:              "person 1",
		Address:           nil,
		AddressId:         1,
		OptionalAddress:   nil,
		OptionalAddressId: sql.NullInt64{Int64: 2, Valid: true},
		Telephones:        nil,
		onInitInvoked:     true})
}

//join 2 levels deep
func (s *querySuite) Test_Find_Single_WhereAutoJoinDeep(c *C) {
	var person *Person
	err := s.db.Query().
		Where("OptionalAddress.Country.id = ?", 2).
		Find(&person)

	c.Assert(err, IsNil)
	c.Assert(person, DeepEquals, &Person{
		Id:                1,
		Name:              "person 1",
		Address:           nil,
		AddressId:         1,
		OptionalAddress:   nil,
		OptionalAddressId: sql.NullInt64{Int64: 2, Valid: true},
		Telephones:        nil,
		onInitInvoked:     true})
}

//auto join trough order by, but no order by stement
func (s *querySuite) Test_Find_Single_WhereAutoJoinOrderBy(c *C) {
	var person *Person
	err := s.db.Query().
		Order("optional_address.line1", ASC).
		Find(&person)

	c.Assert(err, IsNil)
	c.Assert(person, DeepEquals, &Person{
		Id:                3,
		Name:              "person 3",
		Address:           nil,
		AddressId:         5,
		OptionalAddress:   nil,
		OptionalAddressId: sql.NullInt64{Int64: 1, Valid: true},
		Telephones:        nil,
		onInitInvoked:     true})
}

//joining multiple tables (test no duplicate joins)
func (s *querySuite) Test_Find_Single_WhereAutoJoinComplex(c *C) {
	var person *Person
	err := s.db.Query().
		Where("id = ?", 1).
		Where("person.name = ?", "person 1").
		Where("Address.Country.id = ?", 1).
		Where("optional_address.line1 = ?", "address 2 line 1").
		Where("OptionalAddress.Country.id = ?", 2).
		Where("Address.line2 = ?", "address 1 line 2").
		Find(&person)

	c.Assert(err, IsNil)
	c.Assert(person, DeepEquals, &Person{
		Id:                1,
		Name:              "person 1",
		Address:           nil,
		AddressId:         1,
		OptionalAddress:   nil,
		OptionalAddressId: sql.NullInt64{Int64: 2, Valid: true},
		Telephones:        nil,
		onInitInvoked:     true})
}

//joining with a many to one table (count distinct id)
func (s *querySuite) Test_Find_Single_WhereAutoJoinMany(c *C) {
	var person *Person
	err := s.db.Query().
		Where("telephones.number IN	(?, ?, ?)", "111-11-1111", "111-33-1111", "444-11-1111"). //will match 3 (id: 1, 3, 6)
		Where("Telephones.Id IN (?,?,?,?)", 1, 2, 3, 6).
		Find(&person)

	//only 2 unique persons
	c.Assert(err, IsNil)
	c.Assert(person, DeepEquals, &Person{
		Id:                1,
		Name:              "person 1",
		Address:           nil,
		AddressId:         1,
		OptionalAddress:   nil,
		OptionalAddressId: sql.NullInt64{Int64: 2, Valid: true},
		Telephones:        nil,
		onInitInvoked:     true})
}

//auto join to parent record (tries to find a related structure)
func (s *querySuite) Test_Find_Single_WhereAutoJoinReverseToParent(c *C) {
	var country *Country
	err := s.db.Query().
		Where("Address.line2 IN (?,?,?)", "address 1 line 2", "address 2 line 2", "address 5 line 2").
		Find(&country)

	c.Assert(err, IsNil)
	c.Assert(country, DeepEquals, &Country{Id: 1, Name: "nl"})
}

//auto join to parent record (tries to find a related structure) willl only bind on the Find occurnce
//in this case it will only bind on Address and not on OptionalAddress
func (s *querySuite) Test_Find_Single_WhereAutoJoinReverseToParentFindOccurence(c *C) {
	var address *Address
	err := s.db.Query().
		Where("person.name IN (?,?,?)", "person 1", "person 2", "person 4").
		Find(&address)

	c.Assert(err, IsNil)
	c.Assert(address, DeepEquals, &Address{
		Id:        1,
		Line1:     "address 1 line 1",
		Line2:     "address 1 line 2",
		Country:   nil,
		CountryId: 1})
}

func (s *querySuite) Test_Find_Single_WhereAutoJoinReverseToParentHint(c *C) {
	var address *Address
	err := s.db.Query().
		Where("line1 = ?", "address 4 line 1").
		Where("person[optional_address].name = ?", "person 2").
		Find(&address)

	c.Assert(err, IsNil)
	c.Assert(address, DeepEquals, &Address{
		Id:        4,
		Line1:     "address 4 line 1",
		Line2:     "address 4 line 2",
		Country:   nil,
		CountryId: 4})
}

func (s *querySuite) Test_Find_Single_WhereAutoJoinErrorTableResolve(c *C) {
	var person *Person
	err := s.db.Query().
		Where("OptionalAddress.UnknownTable.id = ?", 1).
		Find(&person)

	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, "Cannot resolve table `UnknownTable` in statement `OptionalAddress.UnknownTable.id`")
}

func (s *querySuite) Test_Find_Single_WhereAutoJoinErrorColumnResolve(c *C) {
	var person *Person
	err := s.db.Query().
		Where("OptionalAddress.notexistingcolumn = ?", 1).
		Find(&person)

	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, "Cannot find column `notexistingcolumn` found in table `address` used in statement `OptionalAddress.notexistingcolumn`")
}

func (s *querySuite) Test_Find_Single_ErrorNotAStructure(c *C) {
	var notastruct int
	err := s.db.Query().Find(&notastruct)

	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, "provided input is not a structure type")
}

func (s *querySuite) Test_Find_Single_ErrorNotRegistred(c *C) {
	type notRegisteredStruct struct{}
	var person *notRegisteredStruct
	err := s.db.Query().Find(&person)

	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, "no registered structure for `storm.notRegisteredStruct` found")
}

func (s *querySuite) Test_Find_Single_ErrorNotByReference(c *C) {
	var person Person
	err := s.db.Query().Find(person)

	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, "provided input is not by reference")
}

/**************************************************************************
 * Tests Find (slice)
 **************************************************************************/
func (s *querySuite) Test_Find_Slice(c *C) {
	var persons []*Person
	err := s.db.Query().Find(&persons)

	c.Assert(err, IsNil)
	c.Assert(persons, HasLen, 4)
	c.Assert(persons[0], DeepEquals, &Person{
		Id:                1,
		Name:              "person 1",
		Address:           nil,
		AddressId:         1,
		OptionalAddress:   nil,
		OptionalAddressId: sql.NullInt64{Int64: 2, Valid: true},
		Telephones:        nil,
		onInitInvoked:     true})
	c.Assert(persons[1], DeepEquals, &Person{
		Id:                2,
		Name:              "person 2",
		Address:           nil,
		AddressId:         3,
		OptionalAddress:   nil,
		OptionalAddressId: sql.NullInt64{Int64: 4, Valid: true},
		Telephones:        nil,
		onInitInvoked:     true})
	c.Assert(persons[2], DeepEquals, &Person{
		Id:                3,
		Name:              "person 3",
		Address:           nil,
		AddressId:         5,
		OptionalAddress:   nil,
		OptionalAddressId: sql.NullInt64{Int64: 1, Valid: true},
		Telephones:        nil,
		onInitInvoked:     true})
	c.Assert(persons[3], DeepEquals, &Person{
		Id:                4,
		Name:              "person 4",
		Address:           nil,
		AddressId:         2,
		OptionalAddress:   nil,
		OptionalAddressId: sql.NullInt64{Int64: 2, Valid: true},
		Telephones:        nil,
		onInitInvoked:     true})
}

func (s *querySuite) Test_Find_Slice_NonPointer(c *C) {
	var persons []Person
	err := s.db.Query().Find(&persons)

	c.Assert(err, IsNil)
	c.Assert(persons, HasLen, 4)
	c.Assert(persons[0], DeepEquals, Person{
		Id:                1,
		Name:              "person 1",
		Address:           nil,
		AddressId:         1,
		OptionalAddress:   nil,
		OptionalAddressId: sql.NullInt64{Int64: 2, Valid: true},
		Telephones:        nil,
		onInitInvoked:     true})
	c.Assert(persons[1], DeepEquals, Person{
		Id:                2,
		Name:              "person 2",
		Address:           nil,
		AddressId:         3,
		OptionalAddress:   nil,
		OptionalAddressId: sql.NullInt64{Int64: 4, Valid: true},
		Telephones:        nil,
		onInitInvoked:     true})
	c.Assert(persons[2], DeepEquals, Person{
		Id:                3,
		Name:              "person 3",
		Address:           nil,
		AddressId:         5,
		OptionalAddress:   nil,
		OptionalAddressId: sql.NullInt64{Int64: 1, Valid: true},
		Telephones:        nil,
		onInitInvoked:     true})
	c.Assert(persons[3], DeepEquals, Person{
		Id:                4,
		Name:              "person 4",
		Address:           nil,
		AddressId:         2,
		OptionalAddress:   nil,
		OptionalAddressId: sql.NullInt64{Int64: 2, Valid: true},
		Telephones:        nil,
		onInitInvoked:     true})
}

func (s *querySuite) Test_Find_Slice_NoResulss(c *C) {
	var persons []*Person
	err := s.db.Query().
		Where("id < -1").
		Find(&persons)

	c.Assert(err, Equals, sql.ErrNoRows)
}

//select, order by,where, limit and offset syntax check
func (s *querySuite) Test_Find_Slice_Where(c *C) {
	var persons []*Person
	err := s.db.Query().
		Order("id", DESC).
		Limit(123).
		Offset(2).
		Where("id IN (?,?,?)", 1, 3, 4).
		Find(&persons)

	c.Assert(err, IsNil)
	c.Assert(persons, HasLen, 1)
	c.Assert(persons[0], DeepEquals, &Person{
		Id:                1,
		Name:              "person 1",
		Address:           nil,
		AddressId:         1,
		OptionalAddress:   nil,
		OptionalAddressId: sql.NullInt64{Int64: 2, Valid: true},
		Telephones:        nil,
		onInitInvoked:     true})
}

//inline on id
func (s *querySuite) Test_Find_Slice_Where_Inline(c *C) {
	var persons []*Person
	err := s.db.Query().
		Find(&persons, 2)

	c.Assert(err, IsNil)
	c.Assert(persons, HasLen, 1)
	c.Assert(persons[0], DeepEquals, &Person{
		Id:                2,
		Name:              "person 2",
		Address:           nil,
		AddressId:         3,
		OptionalAddress:   nil,
		OptionalAddressId: sql.NullInt64{Int64: 4, Valid: true},
		Telephones:        nil,
		onInitInvoked:     true})
}

//inline with stement
func (s *querySuite) Test_Find_Slice_Where_InlineStatement(c *C) {
	var persons []*Person
	err := s.db.Query().
		Find(&persons, "id IN (?, ?)", 2, 4)

	c.Assert(err, IsNil)
	c.Assert(persons, HasLen, 2)
	c.Assert(persons[0], DeepEquals, &Person{
		Id:                2,
		Name:              "person 2",
		Address:           nil,
		AddressId:         3,
		OptionalAddress:   nil,
		OptionalAddressId: sql.NullInt64{Int64: 4, Valid: true},
		Telephones:        nil,
		onInitInvoked:     true})
	c.Assert(persons[1], DeepEquals, &Person{
		Id:                4,
		Name:              "person 4",
		Address:           nil,
		AddressId:         2,
		OptionalAddress:   nil,
		OptionalAddressId: sql.NullInt64{Int64: 2, Valid: true},
		Telephones:        nil,
		onInitInvoked:     true})
}

//inline statements should not alter the origianal query
func (s *querySuite) Test_Find_Slice_Where_InlineStatementNotPersistent(c *C) {
	var persons []*Person
	q := s.db.Query()
	err := q.Find(&persons, "id = ?", 2)

	c.Assert(err, IsNil)
	c.Assert(persons, HasLen, 1)
	c.Assert(persons[0], DeepEquals, &Person{
		Id:                2,
		Name:              "person 2",
		Address:           nil,
		AddressId:         3,
		OptionalAddress:   nil,
		OptionalAddressId: sql.NullInt64{Int64: 4, Valid: true},
		Telephones:        nil,
		onInitInvoked:     true})

	err = q.Find(&persons, "id = ?", 1)

	c.Assert(err, IsNil)
	c.Assert(persons, HasLen, 1)
	c.Assert(persons[0], DeepEquals, &Person{
		Id:                1,
		Name:              "person 1",
		Address:           nil,
		AddressId:         1,
		OptionalAddress:   nil,
		OptionalAddressId: sql.NullInt64{Int64: 2, Valid: true},
		Telephones:        nil,
		onInitInvoked:     true})
}

//simple 1 level
func (s *querySuite) Test_Find_Slice_WhereAutoJoin(c *C) {
	var persons []*Person
	err := s.db.Query().
		Where("optional_address.line1 = ?", "address 2 line 1").
		Find(&persons)

	c.Assert(err, IsNil)
	c.Assert(persons, HasLen, 2)
	c.Assert(persons[0], DeepEquals, &Person{
		Id:                1,
		Name:              "person 1",
		Address:           nil,
		AddressId:         1,
		OptionalAddress:   nil,
		OptionalAddressId: sql.NullInt64{Int64: 2, Valid: true},
		Telephones:        nil,
		onInitInvoked:     true})
	c.Assert(persons[1], DeepEquals, &Person{
		Id:                4,
		Name:              "person 4",
		Address:           nil,
		AddressId:         2,
		OptionalAddress:   nil,
		OptionalAddressId: sql.NullInt64{Int64: 2, Valid: true},
		Telephones:        nil,
		onInitInvoked:     true})
}

//join 2 levels deep
func (s *querySuite) Test_Find_Slice_WhereAutoJoinDeep(c *C) {
	var persons []*Person
	err := s.db.Query().
		Where("OptionalAddress.Country.id = ?", 2).
		Find(&persons)

	c.Assert(err, IsNil)
	c.Assert(persons, HasLen, 2)
	c.Assert(persons[0], DeepEquals, &Person{
		Id:                1,
		Name:              "person 1",
		Address:           nil,
		AddressId:         1,
		OptionalAddress:   nil,
		OptionalAddressId: sql.NullInt64{Int64: 2, Valid: true},
		Telephones:        nil,
		onInitInvoked:     true})
	c.Assert(persons[1], DeepEquals, &Person{
		Id:                4,
		Name:              "person 4",
		Address:           nil,
		AddressId:         2,
		OptionalAddress:   nil,
		OptionalAddressId: sql.NullInt64{Int64: 2, Valid: true},
		Telephones:        nil,
		onInitInvoked:     true})
}

//auto join trough order by, but no order by stement
func (s *querySuite) Test_Find_Slice_WhereAutoJoinOrderBy(c *C) {
	var persons []*Person
	err := s.db.Query().
		Order("optional_address.line1", ASC).
		Find(&persons)

	c.Assert(err, IsNil)
	c.Assert(persons, HasLen, 4)
	c.Assert(persons[0], DeepEquals, &Person{
		Id:                3,
		Name:              "person 3",
		Address:           nil,
		AddressId:         5,
		OptionalAddress:   nil,
		OptionalAddressId: sql.NullInt64{Int64: 1, Valid: true},
		Telephones:        nil,
		onInitInvoked:     true})
	c.Assert(persons[1], DeepEquals, &Person{
		Id:                1,
		Name:              "person 1",
		Address:           nil,
		AddressId:         1,
		OptionalAddress:   nil,
		OptionalAddressId: sql.NullInt64{Int64: 2, Valid: true},
		Telephones:        nil,
		onInitInvoked:     true})
	c.Assert(persons[2], DeepEquals, &Person{
		Id:                4,
		Name:              "person 4",
		Address:           nil,
		AddressId:         2,
		OptionalAddress:   nil,
		OptionalAddressId: sql.NullInt64{Int64: 2, Valid: true},
		Telephones:        nil,
		onInitInvoked:     true})
	c.Assert(persons[3], DeepEquals, &Person{
		Id:                2,
		Name:              "person 2",
		Address:           nil,
		AddressId:         3,
		OptionalAddress:   nil,
		OptionalAddressId: sql.NullInt64{Int64: 4, Valid: true},
		Telephones:        nil,
		onInitInvoked:     true})
}

//joining multiple tables (test no duplicate joins)
func (s *querySuite) Test_Find_Slice_WhereAutoJoinComplex(c *C) {
	var persons []*Person
	err := s.db.Query().
		Where("id = ?", 1).
		Where("person.name = ?", "person 1").
		Where("Address.Country.id = ?", 1).
		Where("optional_address.line1 = ?", "address 2 line 1").
		Where("OptionalAddress.Country.id = ?", 2).
		Where("Address.line2 = ?", "address 1 line 2").
		Find(&persons)

	c.Assert(err, IsNil)
	c.Assert(persons, HasLen, 1)
	c.Assert(persons[0], DeepEquals, &Person{
		Id:                1,
		Name:              "person 1",
		Address:           nil,
		AddressId:         1,
		OptionalAddress:   nil,
		OptionalAddressId: sql.NullInt64{Int64: 2, Valid: true},
		Telephones:        nil,
		onInitInvoked:     true})
}

//joining with a many to one table (count distinct id)
func (s *querySuite) Test_Find_Slice_WhereAutoJoinMany(c *C) {
	var persons []*Person
	err := s.db.Query().
		Where("telephones.number IN	(?, ?, ?)", "111-11-1111", "111-33-1111", "444-11-1111"). //will match 3 (id: 1, 3, 6)
		Where("Telephones.Id IN (?,?,?,?)", 1, 2, 3, 6).
		Find(&persons)

	//only 2 unique persons
	c.Assert(err, IsNil)
	c.Assert(persons, HasLen, 2)
	c.Assert(persons[0], DeepEquals, &Person{
		Id:                1,
		Name:              "person 1",
		Address:           nil,
		AddressId:         1,
		OptionalAddress:   nil,
		OptionalAddressId: sql.NullInt64{Int64: 2, Valid: true},
		Telephones:        nil,
		onInitInvoked:     true})
	c.Assert(persons[1], DeepEquals, &Person{
		Id:                4,
		Name:              "person 4",
		Address:           nil,
		AddressId:         2,
		OptionalAddress:   nil,
		OptionalAddressId: sql.NullInt64{Int64: 2, Valid: true},
		Telephones:        nil,
		onInitInvoked:     true})
}

//auto join to parent record (tries to find a related structure)
func (s *querySuite) Test_Find_Slice_WhereAutoJoinReverseToParent(c *C) {
	var countries []*Country
	err := s.db.Query().
		Where("Address.line2 IN (?,?,?)", "address 1 line 2", "address 2 line 2", "address 5 line 2").
		Find(&countries)

	c.Assert(err, IsNil)
	c.Assert(countries, HasLen, 2)
	c.Assert(countries[0], DeepEquals, &Country{Id: 1, Name: "nl"})
	c.Assert(countries[1], DeepEquals, &Country{Id: 2, Name: "usa"})
}

//auto join to parent record (tries to find a related structure) willl only bind on the Find occurnce
//in this case it will only bind on Address and not on OptionalAddress
func (s *querySuite) Test_Find_Slice_WhereAutoJoinReverseToParentFindOccurence(c *C) {
	var addresses []*Address
	err := s.db.Query().
		Where("person.name IN (?,?,?)", "person 1", "person 2", "person 4").
		Find(&addresses)

	c.Assert(err, IsNil)
	c.Assert(addresses, HasLen, 3)
	c.Assert(addresses[0], DeepEquals, &Address{
		Id:        1,
		Line1:     "address 1 line 1",
		Line2:     "address 1 line 2",
		Country:   nil,
		CountryId: 1})
	c.Assert(addresses[1], DeepEquals, &Address{
		Id:        2,
		Line1:     "address 2 line 1",
		Line2:     "address 2 line 2",
		Country:   nil,
		CountryId: 2})
	c.Assert(addresses[2], DeepEquals, &Address{
		Id:        3,
		Line1:     "address 3 line 1",
		Line2:     "address 3 line 2",
		Country:   nil,
		CountryId: 3})
}

func (s *querySuite) Test_Find_Slice_WhereAutoJoinReverseToParentHint(c *C) {
	var addresses []*Address
	err := s.db.Query().
		Where("line1 = ?", "address 4 line 1").
		Where("person[optional_address].name = ?", "person 2").
		Find(&addresses)

	c.Assert(err, IsNil)
	c.Assert(addresses, HasLen, 1)
	c.Assert(addresses[0], DeepEquals, &Address{
		Id:        4,
		Line1:     "address 4 line 1",
		Line2:     "address 4 line 2",
		Country:   nil,
		CountryId: 4})
}

//bug no append on an existing slice, should reset to and add 1 item
func (s *querySuite) Test_Find_Slice_ProvidedSliceResetNoAppend(c *C) {
	var addresses []*Address = []*Address{&Address{}, &Address{}, &Address{}}
	err := s.db.Query().
		Where("line1 = ?", "address 4 line 1").
		Where("person[optional_address].name = ?", "person 2").
		Find(&addresses)

	c.Assert(err, IsNil)
	c.Assert(addresses, HasLen, 1)
	c.Assert(addresses[0], DeepEquals, &Address{
		Id:        4,
		Line1:     "address 4 line 1",
		Line2:     "address 4 line 2",
		Country:   nil,
		CountryId: 4})
}

func (s *querySuite) Test_Find_Slice_ProvidedSliceResetNoResult(c *C) {
	var addresses []*Address = []*Address{&Address{}, &Address{}, &Address{}}
	err := s.db.Query().
		Where("id < -1").
		Find(&addresses)

	c.Assert(err, Equals, sql.ErrNoRows)
	c.Assert(addresses, HasLen, 0)
}

func (s *querySuite) Test_Find_Slice_WhereAutoJoinErrorTableResolve(c *C) {
	var persons []*Person
	err := s.db.Query().
		Where("OptionalAddress.UnknownTable.id = ?", 1).
		Find(&persons)

	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, "Cannot resolve table `UnknownTable` in statement `OptionalAddress.UnknownTable.id`")
}

func (s *querySuite) Test_Find_Slice_WhereAutoJoinErrorColumnResolve(c *C) {
	var persons []*Person
	err := s.db.Query().
		Where("OptionalAddress.notexistingcolumn = ?", 1).
		Find(&persons)

	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, "Cannot find column `notexistingcolumn` found in table `address` used in statement `OptionalAddress.notexistingcolumn`")
}

func (s *querySuite) Test_Find_Slice_ErrorSliceHasNoStructureType(c *C) {
	var notastruct []int
	err := s.db.Query().Find(&notastruct)

	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, "provided input slice has no structure type")
}

func (s *querySuite) Test_Find_Slice_ErrorNotRegistred(c *C) {
	type notRegisteredStruct struct{}
	var notregistered []*notRegisteredStruct
	err := s.db.Query().Find(&notregistered)

	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, "no registered structure for `storm.notRegisteredStruct` found")
}

func (s *querySuite) Test_Find_Slice_ErrorNotByReference(c *C) {
	var person []Person
	err := s.db.Query().Find(person)

	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, "provided input is not by reference")
}

func (s *querySuite) Test_Find_Slice_ErrorNotByReferencePtr(c *C) {
	var person []*Person
	err := s.db.Query().Find(person)

	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, "provided input is not by reference")
}

/**************************************************************************
 * Tests generateSelectSQL (helper)
 **************************************************************************/
func (s *querySuite) Test_GenerateSelectSQL(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*Person)(nil)).Elem())
	sql, bind, err := s.db.Query().
		generateSelectSQL(tbl)

	c.Assert(err, IsNil)
	c.Assert(bind, HasLen, 0)
	c.Assert(sql, Equals, "SELECT `person`.`id`, `person`.`name`, `person`.`address_id`, `person`.`optional_address_id` FROM `person` AS `person`")
}

//select, order by,where, limit and offset syntax check
func (s *querySuite) Test_GenerateSelectSQL_Where(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*Person)(nil)).Elem())
	sql, bind, err := s.db.Query().
		Order("id", DESC).
		Order("name", ASC).
		Limit(123).
		Offset(112).
		Where("id = ?", 1).
		generateSelectSQL(tbl)

	c.Assert(err, IsNil)
	c.Assert(bind, HasLen, 1)
	c.Assert(sql, Equals, "SELECT `person`.`id`, `person`.`name`, `person`.`address_id`, `person`.`optional_address_id` FROM `person` AS `person` "+
		"WHERE `person`.`id` = ? "+
		"ORDER BY `person`.`id` DESC, `person`.`name` ASC LIMIT 123 OFFSET 112")
}

//simple 1 level
func (s *querySuite) Test_GenerateSelectSQL_WhereAutoJoin(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*Person)(nil)).Elem())
	sql, bind, err := s.db.Query().
		Where("optional_address.line1 = ?", 2).
		generateSelectSQL(tbl)

	c.Assert(err, IsNil)
	c.Assert(bind, HasLen, 1)
	c.Assert(sql, Equals, "SELECT `person`.`id`, `person`.`name`, `person`.`address_id`, `person`.`optional_address_id` FROM `person` AS `person` "+
		"JOIN address AS person_optional_address ON person.optional_address_id = person_optional_address.id "+
		"WHERE `person_optional_address`.`line1` = ?")
}

//join 2 levels deep
func (s *querySuite) Test_GenerateSelectSQL_WhereAutoJoinDeep(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*Person)(nil)).Elem())
	sql, bind, err := s.db.Query().
		Where("OptionalAddress.Country.id = ?", 1).
		generateSelectSQL(tbl)

	c.Assert(err, IsNil)
	c.Assert(bind, HasLen, 1)
	c.Assert(sql, Equals, "SELECT `person`.`id`, `person`.`name`, `person`.`address_id`, `person`.`optional_address_id` FROM `person` AS `person` "+
		"JOIN address AS person_optional_address ON person.optional_address_id = person_optional_address.id "+
		"JOIN country AS person_optional_address_country ON person_optional_address.country_id = person_optional_address_country.id "+
		"WHERE `person_optional_address_country`.`id` = ?")
}

//auto join trough order by
func (s *querySuite) Test_GenerateSelectSQL_WhereAutoJoinOrderBy(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*Person)(nil)).Elem())
	sql, bind, err := s.db.Query().
		Order("optional_address.line1", ASC).
		generateSelectSQL(tbl)

	c.Assert(err, IsNil)
	c.Assert(bind, HasLen, 0)
	c.Assert(sql, Equals, "SELECT `person`.`id`, `person`.`name`, `person`.`address_id`, `person`.`optional_address_id` FROM `person` AS `person` "+
		"JOIN address AS person_optional_address ON person.optional_address_id = person_optional_address.id "+
		"ORDER BY `person_optional_address`.`line1` ASC")
}

//joining multiple tables (test no duplicate joins)
func (s *querySuite) Test_GenerateSelectSQL_WhereAutoJoinComplex(c *C) {
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
	c.Assert(sql, Equals, "SELECT `person`.`id`, `person`.`name`, `person`.`address_id`, `person`.`optional_address_id` FROM `person` AS `person` "+
		"JOIN address AS person_address ON person.address_id = person_address.id "+
		"JOIN country AS person_address_country ON person_address.country_id = person_address_country.id "+
		"JOIN address AS person_optional_address ON person.optional_address_id = person_optional_address.id "+
		"JOIN country AS person_optional_address_country ON person_optional_address.country_id = person_optional_address_country.id "+
		"WHERE `person`.`id` = ? AND `person`.`name` = ? AND `person_address_country`.`id` = ? AND `person_optional_address`.`line1` = ? AND "+
		"`person_optional_address_country`.`id` = ? AND `person_optional_address_country`.`id` = ? AND `person_address`.`line1` = ?")
}

//joining with a many to one table
func (s *querySuite) Test_GenerateSelectSQL_WhereAutoJoinMany(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*Person)(nil)).Elem())
	sql, bind, err := s.db.Query().
		Where("telephones.number = ?", 1).
		Where("Telephones.Id IN (?,?,?)", 1, 2, 3).
		generateSelectSQL(tbl)

	c.Assert(err, IsNil)
	c.Assert(bind, HasLen, 4)
	c.Assert(sql, Equals, "SELECT `person`.`id`, `person`.`name`, `person`.`address_id`, `person`.`optional_address_id` FROM `person` AS `person` "+
		"JOIN telephone AS person_telephones ON person.id = person_telephones.person_id "+
		"WHERE `person_telephones`.`number` = ? AND `person_telephones`.`id` IN (?,?,?) "+
		"GROUP BY `person`.`id`")
}

//auto join to parent record (tries to find a related structure)
func (s *querySuite) Test_GenerateSelectSQL_WhereAutoJoinReverseToParent(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*Country)(nil)).Elem())
	sql, bind, err := s.db.Query().
		Where("id = ?", 1).
		Where("country.name = ?", "test").
		Where("Address.line1 = ?", 2).
		generateSelectSQL(tbl)

	c.Assert(err, IsNil)
	c.Assert(bind, HasLen, 3)
	c.Assert(sql, Equals, "SELECT `country`.`id`, `country`.`name` FROM `country` AS `country` "+
		"JOIN address AS country_address_country ON country.id = country_address_country.country_id "+
		"WHERE `country`.`id` = ? AND `country`.`name` = ? AND `country_address_country`.`line1` = ? "+
		"GROUP BY `country`.`id`")
}

//auto join to parent record (tries to find a related structure) will only bind on the first occurnce
//in this case it will only bind on Address and not on OptionalAddress
func (s *querySuite) Test_GenerateSelectSQL_WhereAutoJoinReverseToParentFirstOccurence(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*Address)(nil)).Elem())
	sql, bind, err := s.db.Query().
		Where("id = ?", 1).
		Where("person.name = ?", "piet").
		generateSelectSQL(tbl)

	c.Assert(err, IsNil)
	c.Assert(bind, HasLen, 2)
	c.Assert(sql, Equals, "SELECT `address`.`id`, `address`.`line1`, `address`.`line2`, `address`.`country_id` FROM `address` AS `address` "+
		"JOIN person AS address_person_address ON address.id = address_person_address.address_id "+
		"WHERE `address`.`id` = ? AND `address_person_address`.`name` = ? "+
		"GROUP BY `address`.`id`")
}

//parent hinting support if multiple columns of the same type exists in the parent
func (s *querySuite) Test_GenerateSelectSQL_WhereAutoJoinReverseToParentHint(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*Address)(nil)).Elem())
	sql, bind, err := s.db.Query().
		Where("id = ?", 1).
		Where("person[optional_address].name = ?", "piet").
		generateSelectSQL(tbl)

	c.Assert(err, IsNil)
	c.Assert(bind, HasLen, 2)
	c.Assert(sql, Equals, "SELECT `address`.`id`, `address`.`line1`, `address`.`line2`, `address`.`country_id` FROM `address` AS `address` "+
		"JOIN person AS address_person_optional_address ON address.id = address_person_optional_address.optional_address_id "+
		"WHERE `address`.`id` = ? AND `address_person_optional_address`.`name` = ? "+
		"GROUP BY `address`.`id`")
}

func (s *querySuite) Test_GenerateSelectSQL_WhereAutoJoinErrorTableResolve(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*Person)(nil)).Elem())
	_, _, err := s.db.Query().
		Where("OptionalAddress.UnknownTable.id = ?", 1).
		generateSelectSQL(tbl)

	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, "Cannot resolve table `UnknownTable` in statement `OptionalAddress.UnknownTable.id`")
}

func (s *querySuite) Test_GenerateSelectSQL_WhereAutoJoinErrorColumnResolve(c *C) {
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
func (s *querySuite) Test_GenerateCountSQL(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*Person)(nil)).Elem())
	sql, bind, err := s.db.Query().
		generateCountSQL(tbl)

	c.Assert(err, IsNil)
	c.Assert(bind, HasLen, 0)
	c.Assert(sql, Equals, "SELECT COUNT(*) FROM `person` AS `person`")
}

//select, order by,where, limit and offset syntax check
func (s *querySuite) Test_GenerateCountSQL_Where(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*Person)(nil)).Elem())
	sql, bind, err := s.db.Query().
		Order("id", DESC).
		Limit(123).
		Offset(112).
		Where("id = ?", 1).
		generateCountSQL(tbl)

	c.Assert(err, IsNil)
	c.Assert(bind, HasLen, 1)
	c.Assert(sql, Equals, "SELECT COUNT(*) FROM `person` AS `person` WHERE `person`.`id` = ?")
}

//simple 1 level
func (s *querySuite) Test_GenerateCountSQL_WhereAutoJoin(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*Person)(nil)).Elem())
	sql, bind, err := s.db.Query().
		Where("optional_address.line1 = ?", 2).
		generateCountSQL(tbl)

	c.Assert(err, IsNil)
	c.Assert(bind, HasLen, 1)
	c.Assert(sql, Equals, "SELECT COUNT(*) FROM `person` AS `person` "+
		"JOIN address AS person_optional_address ON person.optional_address_id = person_optional_address.id "+
		"WHERE `person_optional_address`.`line1` = ?")
}

//join 2 levels deep
func (s *querySuite) Test_GenerateCountSQL_WhereAutoJoinDeep(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*Person)(nil)).Elem())
	sql, bind, err := s.db.Query().
		Where("OptionalAddress.Country.id = ?", 1).
		generateCountSQL(tbl)

	c.Assert(err, IsNil)
	c.Assert(bind, HasLen, 1)
	c.Assert(sql, Equals, "SELECT COUNT(*) FROM `person` AS `person` "+
		"JOIN address AS person_optional_address ON person.optional_address_id = person_optional_address.id "+
		"JOIN country AS person_optional_address_country ON person_optional_address.country_id = person_optional_address_country.id "+
		"WHERE `person_optional_address_country`.`id` = ?")
}

//auto join trough order by, but no order by stement
func (s *querySuite) Test_GenerateCountSQL_WhereAutoJoinOrderBy(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*Person)(nil)).Elem())
	sql, bind, err := s.db.Query().
		Order("optional_address.line1", ASC).
		generateCountSQL(tbl)

	c.Assert(err, IsNil)
	c.Assert(bind, HasLen, 0)
	c.Assert(sql, Equals, "SELECT COUNT(*) FROM `person` AS `person` "+
		"JOIN address AS person_optional_address ON person.optional_address_id = person_optional_address.id")
}

//joining multiple tables (test no duplicate joins)
func (s *querySuite) Test_GenerateCountSQL_WhereAutoJoinComplex(c *C) {
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
	c.Assert(sql, Equals, "SELECT COUNT(*) FROM `person` AS `person` "+
		"JOIN address AS person_address ON person.address_id = person_address.id "+
		"JOIN country AS person_address_country ON person_address.country_id = person_address_country.id "+
		"JOIN address AS person_optional_address ON person.optional_address_id = person_optional_address.id "+
		"JOIN country AS person_optional_address_country ON person_optional_address.country_id = person_optional_address_country.id "+
		"WHERE `person`.`id` = ? AND `person`.`name` = ? AND `person_address_country`.`id` = ? AND `person_optional_address`.`line1` = ? AND "+
		"`person_optional_address_country`.`id` = ? AND `person_optional_address_country`.`id` = ? AND `person_address`.`line1` = ?")
}

//joining with a many to one table
func (s *querySuite) Test_GenerateCountSQL_WhereAutoJoinMany(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*Person)(nil)).Elem())
	sql, bind, err := s.db.Query().
		Where("telephones.number = ?", 1).
		Where("Telephones.Id IN (?,?,?)", 1, 2, 3).
		generateCountSQL(tbl)

	c.Assert(err, IsNil)
	c.Assert(bind, HasLen, 4)
	c.Assert(sql, Equals, "SELECT COUNT(DISTINCT `person`.`id`) FROM `person` AS `person` "+
		"JOIN telephone AS person_telephones ON person.id = person_telephones.person_id "+
		"WHERE `person_telephones`.`number` = ? AND `person_telephones`.`id` IN (?,?,?)")
}

//auto join to parent record (tries to find a related structure)
func (s *querySuite) Test_GenerateCountSQL_WhereAutoJoinReverseToParent(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*Country)(nil)).Elem())
	sql, bind, err := s.db.Query().
		Where("id = ?", 1).
		Where("country.name = ?", "test").
		Where("Address.line1 = ?", 2).
		generateCountSQL(tbl)

	c.Assert(err, IsNil)
	c.Assert(bind, HasLen, 3)
	c.Assert(sql, Equals, "SELECT COUNT(DISTINCT `country`.`id`) FROM `country` AS `country` "+
		"JOIN address AS country_address_country ON country.id = country_address_country.country_id "+
		"WHERE `country`.`id` = ? AND `country`.`name` = ? AND `country_address_country`.`line1` = ?")
}

//auto join to parent record (tries to find a related structure) willl only bind on the first occurnce
//in this case it will only bind on Address and not on OptionalAddress
func (s *querySuite) Test_GenerateCountSQL_WhereAutoJoinReverseToParentFirstOccurence(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*Address)(nil)).Elem())
	sql, bind, err := s.db.Query().
		Where("id = ?", 1).
		Where("person.name = ?", "piet").
		generateCountSQL(tbl)

	c.Assert(err, IsNil)
	c.Assert(bind, HasLen, 2)
	c.Assert(sql, Equals, "SELECT COUNT(DISTINCT `address`.`id`) FROM `address` AS `address` "+
		"JOIN person AS address_person_address ON address.id = address_person_address.address_id "+
		"WHERE `address`.`id` = ? AND `address_person_address`.`name` = ?")
}

func (s *querySuite) Test_GenerateCountSQL_WhereAutoJoinReverseToParentHint(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*Address)(nil)).Elem())
	sql, bind, err := s.db.Query().
		Where("id = ?", 1).
		Where("person[optional_address].name = ?", "piet").
		generateCountSQL(tbl)

	c.Assert(err, IsNil)
	c.Assert(bind, HasLen, 2)
	c.Assert(sql, Equals, "SELECT COUNT(DISTINCT `address`.`id`) FROM `address` AS `address` "+
		"JOIN person AS address_person_optional_address ON address.id = address_person_optional_address.optional_address_id "+
		"WHERE `address`.`id` = ? AND `address_person_optional_address`.`name` = ?")
}

func (s *querySuite) Test_GenerateCountSQL_WhereAutoJoinErrorTableResolve(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*Person)(nil)).Elem())
	_, _, err := s.db.Query().
		Where("OptionalAddress.UnknownTable.id = ?", 1).
		generateCountSQL(tbl)

	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, "Cannot resolve table `UnknownTable` in statement `OptionalAddress.UnknownTable.id`")
}

func (s *querySuite) Test_GenerateCountSQL_WhereAutoJoinErrorColumnResolve(c *C) {
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
func (s *querySuite) Test_FormatAndResolveStatement(c *C) {
	personTbl, _ := s.db.tableByName("person")
	//addressTbl, _ := s.db.tableByName("address")

	//no table prefix
	statement, joins, tables, err := s.db.Query().formatAndResolveStatement(personTbl, "id = ?")
	c.Assert(err, IsNil)
	c.Assert(statement, HasLen, 1)
	c.Assert(statement[0], Equals, "`person`.`id` = ?")
	c.Assert(joins, Equals, "")
	c.Assert(tables, HasLen, 0)

	//hardcoded string condition, integer and float condition, glued statements, negative numbers
	statement, joins, tables, err = s.db.Query().formatAndResolveStatement(personTbl, "id = 'id' AND id = 123 AND id = 12.34 AND id=Id AND 1=id AND id=1 AND id = -1")
	c.Assert(err, IsNil)
	c.Assert(statement, HasLen, 1)
	c.Assert(statement[0], Equals, "`person`.`id` = 'id' AND `person`.`id` = 123 AND `person`.`id` = 12.34 AND `person`.`id`=`person`.`id` AND 1=`person`.`id` AND `person`.`id`=1 AND `person`.`id` = -1")
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
func (s *querySuite) TestDepends(c *C) {
	tbl, _ := s.db.table(reflect.TypeOf((*Person)(nil)).Elem())
	sql, _ := s.db.Query().
		DependentColumns("OptionalAddress", "Telephones", "Address.Country").
		generateSelectSQL2(tbl)

	c.Assert(sql, Equals, "SELECT")
}

func (s *querySuite) TestDependsWhereJoin(c *C) {
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
