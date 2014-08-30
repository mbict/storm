package storm

import (
	"database/sql"
	"reflect"
	"time"

	. "gopkg.in/check.v1"
)

/*** test structures ***/
type overrideTestToInt int

type TestProduct struct {
	Id int
	testProductDescription
	Tag         TestTag //ONE ON ONE, uses id column
	TagId       int64
	TagPtr      *TestTag //ONE ON ONE, uses null ptr column
	TagPtrId    sql.NullInt64
	Tags        []TestProductTag  //One On Many, uses related table column id to referer to this struct
	TagsPtr     []*TestProductTag //One On Many, uses related table column id to referer to this struct
	ManyTags    []TestTag         //Many On Many, has a relation table to bind product and tag
	ManyTagsPtr []*TestTag        //Many On Many, has a relation table to bind product and tag

	localNotExported int
}

type TestProductTag struct {
	TestProductId int
	TestTag
}

type TestTag struct {
	Id  int
	Tag string
}

type testProductDescription struct {
	Name  string
	Price float64
}

type testStructureWithTags struct {
	Id               int               `db:"name(xId),pk" json:"id"`
	Name             string            `json:"name"`
	SnakeId          overrideTestToInt `db:"type(int)"`
	Hidden           string            `db:"ignore" json:"-"`
	localNotExported int
}

//*** test suite setup ***/
type tableSuite struct {
	db *Storm
}

var _ = Suite(&tableSuite{})

/*** tests ***/
func (s *tableSuite) TestParseTags(c *C) {

	c.Assert(parseTags(""), HasLen, 0)          //test empty tag
	c.Assert(parseTags("name(abc)"), HasLen, 1) //test tags with 1 property

	tags := parseTags("test,name(abc)")
	c.Assert(tags, HasLen, 2) //test tag with 2 properties
	_, hasTest := tags["test"]
	c.Assert(hasTest, Equals, true)
	_, hasName := tags["name"]
	c.Assert(hasName, Equals, true)
	c.Assert(tags["name"], Equals, "abc")
}

func (s *tableSuite) TestExtractStructColumns_Tags(c *C) {
	columns, relations := extractStructColumns(reflect.ValueOf(testStructureWithTags{}), nil)

	c.Assert(columns, HasLen, 3)                               //check the column count, ignoring 1 column
	c.Assert(relations, HasLen, 0)                             //check relation count
	c.Assert(columns[0].columnName, Equals, "xId")             //column name should be read from the tag name(xId)
	c.Assert(columns[1].columnName, Equals, "name")            //column name should be lower case based on the structure name
	c.Assert(columns[2].columnName, Equals, "snake_id")        //column name should be lower case based on the structure name
	c.Assert(columns[0].goType.Kind(), Equals, reflect.Int)    //check type is a int on column id
	c.Assert(columns[1].goType.Kind(), Equals, reflect.String) //check type is a string on column name
	c.Assert(columns[2].goType.Kind(), Equals, reflect.Int)    //check type is a int
}

func (s *tableSuite) TestExtractStructColumns_EmbeddedStruct(c *C) {
	columns, _ := extractStructColumns(reflect.ValueOf(TestProduct{}), nil)

	c.Assert(columns, HasLen, 5)                               //check the column count
	c.Assert(columns[0].columnName, Equals, "id")              //column name from the structure
	c.Assert(columns[0].goType.Kind(), Equals, reflect.Int)    //check type is a int on column id
	c.Assert(columns[1].columnName, Equals, "name")            //column name should be lower case based on the structure name
	c.Assert(columns[1].goType.Kind(), Equals, reflect.String) //check type is a string on column name
	c.Assert(columns[2].columnName, Equals, "price")
	c.Assert(columns[2].goType.Kind(), Equals, reflect.Float64)
	c.Assert(columns[3].columnName, Equals, "tag_id")
	c.Assert(columns[3].isScanner, Equals, false)
	c.Assert(columns[3].goType.Kind(), Equals, reflect.Int64)
	c.Assert(columns[4].columnName, Equals, "tag_ptr_id")      //column name should be lower case based on the structure name
	c.Assert(columns[4].goType.Kind(), Equals, reflect.Struct) //check type is a string on column name
	c.Assert(columns[4].isScanner, Equals, true)
}

func (s *tableSuite) TestFindPKs(c *C) {
	//setup test data
	cai := &column{
		columnName: "a",
		settings:   make(map[string]string),
		goType:     reflect.TypeOf(int(1)),
	}

	cid := &column{
		columnName: "id",
		settings:   make(map[string]string),
		goType:     reflect.TypeOf(int(1)),
	}

	cfid := &column{
		columnName: "id",
		settings:   make(map[string]string),
		goType:     reflect.TypeOf(string("test")),
	}

	cpk := &column{
		columnName: "id",
		settings:   make(map[string]string),
		goType:     reflect.TypeOf(int(1)),
	}
	cpk.settings["pk"] = "pk"

	cfpk := &column{
		columnName: "xId",
		settings:   make(map[string]string),
		goType:     reflect.TypeOf(string("test")),
	}
	cfpk.settings["pk"] = "pk"

	cdmmy := &column{
		columnName: "dummy1",
		settings:   make(map[string]string),
		goType:     reflect.TypeOf(int(1)),
	}

	//no match
	c.Assert(findPKs([]*column{cdmmy, cfid, cai}), HasLen, 0)

	//1 match on pk key
	c.Assert(findPKs([]*column{cai, cfpk, cdmmy, cpk, cfid, cid, cai}), HasLen, 1)
	c.Assert(findPKs([]*column{cai, cfpk, cdmmy, cpk, cfid, cid, cai})[0], Equals, cpk)

	//2 matches on pk key
	c.Assert(findPKs([]*column{cai, cfpk, cpk, cdmmy, cpk, cfid, cid, cai}), HasLen, 2)
	c.Assert(findPKs([]*column{cai, cfpk, cpk, cdmmy, cpk, cfid, cid, cai})[0], Equals, cpk)
	c.Assert(findPKs([]*column{cai, cfpk, cpk, cdmmy, cpk, cfid, cid, cai})[1], Equals, cpk)

	//1 auto match on id name
	c.Assert(findPKs([]*column{cai, cfpk, cdmmy, cfid, cid}), HasLen, 1)
	c.Assert(findPKs([]*column{cai, cfpk, cdmmy, cfid, cid})[0], Equals, cid)
}

func (s *tableSuite) TestFindAI(c *C) {
	//setup test data
	cai := &column{
		columnName: "a",
		settings:   make(map[string]string),
		goType:     reflect.TypeOf(int(1)),
	}
	cai.settings["ai"] = "ai"

	cfai := &column{
		columnName: "a",
		settings:   make(map[string]string),
		goType:     reflect.TypeOf(string("test")),
	}
	cfai.settings["ai"] = "ai"

	cid := &column{
		columnName: "id",
		settings:   make(map[string]string),
		goType:     reflect.TypeOf(int(1)),
	}
	cid.settings["pk"] = "pk"

	cdmmy1 := &column{
		columnName: "dummy1",
		settings:   make(map[string]string),
		goType:     reflect.TypeOf(string("test")),
	}

	c.Assert(findAI([]*column{cfai, cdmmy1, cid}, nil), IsNil)                    //no match
	c.Assert(findAI([]*column{cdmmy1, cai, cid}, nil), Equals, cai)               //found ai
	c.Assert(findAI([]*column{cdmmy1, cid, cdmmy1}, []*column{cid}), Equals, cid) //fallback on pk
	c.Assert(findAI([]*column{cdmmy1, cid, cdmmy1}, []*column{cid, cid}), IsNil)  //no match multiple pks
}

func (s *tableSuite) TestCamelToSnake(c *C) {
	c.Assert(camelToSnake("TestGoCamelCasing"), Equals, "test_go_camel_casing")
}

func (s *tableSuite) TestSnakeToCamel(c *C) {
	c.Assert(snakeToCamel("test_go_camel_casing"), Equals, "TestGoCamelCasing")
}

func (s *tableSuite) TestIsScanner(c *C) {
	c.Assert(isScanner(reflect.TypeOf(sql.NullInt64{})), Equals, true)
	c.Assert(isScanner(reflect.TypeOf(testCustomType(1))), Equals, true)
	c.Assert(isScanner(reflect.TypeOf(TestProduct{})), Equals, false)
	c.Assert(isScanner(reflect.TypeOf(int64(1))), Equals, false)
}

func (s *tableSuite) TestIsTime(c *C) {
	c.Assert(isTime(reflect.TypeOf(time.Time{})), Equals, true)
	c.Assert(isTime(reflect.TypeOf(testCustomType(1))), Equals, false)
	c.Assert(isTime(reflect.TypeOf(TestProduct{})), Equals, false)
	c.Assert(isTime(reflect.TypeOf(int64(1))), Equals, false)
}
