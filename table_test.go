package storm

import (
	"reflect"
	"testing"
)

type overrideTestToInt int

type testStructureWithTags struct {
	Id               int               `db:"name(xId),pk" json:"id"`
	Name             string            `json:"name"`
	SnakeId          overrideTestToInt `db:"type(int)"`
	Hidden           string            `db:"ignore" json:"-"`
	Tags             []TestProductTags
	TagsPtr          []*TestProductTags
	localNotExported int
}

type TestProductTags struct {
	Id            int
	TestProductId int
	Tags          string
}

type testProductDescription struct {
	Name  string
	Price float64
}

type testProduct struct {
	Id int
	testProductDescription
}

func TestTable_ParseTags(t *testing.T) {

	//test empty tag
	tags := parseTags("")
	if len(tags) != 0 {
		t.Errorf("Expected to have no extracted tags, got %d extracted tags %v", len(tags), tags)
	}

	//test tags with 1 property
	tags = parseTags("name(abc)")
	if len(tags) != 1 {
		t.Errorf("Expected to have 1 extracted tags, got %d extracted tags", len(tags))
	}

	//test tag with 2 properties
	tags = parseTags("test,name(abc)")
	if len(tags) != 2 {
		t.Fatalf("Expected to have 2 extracted tags, got %d extracted tags", len(tags))
	}

	if _, ok := tags["test"]; !ok {
		t.Errorf("Expected tag test")
	}

	if _, ok := tags["name"]; !ok {
		t.Fatalf("Expected tag name")
	}

	if tags["name"] != "abc" {
		t.Fatalf("Expected tag name have the value 'abc', instead i got %s", tags["name"])
	}
}

func TestTable_ExtractStructColumns_Tags(t *testing.T) {

	columns, relations := extractStructColumns(reflect.ValueOf(testStructureWithTags{}), nil)

	//check the column count, ignoring 1 column
	if len(columns) != 3 {
		t.Fatalf("Expected to have 3 columns in the structure, got %d columns", len(columns))
	}

	if len(relations) != 2 {
		t.Fatalf("Expected to have 2 rel;ationColumns in the structure, got %d relatiopn columns", len(relations))
	}

	//column name should be read from the tag name(xId)
	if columns[0].columnName != "xId" {
		t.Errorf("Expected column name 'xId', got '%s'", columns[0].columnName)
	}

	//column name should be lower case based on the structure name
	if columns[1].columnName != "name" {
		t.Errorf("Expected column name 'name', got '%s'", columns[1].columnName)
	}

	//column name should be lower case based on the structure name
	if columns[2].columnName != "snake_id" {
		t.Errorf("Expected column name 'snake_id', got '%s'", columns[2].columnName)
	}

	//check type is a int on column id
	if columns[0].goType.Kind() != reflect.Int {
		t.Errorf("Expected column id to be of type int, got '%s'", columns[0].goType.String())
	}

	//check type is a string on column name
	if columns[1].goType.Kind() != reflect.String {
		t.Errorf("Expected column name to be of type string, got '%s'", columns[1].goType.String())
	}

	//check type is a int
	if columns[2].goType.Kind() != reflect.Int {
		t.Errorf("Expected column name to be of type int (override by type(int), got '%s'", columns[2].goType.String())
	}
}

func TestTable_ExtractStructColumns_EmbeddedStruct(t *testing.T) {

	columns, _ := extractStructColumns(reflect.ValueOf(testProduct{}), nil)

	//check the column count
	if len(columns) != 3 {
		t.Fatalf("Expected to have 3 columns in the structure, got %d columns", len(columns))
	}

	//column name from the structure
	if columns[0].columnName != "id" {
		t.Errorf("Expected var name 'id', got '%s'", columns[0].columnName)
	}

	//column name should be lower case based on the structure name
	if columns[1].columnName != "name" {
		t.Errorf("Expected var name 'name', got '%s'", columns[1].columnName)
	}

	//column name should be lower case based on the structure name
	if columns[2].columnName != "price" {
		t.Errorf("Expected var name 'price', got '%s'", columns[2].columnName)
	}

	//check type is a int on column id
	if columns[0].goType.Kind() != reflect.Int {
		t.Errorf("Expected column id to be of type int, got '%s'", columns[0].goType.String())
	}

	//check type is a string on column name
	if columns[1].goType.Kind() != reflect.String {
		t.Errorf("Expected column name to be of type string, got '%s'", columns[1].goType.String())
	}

	//check type is a string on column name
	if columns[2].goType.Kind() != reflect.Float64 {
		t.Errorf("Expected column price to be of type string, got '%s'", columns[2].goType.String())
	}
}

func TestTable_FindPKs(t *testing.T) {

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
	col := findPKs([]*column{cdmmy, cfid, cai})
	if len(col) > 0 {
		t.Errorf("Expected to get no matches but got %d matches", len(col))
	}

	//1 match on pk key
	col = findPKs([]*column{cai, cfpk, cdmmy, cpk, cfid, cid, cai})
	if len(col) != 1 {
		t.Errorf("Expected to get 1 match but got %d matches `%v`", len(col), col[0])
	} else if col[0] != cpk {
		t.Errorf("Expected to get column `%v` but got `%v` column", cpk, col[0])
	}

	//2 matches on pk key
	col = findPKs([]*column{cai, cfpk, cpk, cdmmy, cpk, cfid, cid, cai})
	if len(col) != 2 {
		t.Errorf("Expected to get 2 match but got %d matches", len(col))
	} else if col[0] != cpk || col[1] != cpk {
		t.Errorf("Expected to get column `%v` but got `%v` and `%v` column", cpk, col[0], col[1])
	}

	//1 auto match on id name
	col = findPKs([]*column{cai, cfpk, cdmmy, cfid, cid})
	if len(col) != 1 {
		t.Errorf("Expected to get 1 match but got %d matches", len(col))
	} else if col[0] != cid {
		t.Errorf("Expected to get column `%v` but got `%v` column", cid, col[0])
	}
}

func TestTable_FindAI(t *testing.T) {

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

	//no match
	col := findAI([]*column{cfai, cdmmy1, cid}, nil)
	if col != nil {
		t.Errorf("Expected to get no match '%v'", col)
	}

	//found ai
	col = findAI([]*column{cdmmy1, cai, cid}, nil)
	if col != cai {
		t.Errorf("Expected to get a match with '%v' but got a match on `%v`", cai, col)
	}

	//fallback on pk
	col = findAI([]*column{cdmmy1, cid, cdmmy1}, []*column{cid})
	if col != cid {
		t.Errorf("Expected to get a match with '%v' but got a match on `%v`", cid, col)
	}

	//no match multiple pks
	col = findAI([]*column{cdmmy1, cid, cdmmy1}, []*column{cid, cid})
	if col != nil {
		t.Errorf("Expected to get no match '%v'", col)
	}

}

func TestTable_camelToSnake(t *testing.T) {
	actual := camelToSnake("TestGoCamelCasing")
	expected := "test_go_camel_casing"
	if actual != expected {
		t.Errorf("Expected `%s` but got `%s`", expected, actual)
	}
}

func TestTable_snakeToCamel(t *testing.T) {
	actual := snakeToCamel("test_go_camel_casing")
	expected := "TestGoCamelCasing"
	if actual != expected {
		t.Errorf("Expected `%s` but got `%s`", expected, actual)
	}
}
