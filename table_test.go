package storm2

import (
	"reflect"
	"testing"
)

type testStructureWithTags struct {
	Id               int    `db:"name(xId),pk" json:"id"`
	Name             string `json:"name"`
	Hidden           string `db:"ignore" json:"-"`
	localNotExported int
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

	columns := extractStructColumns(reflect.ValueOf(testStructureWithTags{}), nil)

	//check the column count, ignoring 1 column
	if len(columns) != 2 {
		t.Fatalf("Expected to have 2 columns in the structure, got %d columns", len(columns))
	}

	//column name should be read from the tag name(xId)
	if columns[0].columnName != "xId" {
		t.Errorf("Expected column name 'xId', got '%s'", columns[0].columnName)
	}

	//column name should be lower case based on the structure name
	if columns[1].columnName != "name" {
		t.Errorf("Expected column name 'name', got '%s'", columns[1].columnName)
	}

	//check type is a int on column id
	if columns[0].goType.Kind() != reflect.Int {
		t.Errorf("Expected column id to be of type int, got '%s'", columns[0].goType.String())
	}

	//check type is a string on column name
	if columns[1].goType.Kind() != reflect.String {
		t.Errorf("Expected column name to be of type string, got '%s'", columns[1].goType.String())
	}
}

func TestTable_ExtractStructColumns_EmbeddedStruct(t *testing.T) {

	columns := extractStructColumns(reflect.ValueOf(testProduct{}), nil)

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

	//find pk with struct tag PK
	pks := findPKs(extractStructColumns(reflect.ValueOf(testStructureWithTags{}), nil))
	if len(pks) != 1 {
		t.Fatalf("Expected to get 1 column back but got %v", len(pks))
	}

	if pks[0].columnName != "xId" {
		t.Fatalf("Expected primary to be %v but got %v", "xId", pks[0].columnName)
	}

	//fallback fist id field of type int
	pks = findPKs(extractStructColumns(reflect.ValueOf(testProduct{}), nil))
	if len(pks) != 1 {
		t.Fatalf("Expected to get 1 column back but got %v", len(pks))
	}

	if pks[0].columnName != "id" {
		t.Fatalf("Expected primary to be %v but got %v", "id", pks[0].columnName)
	}

}
