package storm

import (
	"reflect"
	"testing"
)

func TestTableMap_ParseTags(t *testing.T) {

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

func TestTableMap_ReadColumnStructWithTags(t *testing.T) {

	columns, keys := readStructColumns(reflect.TypeOf(TestStructureWithTags{}), nil)

	//check the column count, ignoring 1 column
	if len(columns) != 2 {
		t.Fatalf("Expected to have 2 columns in the structure, got %d columns", len(columns))
	}

	//check the primary key count
	if len(keys) != 1 {
		t.Fatalf("Expected to have 1 primary key in the structure, got %d primary keys", len(keys))
	}

	//check if primary key is the id
	if keys[0].varName != "Id" {
		t.Errorf("Expected primary key var name to be 'Id', but got '%s'", keys[0].varName)
	}

	//column name should be read from the tag name(xId)
	if columns[0].Name != "xId" {
		t.Errorf("Expected column name 'xId', got '%s'", columns[0].Name)
	}

	//column name should be lower case based on the structure name
	if columns[1].Name != "name" {
		t.Errorf("Expected column name 'name', got '%s'", columns[1].Name)
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

func Test_TableMapReadEmbeddedStruct(t *testing.T) {

	columns, keys := readStructColumns(reflect.TypeOf(Product{}), nil)

	//check the column count
	if len(columns) != 3 {
		t.Fatalf("Expected to have 3 columns in the structure, got %d columns", len(columns))
	}

	//check the primary key count
	if len(keys) != 1 {
		t.Fatalf("Expected to have 1 primary key in the structure, got %d primary keys", len(keys))
	}

	//column name from the structure
	if columns[0].varName != "Id" {
		t.Errorf("Expected var name 'Id', got '%s'", columns[0].varName)
	}

	//column name should be lower case based on the structure name
	if columns[1].varName != "Name" {
		t.Errorf("Expected var name 'name', got '%s'", columns[1].varName)
	}
	
	//column name should be lower case based on the structure name
	if columns[2].varName != "Price" {
		t.Errorf("Expected var name 'name', got '%s'", columns[2].varName)
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
