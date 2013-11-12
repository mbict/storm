package storm

import (
	//"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestRepository_AddStructure(t *testing.T) {

	repo := NewRepository(newTestDialect())
	tblMap, err := repo.AddStructure(TestStructureWithTags{}, "TestStructureElement")
	if nil != err {
		t.Errorf("Returned a error with message \"%v\" while adding a element", err)
	}

	if tblMap == nil {
		t.Errorf("No TableMap returned")
	}
}

func Test_Repository_AddStructureWithPointer(t *testing.T) {

	repo := NewRepository(newTestDialect())
	tblMap, err := repo.AddStructure(&TestStructureWithTags{}, "TestStructureElement")
	if nil != err {
		t.Errorf("Returned a error with message \"%v\" while adding a element", err)
	}

	if tblMap == nil {
		t.Errorf("No TableMap returned")
	}
}

func TestRepository_AddStructureInvalidTypes(t *testing.T) {

	var err error
	repo := NewRepository(newTestDialect())

	//int type test
	inInt64 := int64(123)
	_, err = repo.AddStructure(inInt64, "TestStructure")
	if nil == err {
		t.Errorf("Didn't returned a error while i put a int64 as structure input")
	}

	_, err = repo.AddStructure(&inInt64, "TestStructure")
	if nil == err {
		t.Errorf("Didn't returned a error while i put a pointer to a int64 as structure input")
	}
}

func TestRepository_AddStructureDuplicates(t *testing.T) {

	var err error
	repo := NewRepository(newTestDialect())

	_, err = repo.AddStructure(TestStructure{}, "DuplicateStruct")
	if nil != err {
		t.Errorf("Did returned a error while not expected \"%v\"", err)
	}

	_, err = repo.AddStructure(TestStructure{}, "DuplicateStruct")
	if nil == err {
		t.Errorf("Did not returned a error while a duplicate error is expected")
	}

	if false == strings.Contains(err.Error(), "Duplicate structure, name: ") {
		t.Errorf("Got a error but not the duplicate error, got message: \"%v\"", err)
	}
}

func TestRepository_getTableMap(t *testing.T) {
	repo := newTestRepository()

	//test non existing
	tblMap := repo.getTableMap("nonExistingEntity")
	if nil != tblMap {
		t.Errorf("Expected a nil return, but got a tablemap back of type %v", tblMap.Name)
	}

	//test existing
	tblMap = repo.getTableMap("product")
	if nil == tblMap {
		t.Fatalf("Did not return a expected table map but returned nil")
	}

	if tblMap.Name != "product" {
		t.Fatalf("Expected to get the table map of product but got tablemap of %v", tblMap.Name)
	}

}

func TestRepository_hasTableMap(t *testing.T) {
	repo := newTestRepository()

	//test non existing
	if false != repo.hasTableMap("nonExistingEntity") {
		t.Errorf("Expected a false")
	}

	//test existing
	if true != repo.hasTableMap("product") {
		t.Errorf("Expected a true")
	}
}

func TestRepository_tableMapByType(t *testing.T) {
	repo := newTestRepository()

	//search for non existing in repo
	testStructure := TestStructureWithTags{}
	ttst := reflect.TypeOf(testStructure)
	tblMap := repo.tableMapByType(ttst)

	if tblMap != nil {
		t.Errorf("Expected a nil return, but got a tablemap back of type %v", tblMap.Name)
	}

	//search for existing in repo
	product := Product{}
	tp := reflect.TypeOf(product)
	tblMap = repo.tableMapByType(tp)

	if tblMap == nil {
		t.Fatalf("Did not return a expected table map but returned nil")
	}

	if tblMap.Name != "product" {
		t.Fatalf("Expected to get the table map of product but got tablemap of %v", tblMap.Name)
	}
}
