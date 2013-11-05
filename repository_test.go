package storm

import (
	//"fmt"
	"strings"
	"testing"
)

func TestRepository_AddStructure(t *testing.T) {

	repo := NewRepository(&Dialect{})
	tblMap, err := repo.AddStructure(TestStructureWithTags{}, "TestStructureElement")
	if nil != err {
		t.Errorf("Returned a error with message \"%v\" while adding a element", err)
	}

	if tblMap == nil {
		t.Errorf("No TableMap returned")
	}
}

func Test_Repository_AddStructureWithPointer(t *testing.T) {

	repo := NewRepository(&Dialect{})
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
	repo := NewRepository(&Dialect{})

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
	repo := NewRepository(&Dialect{})

	_, err = repo.AddStructure(TestStructure{}, "DuplicateStruct")
	if nil != err {
		t.Errorf("Did returned a error while not expected \"%v\"", err)
	}

	_, err = repo.AddStructure(TestStructure{}, "DuplicateStruct")
	if nil == err {
		t.Errorf("Did not returned a error while a duplicate error is expected", err)
	}

	if false == strings.Contains(err.Error(), "Duplicate structure, name: ") {
		t.Errorf("Got a error but not the duplicate error, got message: \"%v\"", err)
	}
}
