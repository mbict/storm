package storm

import (
	"errors"
	"reflect"
)

type Repository struct {
	tables  map[string]*TableMap
	dialect Dialect
}

//Create a new storm connection
func NewRepository(dialect Dialect) *Repository {
	r := Repository{}
	r.dialect = dialect
	r.tables = make(map[string]*TableMap)

	return &r
}

//Add a structure to the repository
// Tags to use in structure `db:"name(altcolumn),pk"`
// ignore = ignore entire struct
// pk = primary key
// name(alternativecolumnname) = alternative column name
func (r Repository) AddStructure(structure interface{}, name string) (*TableMap, error) {

	t := reflect.TypeOf(structure)

	//if its a pointer get the real adrress of the element
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	//check if we got a structure here
	if t.Kind() != reflect.Struct {
		return nil, errors.New("Input value is not a struct but a " + t.Kind().String())
	}

	if _, ok := r.tables[name]; ok {
		return nil, errors.New("Duplicate structure, name: " + name + "  with type :'" + r.tables[name].goType.String() + "' already exists")
	}

	//read the structure
	columns, keys := readStructColumns(structure, nil)

	//add table map
	tblMap := &TableMap{
		Name:    name,
		goType:  t,
		columns: columns,
		keys:    keys,
	}

	//add columns to list
	r.tables[name] = tblMap

	return tblMap, nil
}

//Check if the repository has a table map matching the name
func (r Repository) hasTableMap(name string) bool {
	_, ok := r.tables[name]
	return ok
}

func (r Repository) getTableMap(name string) *TableMap {
	if tblMap, ok := r.tables[name]; ok {
		return tblMap
	}
	return nil
}

func (r Repository) tableMapByType(goType reflect.Type) *TableMap {

	for _, tblMap := range r.tables {
		if tblMap.goType == goType {
			return tblMap
		}
	}
	return nil
}
