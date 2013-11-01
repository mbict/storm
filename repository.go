package storm

import (
	"errors"
	"reflect"
)

type Repository struct {
	tables  map[string]*TableMap
	dialect *Dialect
}

//Create a new storm connection
func NewRepository(dialect *Dialect) *Repository {
	r := Repository{}
	r.dialect = dialect
	r.tables = make(map[string]*TableMap)

	return &r
}

//Add a structure to the repository
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

	if r.tables[name] != nil {
		return nil, errors.New("Duplicate structure, name: " + name + "  with type :'" + r.tables[name].goType.String() + "' already exists")
	}

	columns, keys := readStructColumns(t)

	//add table map
	tblMap := &TableMap{
		Name:    name,
		goType:  t,
		columns: columns,
		keys:    keys,
	}

	//add columns to list
	r.tables[name] = tblMap

	//fmt.Println("Element name: ", t.Name())
	//fmt.Println(a.tables)
	/*
		f := reflect.New(t)
		dst := f.Elem()

		fmt.Println("reflect create", f)
		fmt.Println("reflect create", f.Elem())

		aa := dst.Addr().Interface()

		fmt.Println("reflect create a dst.interface", aa)
		fmt.Println("reflect create a &dst.Interface", &aa)

		f.Elem().FieldByName("Id").SetInt(1234)

		dst2 := dst.Addr().Interface()

		fmt.Println("reflect create b dst.interface", aa)
		fmt.Println("reflect create b &dst.Interface", &aa)

		fmt.Println("reflect create c dst2.interface", dst2)
		fmt.Println("reflect create c &dst2.Interface", &dst2)
	*/

	return tblMap, nil
}

//Check if the repository has a table map matching the name
func (r Repository) hasTableMap(name string) bool {
	_, ok := r.tables[name]
	return ok
}
