package storm

import (
	"database/sql"
	"errors"
	"reflect"
)

type Storm struct {
	repository *Repository
	db         *sql.DB
}

// Add a structure to the table map
//
// structure tags in db
// ignore = ignore entire struct
// pk = primary key
// name(alternativecolumnname) = alternative column name

//https://github.com/jinzhu/gorm/blob/master/main.go

func NewStorm(db *sql.DB, repository *Repository) *Storm {
	a := Storm{}
	a.repository = repository
	a.db = db

	return &a
}

//get a single entity from the datastore
func (a Storm) Get(entityName string, keys ...interface{}) (interface{}, error) {

	tblMap := a.repository.getTableMap(entityName)
	if tblMap == nil {
		return nil, errors.New("No entity with the name '" + entityName + "' found")
	}
	
	stmt, err := a.db.Prepare("SELECT id, name FROM "+tblMap.Name+" WHERE id = ?")
	if err != nil {
		return nil, errors.New("Error in prepared statement '" + err.Error() + "'")
	}
	defer stmt.Close()
	
	row := stmt.QueryRow(keys...)
	structFields := make([]interface{}, len(tblMap.columns))
		
	vt := reflect.New( tblMap.goType )
	
	
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
	
	
	
	//var test2 string
	//var test int
	
	
	
	structFields[0] = vt.Elem().FieldByName("Id").Addr().Interface()
	structFields[1] = vt.Elem().FieldByName("Name").Addr().Interface()
	
	err = row.Scan(structFields...)
	if err != nil {
		return nil, errors.New("Error in while scanning result '" + err.Error() + "'")
	}

	dst := vt.Interface()

	return dst, nil
}
