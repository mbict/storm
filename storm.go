package storm

import (
	"database/sql"
	"errors"
	"reflect"
	"fmt"
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

//get a query stack for an entity
func (a *Storm) Query(entityName string) (*Query, error) {
	
	tblMap := a.repository.getTableMap(entityName)
	if tblMap == nil {
		return nil, errors.New("No entity with the name '" + entityName + "' found")
	}
	
	return NewQuery( tblMap, a), nil
}

//get a single entity from the datastore
func (a Storm) Get(entityName string, keys ...interface{}) (interface{}, error) {
	
	q, err := a.Query(entityName)
	if err != nil {
		return nil, err
	}
	
	//q.Limit(1).Order("id", DESC)
	
	//add where keys
	for key, col := range q.tblMap.keys {
		q.Where( fmt.Sprintf( "`%v` = ?", col.Name ), keys[key] )
	}

	sql, bind := q.prepareSelect()
	stmt, err := a.db.Prepare(sql)
	if err != nil {
		return nil, errors.New("Error in prepared statement '" + err.Error() + "'")
	}
	defer stmt.Close()
	
	row := stmt.QueryRow(bind...)
	
	
	
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
	

	//vt.Elem().Fi
		
	vt := reflect.New( q.tblMap.goType )
	scanFields := make([]interface{}, len(q.tblMap.columns))
	for key, col := range q.tblMap.columns {
		scanFields[key] = vt.Elem().FieldByIndex(col.goIndex).Addr().Interface()
	}
	
//	structFields[0] = vt.Elem().FieldByName("Id").Addr().Interface()
//	structFields[1] = vt.Elem().FieldByName("Name").Addr().Interface()
	
	err = row.Scan(scanFields...)
	if err != nil {
		return nil, errors.New("Error in while scanning result '" + err.Error() + "'")
	}

	dst := vt.Interface()

	return dst, nil
}

//get all the database entries
func (a Storm) GetAll(entityName string, args ...interface{}) ([]interface{}, error) {
	return nil, nil
}

//update a entity
func (a Storm) Save(entity interface{}) (error) {
	return nil
}

//delete a entity
func (a Storm) Delete(entity interface{}) (error) {
	return nil
}
