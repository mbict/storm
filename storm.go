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
	
	//create a new structure
	vt := reflect.New( q.tblMap.goType )
	
	//get the columns in the structure
	scanFields := make([]interface{}, len(q.tblMap.columns))
	for key, col := range q.tblMap.columns {
		scanFields[key] = vt.Elem().FieldByIndex(col.goIndex).Addr().Interface()
	}

	//scan the row into the struct
	err = row.Scan(scanFields...)
	if err != nil {
		return nil, errors.New("Error in while scanning result '" + err.Error() + "'")
	}

	return vt.Interface(), nil
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
