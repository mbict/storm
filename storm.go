package storm

import (
	"database/sql"
	"errors"
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

	if !a.repository.hasTableMap(entityName) {
		return nil, errors.New("No entity with the name '" + entityName + "' found")
	}

	return nil, nil
}
