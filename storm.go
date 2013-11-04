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

	err = row.Scan(structFields...)
	if err != nil {
		return nil, errors.New("Error in while scanning resilt '" + err.Error() + "'")
	}

	return nil, nil
}
