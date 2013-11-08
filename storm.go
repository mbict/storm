package storm

import (
	"database/sql"
	"errors"
	"fmt"
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

//get a query stack for an entity
func (a *Storm) Query(entityName string) (*Query, error) {

	tblMap := a.repository.getTableMap(entityName)
	if tblMap == nil {
		return nil, errors.New("No entity with the name '" + entityName + "' found")
	}

	return NewQuery(tblMap, a), nil
}

//get a single entity from the datastore
func (a *Storm) Get(entityName string, keys ...interface{}) (interface{}, error) {

	q, err := a.Query(entityName)
	if err != nil {
		return nil, err
	}

	pkc := len(q.tblMap.keys)
	if pkc == 0 {
		return nil, errors.New("No primary key defined")
	}

	if pkc > len(keys) {
		return nil, errors.New(fmt.Sprintf("Not engough arguments for provided for primary keys, need %d attributes", pkc))
	}

	//add where keys
	for key, col := range q.tblMap.keys {
		q.Where(fmt.Sprintf("`%v` = ?", col.Name), keys[key])
	}

	sql, bind := q.generateSelectSQL()
	stmt, err := a.db.Prepare(sql)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	row := stmt.QueryRow(bind...)

	//create a new structure
	v := reflect.New(q.tblMap.goType)

	//get the columns in the structure
	scanFields := make([]interface{}, len(q.tblMap.columns))
	for key, col := range q.tblMap.columns {
		scanFields[key] = v.Elem().FieldByIndex(col.goIndex).Addr().Interface()
	}

	//scan the row into the struct
	err = row.Scan(scanFields...)
	if err != nil {
		if "sql: no rows in result set" == err.Error() {
			//no row found we return nil
			return nil, nil
		}
		return nil, errors.New("Error while scanning result '" + err.Error() + "'")
	}

	return v.Interface(), nil
}

//update a entity
func (a *Storm) Save(entity interface{}) error {

	v := reflect.ValueOf(entity)

	//if its a pointer we try to get the structure
	if v.Type().Kind() == reflect.Ptr {
		v = v.Elem()
	}

	tblMap := a.repository.tableMapByType(v.Type())
	if tblMap == nil {
		return errors.New("No structure registered in repository of type '" + v.Type().String() + "'")
	}

	pkCount := len(tblMap.keys)
	if pkCount == 0 {
		return errors.New("No primary key defined")
	} else if pkCount > 1 {
		return errors.New("Entities with more than 1 pk currently not suppported")
	}

	//create query
	q := NewQuery(tblMap, a)

	//add the columns
	var bindValues []interface{}
	for _, col := range q.tblMap.columns {
		//ignore pk
		if tblMap.keys[0] != col {
			q.Column(col.Name)
			bindValues = append(bindValues, v.FieldByIndex(col.goIndex).Interface())
		}
	}

	//update if pk is non zero
	var sql string
	var bind []interface{}
	pkValue := v.FieldByIndex(tblMap.keys[0].goIndex).Interface()
	if pkValue == 0 {
		//insert
		sql = q.generateInsertSQL()
	} else {
		//update

		//add pk where
		q.Where(fmt.Sprintf("%v = ?", tblMap.keys[0].Name), pkValue)
		sql, bind = q.generateUpdateSQL()
	}

	bind = append(bindValues, bind...)
	stmt, err := a.db.Prepare(sql)
	if err != nil {
		return err
	}
	defer stmt.Close()

	_, err = stmt.Exec(bind...)
	if err != nil {
		return err
	}

	return nil
}

//delete a entity
func (a *Storm) Delete(entity interface{}) error {

	v := reflect.ValueOf(entity)
	tblMap := a.repository.tableMapByType(v.Type())

	if tblMap == nil {
		return errors.New("No structure registered in repository of type '" + v.Type().String() + "'")
	}

	if len(tblMap.keys) == 0 {
		return errors.New("No primary key defined")
	}

	//create query
	q := NewQuery(tblMap, a)

	//set the where
	for _, col := range q.tblMap.keys {
		q.Where(fmt.Sprintf("`%v` = ?", col.Name), v.FieldByIndex(col.goIndex).Interface())
	}

	sql, bind := q.generateDeleteSQL()
	stmt, err := a.db.Prepare(sql)
	if err != nil {
		return err
	}
	defer stmt.Close()

	_, err = stmt.Exec(bind...)
	if err != nil {
		return err
	}

	return nil
}
