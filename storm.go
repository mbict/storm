package storm

import (
	"bytes"
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
	s := Storm{}
	s.repository = repository
	s.db = db

	return &s
}

//get a query stack for an entity
func (s *Storm) Query(entityName string) (*Query, error) {

	tblMap := s.repository.getTableMap(entityName)
	if tblMap == nil {
		return nil, errors.New("No entity with the name '" + entityName + "' found")
	}

	return NewQuery(tblMap, s), nil
}

//get a single entity from the datastore
func (s *Storm) Get(entityName string, keys ...interface{}) (interface{}, error) {

	q, err := s.Query(entityName)
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
	stmt, err := s.db.Prepare(sql)
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
func (s *Storm) Save(entity interface{}) error {

	v := reflect.ValueOf(entity)

	//check if the passed item is a pointer
	if v.Type().Kind() != reflect.Ptr {
		return errors.New(fmt.Sprintf("storm: passed structure is not a pointer: %v (kind=%v)", entity, v.Kind()))
	}

	v = v.Elem()
	tblMap := s.repository.tableMapByType(v.Type())
	if tblMap == nil {
		return errors.New("No structure registered in repository of type '" + v.Type().String() + "'")
	}

	pkCount := len(tblMap.keys)
	if pkCount == 0 {
		return errors.New("No primary key defined")
	} else if pkCount > 1 {
		return errors.New("Entities with more than 1 pk currently not suppported")
	}

	//update if pk is non zero
	var (
		getLastInsertId bool = false
		sql             string
		bind            []interface{}
		pkValue         int64 = v.FieldByIndex(tblMap.keys[0].goIndex).Int()
	)

	if pkValue == 0 {
		//insert
		getLastInsertId = true
		sql, bind = s.generateInsertSQL(v, tblMap)
	} else {
		//update
		sql, bind = s.generateUpdateSQL(v, tblMap)
	}

	stmt, err := s.db.Prepare(sql)
	if err != nil {
		return err
	}
	defer stmt.Close()

	//get the pk if this was a insert
	if getLastInsertId == true {
		id, err := s.repository.dialect.InsertAutoIncrement(stmt, bind...)
		if err != nil {
			return err
		}

		if v.FieldByIndex(tblMap.keys[0].goIndex).CanSet() {
			v.FieldByIndex(tblMap.keys[0].goIndex).SetInt(id)
		} else {

		}
	} else {
		_, err = stmt.Exec(bind...)
		if err != nil {
			return err
		}
	}

	return nil
}

//delete a entity
func (s *Storm) Delete(entity interface{}) error {

	v := reflect.ValueOf(entity)
	tblMap := s.repository.tableMapByType(v.Type())

	if tblMap == nil {
		return errors.New("No structure registered in repository of type '" + v.Type().String() + "'")
	}

	if len(tblMap.keys) == 0 {
		return errors.New("No primary key defined")
	}

	//execute
	sql, bind := s.generateDeleteSQL(v, tblMap)
	stmt, err := s.db.Prepare(sql)
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

// Generation of insert sql
// @todo implent dialect
func (s *Storm) generateInsertSQL(entityValue reflect.Value, tblMap *TableMap) (string, []interface{}) {
	var (
		sqlColumns bytes.Buffer
		sqlValues  bytes.Buffer
		pos        int = 0
		bind           = make([]interface{}, 0)
	)

	for _, col := range tblMap.columns {
		for _, pk := range tblMap.keys {
			if col != pk {
				if pos > 0 {
					sqlColumns.WriteString(", ")
					sqlValues.WriteString(", ")
				}

				sqlColumns.WriteString(fmt.Sprintf("`%s`", col.Name))
				sqlValues.WriteString("?")
				bind = append(bind, entityValue.FieldByIndex(col.goIndex).Interface())
				pos++
			}
		}
	}

	return fmt.Sprintf("INSERT INTO `%s`(%s) VALUES (%s)", tblMap.Name, sqlColumns.String(), sqlValues.String()), bind
}

// Generate update sql
// @todo need to implement dialect
func (s *Storm) generateUpdateSQL(entityValue reflect.Value, tblMap *TableMap) (string, []interface{}) {

	var (
		sql  bytes.Buffer
		pos  int = 0
		bind     = make([]interface{}, 0)
	)

	sql.WriteString(fmt.Sprintf("UPDATE `%s` SET ", tblMap.Name))

	for _, col := range tblMap.columns {
		for _, pk := range tblMap.keys {
			if col != pk {
				if pos > 0 {
					sql.WriteString(", ")
				}

				sql.WriteString(fmt.Sprintf("`%s` = ?", col.Name))
				bind = append(bind, entityValue.FieldByIndex(col.goIndex).Interface())
				pos++
			}
		}
	}

	sql.WriteString(" WHERE ")
	pos = 0
	for _, col := range tblMap.keys {
		if pos > 0 {
			sql.WriteString(" AND ")
		}
		sql.WriteString(fmt.Sprintf("`%s` = ?", col.Name))
		bind = append(bind, entityValue.FieldByIndex(col.goIndex).Interface())
		pos++
	}

	return sql.String(), bind
}

// Generation of delete sql
// @todo need to implement dialect
func (s *Storm) generateDeleteSQL(entityValue reflect.Value, tblMap *TableMap) (string, []interface{}) {

	var (
		sql  bytes.Buffer
		pos  int = 0
		bind     = make([]interface{}, 0)
	)

	sql.WriteString(fmt.Sprintf("DELETE FROM `%s` WHERE ", tblMap.Name))
	for _, col := range tblMap.keys {
		if pos > 0 {
			sql.WriteString(" AND ")
		}
		sql.WriteString(fmt.Sprintf("`%s` = ?", col.Name))

		bind = append(bind, entityValue.FieldByIndex(col.goIndex).Interface())
		pos++
	}

	return sql.String(), bind
}
