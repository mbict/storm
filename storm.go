package storm

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strings"
	"sync"

	"github.com/mbict/storm/dialect"
)

var RecordNotFound = sql.ErrNoRows

//Context interface for Transaction and Query
type Context interface {
	DB() sqlCommon
	Storm() *Storm
	Dialect() dialect.Dialect

	Query() *Query
	Order(column string, direction SortDirection) *Query
	Where(condition string, bindAttr ...interface{}) *Query
	Limit(limit int) *Query
	Offset(offset int) *Query
	Find(i interface{}, where ...interface{}) error
	Dependent(i interface{}, columns ...string) error
	Delete(i interface{}) error
	Save(i interface{}) error

	table(t reflect.Type) (tbl *table, ok bool)
	tableByName(s string) (tbl *table, ok bool)
	logger() *log.Logger
}

//Storm structure
type Storm struct {
	db        *sql.DB
	dialect   dialect.Dialect
	tables    map[reflect.Type]*table
	tableLock sync.RWMutex
	log       *log.Logger
}

//DB is a alias for Storm
type DB Storm

//Open opens a new connection to the datastore
func Open(driverName string, dataSourceName string) (*Storm, error) {
	db, err := sql.Open(driverName, dataSourceName)
	return &Storm{
		db:      db,
		dialect: dialect.New(driverName),
		tables:  make(map[reflect.Type]*table),
	}, err
}

//SetMaxIdleConns will the the maxiumum of idle connections
func (storm *Storm) SetMaxIdleConns(n int) {
	storm.db.SetMaxIdleConns(n)
}

//SetMaxOpenConns will the the maxiumum open connections
func (storm *Storm) SetMaxOpenConns(n int) {
	storm.db.SetMaxOpenConns(n)
}

//DB will return the current connection
func (storm *Storm) DB() sqlCommon {
	return storm.db
}

//Storm will return the storm instance
func (storm *Storm) Storm() *Storm {
	return storm
}

//Ping performs a ping to the datastore to check if we have a valid connection
func (storm *Storm) Ping() error {
	return storm.db.Ping()
}

//Dialect returns the current dialect used by the connection
func (storm *Storm) Dialect() dialect.Dialect {
	return storm.dialect
}

//Log you can assign a new logger
func (storm *Storm) Log(log *log.Logger) {
	storm.log = log
}

//Query Create a new query object
func (storm *Storm) Query() *Query {
	return newQuery(storm, nil)
}

//Order will create a new query object and set the order
func (storm *Storm) Order(column string, direction SortDirection) *Query {
	return storm.Query().Order(column, direction)
}

//Where will create a new query object and add a new where statement
func (storm *Storm) Where(condition string, bindAttr ...interface{}) *Query {
	return storm.Query().Where(condition, bindAttr...)
}

//Limit will create a new query object and set the limit
func (storm *Storm) Limit(limit int) *Query {
	return storm.Query().Limit(limit)
}

//Offset will create a new query object and set the offset
func (storm *Storm) Offset(offset int) *Query {
	return storm.Query().Offset(offset)
}

//Find will try to retreive the matching structure/entity based on your where statement
//Example:
// var row *TestModel
// s.Find(&row,1)
func (storm *Storm) Find(i interface{}, where ...interface{}) error {
	return storm.Query().Find(i, where...)
}

//Dependent will try to fetch all the related enities and populate the dependent fields (slice and single values)
//You can provide a list with column names if you only want those fields to be populated
func (storm *Storm) Dependent(i interface{}, columns ...string) error {
	return storm.Query().Dependent(i, columns...)
}

//Delete will delete the provided structure from the datastore
func (storm *Storm) Delete(i interface{}) error {
	tx := storm.Begin()
	err := storm.deleteEntity(i, tx)
	if err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

//Save will insert or update the provided structure in the datastore
func (storm *Storm) Save(i interface{}) error {
	tx := storm.Begin()
	err := storm.saveEntity(i, tx)
	if err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

//Begin will start a new transaction connection
func (storm *Storm) Begin() *Transaction {
	return newTransaction(storm)
}

//CreateTable creates new table in the datastore
func (storm *Storm) CreateTable(i interface{}) error {
	t := reflect.TypeOf(i)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct {
		return errors.New("provided input is not a structure type")
	}

	//find the table
	tbl, ok := storm.table(t)
	if !ok {
		return fmt.Errorf("no registered structure for `%s` found", t)
	}

	sqlCreateTable := storm.generateCreateTableSQL(tbl)
	if storm.log != nil {
		storm.log.Println(sqlCreateTable)
	}

	_, err := storm.db.Exec(sqlCreateTable)
	return err
}

//DropTable removes the table in the datastore
func (storm *Storm) DropTable(i interface{}) error {

	t := reflect.TypeOf(i)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct {
		return errors.New("provided input is not a structure type")
	}

	//find the table
	tbl, ok := storm.table(t)
	if !ok {
		return fmt.Errorf("no registered structure for `%s` found", t)
	}

	sqlDropTable := storm.generateDropTableSQL(tbl)
	if storm.log != nil {
		storm.log.Println(sqlDropTable)
	}

	_, err := storm.db.Exec(sqlDropTable)
	return err
}

//RegisterStructure will parse the provided structure and links it to a table in the datastore
func (storm *Storm) RegisterStructure(i interface{}) error {

	t := reflect.TypeOf(i)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct {
		return errors.New("provided input is not a structure type")
	}

	storm.tableLock.Lock()
	defer storm.tableLock.Unlock()
	if _, exists := storm.tables[t]; exists == true {

		return fmt.Errorf("duplicate structure, '%s' already exists", t)
	}

	storm.tables[t] = newTable(reflect.New(t))
	storm.resolveRelations()

	return nil
}

//
func (storm *Storm) resolveRelations() error {
	for _, tbl := range storm.tables {
		for _, rel := range tbl.relations {

			//skip already found relations
			if rel.relTable != nil || rel.relColumn != nil {
				continue
			}

			//find related columns One To One
			colName := rel.name + "_id"
			for _, relCol := range tbl.columns {
				if strings.EqualFold(relCol.columnName, colName) {
					rel.relColumn = relCol
					break
				}
			}

			//find related columns One To Many
			if relTbl, ok := storm.tables[rel.goSingularType]; ok == true {
				for _, relCol := range relTbl.columns {
					if relCol.columnName == tbl.tableName+"_id" {
						//found a relation (slice type)
						rel.relTable = relTbl
						rel.relColumn = relCol
						break
					}
				}
			}

			//Todo: many to many resolve
		}
	}
	return nil
}

func (storm *Storm) deleteEntity(i interface{}, tx *Transaction) (err error) {
	v := reflect.ValueOf(i)
	if v.Kind() != reflect.Ptr {
		return errors.New("provided input is not by reference")
	}

	v = v.Elem()
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return errors.New("provided input is a nil pointer")
		}
	}
	v = reflect.Indirect(v)

	if v.Kind() != reflect.Struct || !v.CanSet() {
		return errors.New("provided input is not a structure type")
	}

	//find the table
	tbl, ok := storm.table(v.Type())
	if !ok {
		return fmt.Errorf("no registered structure for `%s` found", v.Type())
	}

	sqlDelete, bind := storm.generateDeleteSQL(v, tbl)
	if storm.log != nil {
		storm.log.Printf("`%s` binding : %v", sqlDelete, bind)
	}

	err = tbl.callbacks.invoke(v.Addr(), "OnDelete", tx)
	if err != nil {
		return err
	}

	_, err = tx.DB().Exec(sqlDelete, bind...)
	if err != nil {
		return err
	}

	return tbl.callbacks.invoke(v.Addr(), "OnPostDelete", tx)
}

func (storm *Storm) saveEntity(i interface{}, tx *Transaction) (err error) {

	v := reflect.ValueOf(i)
	if v.Kind() != reflect.Ptr {
		return errors.New("provided input is not by reference")
	}

	v = v.Elem()
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return errors.New("provided input is a nil pointer")
		}
	}
	v = reflect.Indirect(v)

	if v.Kind() != reflect.Struct || !v.CanSet() {
		return errors.New("provided input is not a structure type")
	}

	//find the table
	tbl, ok := storm.table(v.Type())
	if !ok {
		return fmt.Errorf("no registered structure for `%s` found", v.Type())
	}

	var (
		sqlQuery string
		bind     []interface{}
	)

	if tbl.aiColumn != nil {
		insert := v.FieldByIndex(tbl.aiColumn.goIndex).Int() == 0
		if insert == true {
			//insert
			err = tbl.callbacks.invoke(v.Addr(), "OnInsert", tx)
			sqlQuery, bind = storm.generateInsertSQL(v, tbl)
		} else {
			err = tbl.callbacks.invoke(v.Addr(), "OnUpdate", tx)
			sqlQuery, bind = storm.generateUpdateSQL(v, tbl)
		}

		//no errors on the before callbacks
		if err != nil {
			return err
		}

		//prepare
		stmt, err := tx.DB().Prepare(sqlQuery)
		if err != nil {
			return err
		}
		defer stmt.Close()

		if storm.log != nil {
			storm.log.Printf("`%s` binding : %v", sqlQuery, bind)
		}

		if insert == true {
			var id int64
			id, err = storm.dialect.InsertAutoIncrement(stmt, bind...)
			v.FieldByIndex(tbl.aiColumn.goIndex).SetInt(id)
			if err != nil {
				return err
			}
			err = tbl.callbacks.invoke(v.Addr(), "OnPostInsert", tx)
		} else {
			_, err = stmt.Exec(bind...)
			if err != nil {
				return err
			}
			err = tbl.callbacks.invoke(v.Addr(), "OnPostUpdate", tx)
		}
		return err
	}
	return errors.New("no PK auto increment field defined dont know yet if to update or insert")
}

func (storm *Storm) generateDeleteSQL(v reflect.Value, tbl *table) (string, []interface{}) {
	var (
		sqlQuery bytes.Buffer
		pos      int
		bind     = make([]interface{}, 0)
	)

	sqlQuery.WriteString(fmt.Sprintf("DELETE FROM `%s` WHERE ", tbl.tableName))
	for _, col := range tbl.keys {
		if pos > 0 {
			sqlQuery.WriteString(" AND ")
		}
		sqlQuery.WriteString(fmt.Sprintf("`%s` = ?", col.columnName))

		bind = append(bind, v.FieldByIndex(col.goIndex).Interface())
		pos++
	}

	return sqlQuery.String(), bind
}

func (storm *Storm) generateInsertSQL(v reflect.Value, tbl *table) (string, []interface{}) {
	var (
		sqlColumns bytes.Buffer
		sqlValues  bytes.Buffer
		pos        int
		bind       = make([]interface{}, 0)
	)

	for _, col := range tbl.columns {
		if col != tbl.aiColumn {
			if pos > 0 {
				sqlColumns.WriteString(", ")
				sqlValues.WriteString(", ")
			}

			sqlColumns.WriteString(fmt.Sprintf("`%s`", col.columnName))
			sqlValues.WriteString("?")
			bind = append(bind, v.FieldByIndex(col.goIndex).Interface())
			pos++
		}
	}

	return fmt.Sprintf("INSERT INTO `%s` (%s) VALUES (%s)", tbl.tableName, sqlColumns.String(), sqlValues.String()), bind
}

func (storm *Storm) generateUpdateSQL(v reflect.Value, tbl *table) (string, []interface{}) {
	var (
		sqlQuery bytes.Buffer
		pos      int
		bind     = make([]interface{}, 0)
	)

	sqlQuery.WriteString(fmt.Sprintf("UPDATE `%s` SET ", tbl.tableName))

	for _, col := range tbl.columns {
		if col != tbl.aiColumn {
			if pos > 0 {
				sqlQuery.WriteString(", ")
			}

			sqlQuery.WriteString(fmt.Sprintf("`%s` = ?", col.columnName))
			bind = append(bind, v.FieldByIndex(col.goIndex).Interface())
			pos++
		}
	}

	sqlQuery.WriteString(" WHERE ")
	pos = 0

	if tbl.aiColumn != nil {
		sqlQuery.WriteString(fmt.Sprintf("`%s` = ?", tbl.aiColumn.columnName))
		bind = append(bind, v.FieldByIndex(tbl.aiColumn.goIndex).Interface())
	} else {
		for _, col := range tbl.keys {
			if pos > 0 {
				sqlQuery.WriteString(" AND ")
			}
			sqlQuery.WriteString(fmt.Sprintf("`%s` = ?", col.columnName))
			bind = append(bind, v.FieldByIndex(col.goIndex).Interface())
			pos++
		}
	}
	return sqlQuery.String(), bind
}

func (storm *Storm) generateCreateTableSQL(tbl *table) string {
	var columns []string
	for _, col := range tbl.columns {
		column := reflect.Zero(col.goType).Interface()
		params := ""
		if tbl.aiColumn == col {
			params = " " + storm.dialect.SqlPrimaryKey(column, 0)
		}

		columns = append(columns, storm.dialect.Quote(col.columnName)+" "+storm.dialect.SqlType(column, 0)+params)
	}

	return fmt.Sprintf("CREATE TABLE %s (%s)", storm.dialect.Quote(tbl.tableName), strings.Join(columns, ","))
}

func (storm *Storm) generateDropTableSQL(tbl *table) string {
	return fmt.Sprintf("DROP TABLE %s", storm.dialect.Quote(tbl.tableName))
}

//find a table
func (storm *Storm) table(t reflect.Type) (tbl *table, ok bool) {

	storm.tableLock.RLock()
	defer storm.tableLock.RUnlock()

	tbl, ok = storm.tables[t]
	return
}

//find table by name
func (storm *Storm) tableByName(s string) (tbl *table, ok bool) {
	storm.tableLock.RLock()
	defer storm.tableLock.RUnlock()
	for _, tbl := range storm.tables {
		if strings.EqualFold(tbl.tableName, s) {
			return tbl, true
		}
	}
	return nil, false
}

func (storm *Storm) logger() *log.Logger {
	return storm.log
}
