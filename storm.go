package storm

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/mbict/storm/dialect"
)

type context interface {
	DB() sqlCommon
	Dialect() dialect.Dialect
	table(t reflect.Type) (tbl *table, ok bool)
}

type Storm struct {
	db        *sql.DB
	dialect   dialect.Dialect
	tables    map[reflect.Type]*table
	tableLock sync.RWMutex
}

func Open(driverName string, dataSourceName string) (*Storm, error) {
	db, err := sql.Open(driverName, dataSourceName)
	return &Storm{
		db:      db,
		dialect: dialect.New(driverName),
		tables:  make(map[reflect.Type]*table),
	}, err
}


//get the connection context
func (this *Storm) DB() sqlCommon {
	return this.db
}

//get the current dialect used by the connection
func (this *Storm) Dialect() dialect.Dialect {
	return this.dialect
}

func (this *Storm) Query() *Query {
	return newQuery(this, nil)
}

func (this *Storm) Order(column string, direction SortDirection) *Query {
	return this.Query().Order(column, direction)
}

func (this *Storm) Where(condition string, bindAttr ...interface{}) *Query {
	return this.Query().Where(condition, bindAttr...)
}

func (this *Storm) Limit(limit int) *Query {
	return this.Query().Limit(limit)
}

func (this *Storm) Offset(offset int) *Query {
	return this.Query().Offset(offset)
}

func (this *Storm) Find(i interface{}, where ...interface{}) error {
	return this.Query().Find(i, where...)
}

func (this *Storm) Delete(i interface{}) error {
	return this.deleteEntity(i, this.db)
}

func (this *Storm) Save(i interface{}) error {
	return this.saveEntity(i, this.db)
}

func (this *Storm) Begin() *Transaction {
	return newTransaction(this)
}

func (this *Storm) CreateTable(i interface{}) error {
	t := reflect.TypeOf(i)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct {
		return errors.New("Provided input is not a structure type")
	}

	//find the table
	tbl, ok := this.table(t)
	if !ok {
		return errors.New(fmt.Sprintf("No registered structure for `%s` found", t))
	}

	_, err := this.db.Exec(this.generateCreateTableSQL(tbl))
	return err
}

func (this *Storm) DropTable(i interface{}) error {

	t := reflect.TypeOf(i)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct {
		return errors.New("Provided input is not a structure type")
	}

	//find the table
	tbl, ok := this.table(t)
	if !ok {
		return errors.New(fmt.Sprintf("No registered structure for `%s` found", t))
	}

	_, err := this.db.Exec(this.generateDropTableSQL(tbl))
	return err
}

func (this *Storm) RegisterStructure(i interface{}, name string) error {

	t := reflect.TypeOf(i)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct {
		return errors.New("Provided input is not a structure type")
	}

	this.tableLock.Lock()
	defer this.tableLock.Unlock()

	if _, exists := this.tables[t]; exists == true {
		return errors.New(fmt.Sprintf("Duplicate structure, '%s' already exists", t))
	}

	this.tables[t] = newTable(reflect.Zero(t), name)
	return nil
}

//helpers

func (this *Storm) deleteEntity(i interface{}, db sqlCommon) (err error) {
	v := reflect.Indirect(reflect.ValueOf(i))
	if v.Kind() != reflect.Struct {
		return errors.New("Provided input is not a structure type")
	}

	//find the table
	tbl, ok := this.table(v.Type())
	if !ok {
		return errors.New(fmt.Sprintf("No registered structure for `%s` found", v.Type()))
	}

	deleteSql, bind := this.generateDeleteSQL(v, tbl)

	_, err = db.Exec(deleteSql, bind...)
	return err
}

func (this *Storm) saveEntity(i interface{}, db sqlCommon) error {

	v := reflect.ValueOf(i)
	if v.Kind() != reflect.Ptr {
		return errors.New("Provided structure is not a pointer type")
	}

	v = v.Elem()
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			v.Set(reflect.New(v.Type().Elem()))
		}
	}
	v = reflect.Indirect(v)

	if v.Kind() != reflect.Struct || !v.CanSet() {
		return errors.New("Provided input is not a structure type")
	}

	//find the table
	tbl, ok := this.table(v.Type())
	if !ok {
		return errors.New(fmt.Sprintf("No registered structure for `%s` found", v.Type()))
	}

	var (
		sqlQuery string
		bind     []interface{}
	)

	if tbl.aiColumn != nil {
		var insert bool = v.FieldByIndex(tbl.aiColumn.goIndex).Int() == 0
		if insert == true {
			//insert
			sqlQuery, bind = this.generateInsertSQL(v, tbl)
		} else {
			sqlQuery, bind = this.generateUpdateSQL(v, tbl)
		}

		//prepare
		stmt, err := db.Prepare(sqlQuery)
		if err != nil {
			return err
		}
		defer stmt.Close()

		if insert == true {
			id, err := this.dialect.InsertAutoIncrement(stmt, bind...)
			v.FieldByIndex(tbl.aiColumn.goIndex).SetInt(id)
			return err
		} else {
			_, err := stmt.Exec(bind...)
			return err
		}
	} else {
		return errors.New("No PK auto increment field defined dont know yet if to update or insert")
	}
}

func (this *Storm) generateDeleteSQL(v reflect.Value, tbl *table) (string, []interface{}) {
	var (
		sqlQuery bytes.Buffer
		pos      int = 0
		bind         = make([]interface{}, 0)
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

func (this *Storm) generateInsertSQL(v reflect.Value, tbl *table) (string, []interface{}) {
	var (
		sqlColumns bytes.Buffer
		sqlValues  bytes.Buffer
		pos        int = 0
		bind           = make([]interface{}, 0)
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

func (this *Storm) generateUpdateSQL(v reflect.Value, tbl *table) (string, []interface{}) {
	var (
		sqlQuery bytes.Buffer
		pos      int = 0
		bind         = make([]interface{}, 0)
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

func (this *Storm) generateCreateTableSQL(tbl *table) string {
	var columns []string
	for _, col := range tbl.columns {
		column := reflect.Zero(col.goType).Interface()
		params := ""
		if tbl.aiColumn == col {
			params = " " + this.dialect.SqlPrimaryKey(column, 0)
		}

		columns = append(columns, this.dialect.Quote(col.columnName)+" "+this.dialect.SqlType(column, 0)+params)
	}

	return fmt.Sprintf("CREATE TABLE %s (%s)", this.dialect.Quote(tbl.tableName), strings.Join(columns, ","))
}

func (this *Storm) generateDropTableSQL(tbl *table) string {
	return fmt.Sprintf("DROP TABLE %s", this.dialect.Quote(tbl.tableName))
}

func (this *Storm) table(t reflect.Type) (tbl *table, ok bool) {

	this.tableLock.RLock()
	defer this.tableLock.RUnlock()

	tbl, ok = this.tables[t]
	return
}
