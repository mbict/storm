package storm2

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"sync"

	"github.com/mbict/storm2/dialect"
)

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

func (this *Storm) DB() *sql.DB {
	return this.db
}

func (this *Storm) Query() *Query {
	return newQuery(this, nil, nil)
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

func (this *Storm) Begin() *Query {
	return nil
}

func (this *Storm) CreateTable(i interface{}) error {
	return nil
}

func (this *Storm) DropTable(i interface{}) error {
	return nil
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
		return errors.New(fmt.Sprintf("Duplicate structure, '%v' already exists", t.String()))
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
	tbl, ok := this.getTable(v.Type())
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
	tbl, ok := this.getTable(v.Type())
	if !ok {
		return errors.New(fmt.Sprintf("No registered structure for `%s` found", v.Type().String()))
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
	return "", nil
}

func (this *Storm) generateUpdateSQL(v reflect.Value, tbl *table) (string, []interface{}) {
	return "", nil
}

func (this *Storm) generateCreateTableSQL(tbl *table) string {
	return ""
}

func (this *Storm) generateDropTableSQL(tbl *table) string {
	return ""
}

func (this *Storm) getTable(t reflect.Type) (tbl *table, ok bool) {

	this.tableLock.RLock()
	defer this.tableLock.RUnlock()

	tbl, ok = this.tables[t]
	return
}
