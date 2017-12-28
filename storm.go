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

var TAG = "db"

var (
	ErrNotStructureType = errors.New("interface is not of type structure")
	ErrRecordNotFound   = sql.ErrNoRows
)

type CRUD interface {
	Delete(i interface{}) error
	Save(i interface{}) error
}

type Storm interface {
	dbContext
	Query
	CRUD

	Begin() (Transaction, error)

	DB() *sql.DB
	Close() error
	Register(i interface{}) error
	SetLogger(log *log.Logger)
}

//DB is a alias for Storm
type DB = Storm

//Storm structure
type storm struct {
	_db       *sql.DB
	_dialect  dialect.Dialect
	tables    schemes
	tableLock sync.RWMutex
	log       *log.Logger

	namingStrategy NamingStrategy
}


//Open opens a new connection to the datastore
func Open(driverName string, dataSourceName string) (Storm, error) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		return nil, err
	}

	return New(db, driverName), nil
}

//New creates a new storm instance
func New(db *sql.DB, driverName string) Storm {
	return &storm{
		_db:      db,
		_dialect: dialect.New(driverName),
		tables:   make(schemes),

		namingStrategy: DefaultNamingStrategy,
	}
}

/*****************************************
  Implementation Storm interface
 *****************************************/

//DB will return the current connection
func (s *storm) DB() *sql.DB {
	return s._db
}

//close database connection
func (s *storm) Close() error {
	return s._db.Close()
}

//Register will parse the provided structure and links it to a table in the datastore
func (s *storm) Register(model interface{}) error {

	t := indirect(reflect.TypeOf(model))
	if t.Kind() != reflect.Struct {
		return ErrNotStructureType
	}
	return s.tables.add(t)
}

//Begin will start a new transaction connection
func (s *storm) Begin() (Transaction, error) {
	tx, err := s._db.Begin()
	if err != nil {
		return nil, err
	}
	return newTransaction(s, tx), nil
}

/*****************************************
  Implementation QueryBuilder interface
 *****************************************/

//Query Create a new query object
func (s *storm) Query() Query {
	return newQuery(s, nil)
}

//Order will create a new query object and set the order
func (s *storm) Order(column string, direction SortDirection) Query {
	return s.Query().Order(column, direction)
}

//Where will create a new query object and add a new where statement
func (s *storm) Where(condition string, bindAttr ...interface{}) Query {
	return s.Query().Where(condition, bindAttr...)
}

//Limit will create a new query object and set the limit
func (s *storm) Limit(limit int) Query {
	return s.Query().Limit(limit)
}

//Offset will create a new query object and set the offset
func (s *storm) Offset(offset int) Query {
	return s.Query().Offset(offset)
}

func (s *storm) FetchRelated(columns ...string) Query {
	return s.Query().FetchRelated(columns...)
}

func (s *storm) Count(i interface{}) (int64, error) {
	return s.Query().Count(i)
}

func (s *storm) First(i interface{}) error {
	return s.Query().First(i)
}

//Find will try to retreive the matching structure/entity based on your where statement
//Example:
// var row *TestModel
// s.Find(&row,1)
func (s *storm) Find(i interface{}, where ...interface{}) error {
	return s.Query().Find(i, where...)
}

//Dependent will try to fetch all the related enities and populate the dependent fields (slice and single values)
//You can provide a list with column names if you only want those fields to be populated
func (s *storm) FindRelated(i interface{}, columns ...string) error {
	return s.Query().FindRelated(i, columns...)
}

/*****************************************
  Implementation CRUD interface
 *****************************************/
//Delete will delete the provided structure from the datastore
func (s *storm) Delete(i interface{}) error {
	tx, err := s.Begin()
	if err != nil {
		return err
	}

	if err := s.deleteEntity(i, tx); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

//Save will insert or update the provided structure in the datastore
func (s *storm) Save(i interface{}) error {
	tx, err := s.Begin()
	if err != nil {
		return err
	}

	if err := s.saveEntity(i, tx); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

/*****************************************
  Helper functions for creating
 *****************************************/

//CreateTable creates new table in the datastore
func (s *storm) CreateTable(i interface{}) error {
	t := reflect.TypeOf(i)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct {
		return errors.New("provided input is not a structure type")
	}

	//find the table
	tbl, ok := s.table(t)
	if !ok {
		return fmt.Errorf("no registered structure for `%s` found", t)
	}

	sqlCreateTable := s.generateCreateTableSQL(tbl)
	if s.log != nil {
		s.log.Println(sqlCreateTable)
	}

	_, err := s._db.Exec(sqlCreateTable)
	return err
}

//DropTable removes the table in the datastore
func (s *storm) DropTable(i interface{}) error {

	t := reflect.TypeOf(i)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct {
		return errors.New("provided input is not a structure type")
	}

	//find the table
	tbl, ok := s.table(t)
	if !ok {
		return fmt.Errorf("no registered structure for `%s` found", t)
	}

	sqlDropTable := s.generateDropTableSQL(tbl)
	if s.log != nil {
		s.log.Println(sqlDropTable)
	}

	_, err := s._db.Exec(sqlDropTable)
	return err
}

func (s *storm) deleteEntity(i interface{}, tx Transaction) (err error) {
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
	tbl, ok := s.table(v.Type())
	if !ok {
		return fmt.Errorf("no registered structure for `%s` found", v.Type())
	}

	sqlDelete, bind := s.generateDeleteSQL(v, tbl)
	if s.log != nil {
		s.log.Printf("`%s` binding : %v", sqlDelete, bind)
	}

	/* @todo implement dbContext */
	if cb, ok := v.Addr().Interface().(OnDeleteCallback); ok {
		if err := cb.OnDelete(nil, v.Addr().Interface()); err != nil {
			return err
		}
	}

	_, err = tx.DB().Exec(sqlDelete, bind...)
	if err != nil {
		return err
	}

	/* @todo implement dbContext */
	if cb, ok := v.Addr().Interface().(OnPostDeleteCallback); ok {
		return cb.OnPostDelete(nil, v.Addr().Interface())
	}
	return nil
}

func (s *storm) saveEntity(i interface{}, tx Transaction) (err error) {

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
	tbl, ok := s.table(v.Type())
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

			/* @todo implement dbContext */
			if cb, ok := v.Addr().Interface().(OnInsertCallback); ok {
				if err := cb.OnInsert(nil, v.Addr().Interface()); err != nil {
					return err
				}
			}

			sqlQuery, bind = s.generateInsertSQL(v, tbl)
		} else {
			/* @todo implement dbContext */
			if cb, ok := v.Addr().Interface().(OnUpdateCallback); ok {
				if err := cb.OnUpdate(nil, v.Addr().Interface()); err != nil {
					return err
				}
			}
			sqlQuery, bind = s.generateUpdateSQL(v, tbl)
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

		if s.log != nil {
			s.log.Printf("`%s` binding : %v", sqlQuery, bind)
		}

		if insert == true {
			var id int64
			id, err = s._dialect.InsertAutoIncrement(stmt, bind...)
			v.FieldByIndex(tbl.aiColumn.goIndex).SetInt(id)
			if err != nil {
				return err
			}

			/* @todo implement dbContext */
			if cb, ok := v.Addr().Interface().(OnPostInsertCallback); ok {
				return cb.OnPostInsert(nil, v.Addr().Interface())
			}
		} else {
			_, err = stmt.Exec(bind...)
			if err != nil {
				return err
			}
			/* @todo implement dbContext */
			if cb, ok := v.Addr().Interface().(OnPostUpdateCallback); ok {
				return cb.OnPostUpdate(nil, v.Addr().Interface())
			}
		}
		return err
	}
	return errors.New("no PK auto increment field defined dont know yet if to update or insert")
}

func (s *storm) generateDeleteSQL(v reflect.Value, tbl *schema) (string, []interface{}) {
	var (
		sqlQuery bytes.Buffer
		pos      int
		bind     = make([]interface{}, 0)
	)

	sqlQuery.WriteString(fmt.Sprintf("DELETE FROM `%s` WHERE ", tbl.name))
	for _, col := range tbl.keys {
		if pos > 0 {
			sqlQuery.WriteString(" AND ")
		}
		sqlQuery.WriteString(fmt.Sprintf("`%s` = ?", col.name))

		bind = append(bind, v.FieldByIndex(col.goIndex).Interface())
		pos++
	}

	return sqlQuery.String(), bind
}

func (s *storm) generateInsertSQL(v reflect.Value, tbl *schema) (string, []interface{}) {
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

			sqlColumns.WriteString(fmt.Sprintf("`%s`", col.name))
			sqlValues.WriteString("?")
			bind = append(bind, v.FieldByIndex(col.goIndex).Interface())
			pos++
		}
	}

	return fmt.Sprintf("INSERT INTO `%s` (%s) VALUES (%s)", tbl.name, sqlColumns.String(), sqlValues.String()), bind
}

func (s *storm) generateUpdateSQL(v reflect.Value, tbl *schema) (string, []interface{}) {
	var (
		sqlQuery bytes.Buffer
		pos      int
		bind     = make([]interface{}, 0)
	)

	sqlQuery.WriteString(fmt.Sprintf("UPDATE `%s` SET ", tbl.name))

	for _, col := range tbl.columns {
		if col != tbl.aiColumn {
			if pos > 0 {
				sqlQuery.WriteString(", ")
			}

			sqlQuery.WriteString(fmt.Sprintf("`%s` = ?", col.name))
			bind = append(bind, v.FieldByIndex(col.goIndex).Interface())
			pos++
		}
	}

	sqlQuery.WriteString(" WHERE ")
	pos = 0

	if tbl.aiColumn != nil {
		sqlQuery.WriteString(fmt.Sprintf("`%s` = ?", tbl.aiColumn.name))
		bind = append(bind, v.FieldByIndex(tbl.aiColumn.goIndex).Interface())
	} else {
		for _, col := range tbl.keys {
			if pos > 0 {
				sqlQuery.WriteString(" AND ")
			}
			sqlQuery.WriteString(fmt.Sprintf("`%s` = ?", col.name))
			bind = append(bind, v.FieldByIndex(col.goIndex).Interface())
			pos++
		}
	}
	return sqlQuery.String(), bind
}

func (s *storm) generateCreateTableSQL(tbl *schema) string {
	var columns []string
	for _, col := range tbl.columns {
		column := reflect.Zero(col.goType).Interface()
		params := ""
		if tbl.aiColumn == col {
			params = " " + s._dialect.SqlPrimaryKey(column, 0)
		}
		columns = append(columns, s._dialect.Quote(col.name)+" "+s._dialect.SqlType(column, 0)+params)
	}

	return fmt.Sprintf("CREATE TABLE %s (%s)", s._dialect.Quote(tbl.name), strings.Join(columns, ","))
}

func (s *storm) generateDropTableSQL(tbl *schema) string {
	return fmt.Sprintf("DROP TABLE %s", s._dialect.Quote(tbl.name))
}

//find a table
func (s *storm) table(t reflect.Type) (tbl *schema, ok bool) {
	s.tableLock.RLock()
	defer s.tableLock.RUnlock()

	tbl, err := s.tables.find(t)
	return tbl, err == nil
}

//find table by name
func (s *storm) tableByName(name string) (*schema, bool) {
	s.tableLock.RLock()
	defer s.tableLock.RUnlock()
	tbl, err := s.tables.findByName(name)
	return tbl, err == nil
}

//Storm will return the storm instance
func (s *storm) storm() Storm {
	return s
}

func (s *storm) db() sqlCommon {
	return s._db
}

//Dialect returns the current _dialect used by the connection
func (s *storm) dialect() dialect.Dialect {
	return s._dialect
}

//Log you can assign a new logger
func (s *storm) SetLogger(log *log.Logger) {
	s.log = log
}

func (s *storm) logger() *log.Logger {
	return s.log
}
