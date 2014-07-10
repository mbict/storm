package storm

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
)

//SortDirection indicates the sort direction used in Order
type (
	SortDirection string

	where struct {
		Statement string
		Bindings  []interface{}
	}

	order struct {
		Statement string
		Direction SortDirection
	}
)

//ASC ascending order
//DESC descending order
const (
	ASC  SortDirection = "ASC"
	DESC SortDirection = "DESC"
)

//Query structure
type Query struct {
	ctx    Context
	where  []where
	order  []order
	offset int
	limit  int
}

func newQuery(ctx Context, parent *Query) *Query {

	q := Query{
		ctx:    ctx,
		offset: -1,
		limit:  -1,
	}

	if parent != nil {
		//clone parent
		q.where = parent.where
		q.order = parent.order
		q.offset = parent.offset
		q.limit = parent.limit
	} else {
		q.where = make([]where, 0)
		q.order = make([]order, 0)
	}

	return &q

}

//Query Creates a clone of the current query object
func (query *Query) Query() *Query {
	return newQuery(query.ctx, query)
}

//Order will set the order
//Example:
// q.Order("columnnname", storm.ASC)
// q.Order("columnnname", storm.DESC)
func (query *Query) Order(column string, direction SortDirection) *Query {
	query.order = append(query.order, order{column, direction})
	return query
}

//Where adds new where conditions to the query
//Example:
// q.Where(1) //automatic uses the pk becomes id = 1
// q.Where("column = 1") //textual condition
// q.Where("column = ?", 1) //bind params
// q.Where("(column = ? OR other = ?)",1,2) //multiple bind params
func (query *Query) Where(condition string, bindAttr ...interface{}) *Query {
	query.where = append(query.where, where{condition, bindAttr})
	return query
}

//Limit sets the limit for select
func (query *Query) Limit(limit int) *Query {
	query.limit = limit
	return query
}

//Offset sets the offset for select
func (query *Query) Offset(offset int) *Query {
	query.offset = offset
	return query
}

//Find will try to retreive the matching structure/entity based on your where statement
//you can provide a slice or a single element
func (query *Query) Find(i interface{}, where ...interface{}) error {

	//slice given
	if reflect.Indirect(reflect.ValueOf(i)).Kind() == reflect.Slice {
		if len(where) >= 1 {
			return query.Query().fetchAll(i, where...)
		}
		return query.fetchAll(i)
	}

	//single context
	if len(where) >= 1 {
		return query.Query().fetchRow(i, where...)
	}
	return query.fetchRow(i)
}

//First will execute the query and return one result to i
//Example:
// var result *TestModel
// q.First(&result)
func (query *Query) First(i interface{}) error {
	return query.fetchRow(i)
}

//Count will execute a query and return the resulting rows Select will return
//Example:
// count, err := q.Count((*TestModel)(nil))
func (query *Query) Count(i interface{}) (int64, error) {
	return query.fetchCount(i)
}

//create additional where stements from arguments
func (query *Query) applyWhere(tbl *table, where ...interface{}) error {

	switch t := where[0].(type) {
	case string:
		query.Where(t, where[1:]...)
	case int, int8, int16, int32, uint, uint8, uint16, uint32, int64, uint64, sql.NullInt64:
		if len(tbl.keys) == 1 {
			if len(where) == 1 {
				query.Where(fmt.Sprintf("%s = ?", query.ctx.Dialect().Quote(tbl.keys[0].columnName)), where...)
			} else {
				return errors.New("not implemented having multiple pk values for find")
			}
		} else {
			return errors.New("not implemented having multiple pks for find")
		}
	default:
		return errors.New("unsupported pk find type")
	}

	return nil
}

//fetch a single row into a element
func (query *Query) fetchCount(i interface{}) (cnt int64, err error) {
	t := reflect.TypeOf(i)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct {
		return 0, errors.New("provided input is not a structure type")
	}

	//find the table
	tbl, ok := query.ctx.table(t)
	if !ok {
		return 0, fmt.Errorf("no registered structure for `%s` found", t)
	}

	//generate sql and prepare
	sqlQuery, bind := query.generateCountSQL(tbl)

	if query.ctx.logger() != nil {
		query.ctx.logger().Printf("`%s` binding : %v", sqlQuery, bind)
	}

	stmt, err := query.ctx.DB().Prepare(sqlQuery)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	//query the row
	row := stmt.QueryRow(bind...)

	//create destination and scan
	err = row.Scan(&cnt)
	return cnt, err
}

//fetch a single row into a element
func (query *Query) fetchRow(i interface{}, where ...interface{}) (err error) {
	v := reflect.ValueOf(i)
	if v.Kind() != reflect.Ptr {
		return errors.New("provided input is not a pointer type")
	}

	v = v.Elem()
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			v.Set(reflect.New(v.Type().Elem()))
		}
	}
	v = reflect.Indirect(v)

	if v.Kind() != reflect.Struct || !v.CanSet() {
		return errors.New("provided input is not a structure type")
	}

	//find the table
	tbl, ok := query.ctx.table(v.Type())
	if !ok {
		return fmt.Errorf("no registered structure for `%s` found", v.Type().String())
	}

	//add the last minute where
	if len(where) >= 1 {
		if err = query.applyWhere(tbl, where...); err != nil {
			return err
		}
	}

	//generate sql and prepare
	sqlQuery, bind := query.generateSelectSQL(tbl)
	if query.ctx.logger() != nil {
		query.ctx.logger().Printf("`%s` binding : %v", sqlQuery, bind)
	}

	stmt, err := query.ctx.DB().Prepare(sqlQuery)
	if err != nil {
		return err
	}
	defer stmt.Close()

	//query the row
	row := stmt.QueryRow(bind...)

	//create destination and scan
	dest := make([]interface{}, len(tbl.columns))
	for key, col := range tbl.columns {
		dest[key] = v.FieldByIndex(col.goIndex).Addr().Interface()
	}

	err = row.Scan(dest...)
	if err != nil {
		return err
	}

	return tbl.callbacks.invoke(v.Addr(), "OnInit", query.ctx)
}

//fetch a all rows into a slice
func (query *Query) fetchAll(i interface{}, where ...interface{}) (err error) {

	ts := reflect.TypeOf(i)
	if ts.Kind() != reflect.Ptr {
		return errors.New("provided input is not a pointer type")
	}

	if ts.Elem().Kind() != reflect.Slice {
		return errors.New("provided input pointer is not a slice type")
	}

	//get the element type
	t := ts.Elem().Elem()
	var sliceTypeIsPtr = false
	if t.Kind() == reflect.Ptr {
		sliceTypeIsPtr = true
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct {
		return errors.New("provided input slice has no structure type")
	}

	//find the table
	tbl, ok := query.ctx.table(t)
	if !ok {
		return fmt.Errorf("no registered structure for `%s` found", t.String())
	}

	//add the last minute where
	if len(where) >= 1 {
		if err = query.applyWhere(tbl, where...); err != nil {
			return err
		}
	}

	//generate sql and prepare
	sqlQuery, bind := query.generateSelectSQL(tbl)
	if query.ctx.logger() != nil {
		query.ctx.logger().Printf("`%s` binding : %v", sqlQuery, bind)
	}

	stmt, err := query.ctx.DB().Prepare(sqlQuery)
	if err != nil {
		return err
	}
	defer stmt.Close()

	//query for the results
	rows, err := stmt.Query(bind...)
	if err != nil {
		return err
	}
	defer rows.Close()

	vs := reflect.ValueOf(i).Elem()
	vs.SetLen(0)

	for {
		if !rows.Next() {
			// if error occured return rawselect
			if rows.Err() != nil {
				return rows.Err()
			} else if vs.Len() == 0 {
				return sql.ErrNoRows
			}
			return nil
		}

		v := reflect.New(tbl.goType)

		//create destination and scan
		dest := make([]interface{}, len(tbl.columns))
		for key, col := range tbl.columns {
			dest[key] = v.Elem().FieldByIndex(col.goIndex).Addr().Interface()
		}

		if err = rows.Scan(dest...); err != nil {
			return err
		}

		if err = tbl.callbacks.invoke(v, "OnInit", query.ctx); err != nil {
			return err
		}

		if sliceTypeIsPtr == true {
			vs.Set(reflect.Append(vs, v))
		} else {
			vs.Set(reflect.Append(vs, v.Elem()))
		}
	}
}

func (query *Query) generateSelectSQL(tbl *table) (string, []interface{}) {

	var bindVars []interface{}
	var sql bytes.Buffer
	var pos int

	sql.WriteString("SELECT ")

	//create columns
	pos = 0
	for _, col := range tbl.columns {
		if pos > 0 {
			sql.WriteString(", ")
		}
		sql.WriteString(fmt.Sprintf("`%v`", col.columnName))
		pos++
	}

	//add table name
	sql.WriteString(fmt.Sprintf(" FROM `%v`", tbl.tableName))

	//add where
	if len(query.where) > 0 {

		sql.WriteString(" WHERE ")

		//create where keys
		pos = 0
		for _, cond := range query.where {
			if pos > 0 {
				sql.WriteString(" AND ")
			}
			sql.WriteString(cond.Statement)
			bindVars = append(bindVars, cond.Bindings...)
			pos++
		}
	}

	//add order
	if len(query.order) > 0 {
		sql.WriteString(" ORDER BY ")
		pos = 0
		for _, col := range query.order {
			if pos > 0 {
				sql.WriteString(", ")
			}
			sql.WriteString(fmt.Sprintf("`%s` %s", col.Statement, col.Direction))
			pos++
		}
	}

	//add limit
	if query.limit > 0 {
		sql.WriteString(fmt.Sprintf(" LIMIT %d", query.limit))
	}

	//add offset
	if query.offset > 0 {
		sql.WriteString(fmt.Sprintf(" OFFSET %d", query.offset))
	}
	return sql.String(), bindVars
}

func (query *Query) generateCountSQL(tbl *table) (string, []interface{}) {

	var bindVars []interface{}
	var sql bytes.Buffer
	var pos int

	//add table name
	sql.WriteString(fmt.Sprintf("SELECT COUNT(*) FROM %s", query.ctx.Dialect().Quote(tbl.tableName)))

	//add where
	if len(query.where) > 0 {

		sql.WriteString(" WHERE ")

		//create where keys
		pos = 0
		for _, cond := range query.where {
			if pos > 0 {
				sql.WriteString(" AND ")
			}

			sql.WriteString(cond.Statement)
			bindVars = append(bindVars, cond.Bindings...)
			pos++
		}
	}

	return sql.String(), bindVars
}
