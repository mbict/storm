package storm

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
)

type SortDirection string

const (
	ASC  SortDirection = "ASC"
	DESC SortDirection = "DESC"
)

type Query struct {
	ctx    Context
	where  map[string][]interface{}
	order  map[string]SortDirection
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
		q.where = make(map[string][]interface{}, len(parent.where))
		for k, v := range parent.where {
			q.where[k] = v
		}

		q.order = make(map[string]SortDirection, len(parent.order))
		for k, v := range parent.order {
			q.order[k] = v
		}
		q.offset = parent.offset
		q.limit = parent.limit
	} else {
		q.where = make(map[string][]interface{}, 0)
		q.order = make(map[string]SortDirection, 0)
	}

	return &q

}

//Returns a new query object
func (this *Query) Query() *Query {
	return newQuery(this.ctx, this)
}

func (this *Query) Order(column string, direction SortDirection) *Query {
	this.order[column] = direction
	return this
}

func (this *Query) Where(condition string, bindAttr ...interface{}) *Query {
	this.where[condition] = bindAttr
	return this
}

func (this *Query) Limit(limit int) *Query {
	this.limit = limit
	return this
}

func (this *Query) Offset(offset int) *Query {
	this.offset = offset
	return this
}

func (this *Query) Find(i interface{}, where ...interface{}) error {

	if len(where) >= 1 {
		return this.Query().fetchRow(i, where...)
	}
	return this.fetchRow(i)
}

func (this *Query) Select(i interface{}) error {
	return this.fetchAll(i)
}

func (this *Query) SelectRow(i interface{}) error {
	return this.Find(i)
}

func (this *Query) Count(i interface{}) (int64, error) {
	return this.fetchCount(i)
}

//create additional where stements from arguments
func (this *Query) applyWhere(tbl *table, where ...interface{}) error {

	switch t := where[0].(type) {
	case string:
		this.Where(t, where[1:]...)
	case int, int8, int16, int32, uint, uint8, uint16, uint32, int64, uint64, sql.NullInt64:

		if len(tbl.keys) == 1 {
			if len(where) == 1 {
				this.Where(fmt.Sprintf("%s = ?", this.ctx.Dialect().Quote(tbl.keys[0].columnName)), where...)
			} else {
				return errors.New("Not implemented having multiple pk values for find")
			}
		} else {
			return errors.New("Not implemented having multiple pks for find")
		}
	default:
		return errors.New("Unsupported pk find type")
	}

	return nil
}

//fetch a single row into a element
func (this *Query) fetchCount(i interface{}) (cnt int64, err error) {
	t := reflect.TypeOf(i)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct {
		return 0, errors.New("Provided input is not a structure type")
	}

	//find the table
	tbl, ok := this.ctx.table(t)
	if !ok {
		return 0, errors.New(fmt.Sprintf("No registered structure for `%s` found", t))
	}

	//generate sql and prepare
	sqlQuery, bind := this.generateCountSQL(tbl)

	if this.ctx.logger() != nil {
		this.ctx.logger().Printf("`%s` binding : %v", sqlQuery, bind)
	}

	stmt, err := this.ctx.DB().Prepare(sqlQuery)
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
func (this *Query) fetchRow(i interface{}, where ...interface{}) (err error) {
	v := reflect.ValueOf(i)
	if v.Kind() != reflect.Ptr {
		return errors.New("Provided input is not a pointer type")
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
	tbl, ok := this.ctx.table(v.Type())
	if !ok {
		return errors.New(fmt.Sprintf("No registered structure for `%s` found", v.Type().String()))
	}

	//add the last minute where
	if len(where) >= 1 {
		if err = this.applyWhere(tbl, where...); err != nil {
			return err
		}
	}

	//generate sql and prepare
	sqlQuery, bind := this.generateSelectSQL(tbl)
	if this.ctx.logger() != nil {
		this.ctx.logger().Printf("`%s` binding : %v", sqlQuery, bind)
	}

	stmt, err := this.ctx.DB().Prepare(sqlQuery)
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

	return tbl.callbacks.invoke(v.Addr(), "OnInit", this.ctx)
}

//fetch a single row into a element
func (this *Query) fetchAll(i interface{}) error {

	ts := reflect.TypeOf(i)
	if ts.Kind() != reflect.Ptr {
		return errors.New("Provided input is not a pointer type")
	}

	if ts.Elem().Kind() != reflect.Slice {
		return errors.New("Provided input pointer is not a slice type")
	}

	//get the element type
	t := ts.Elem().Elem()
	var sliceTypeIsPtr = false
	if t.Kind() == reflect.Ptr {
		sliceTypeIsPtr = true
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct {
		return errors.New("Provided input slice has no structure type")
	}

	//find the table
	tbl, ok := this.ctx.table(t)
	if !ok {
		return errors.New(fmt.Sprintf("No registered structure for `%s` found", t.String()))
	}

	//generate sql and prepare
	sqlQuery, bind := this.generateSelectSQL(tbl)
	if this.ctx.logger() != nil {
		this.ctx.logger().Printf("`%s` binding : %v", sqlQuery, bind)
	}

	stmt, err := this.ctx.DB().Prepare(sqlQuery)
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

		if err = tbl.callbacks.invoke(v, "OnInit", this.ctx); err != nil {
			return err
		}

		if sliceTypeIsPtr == true {
			vs.Set(reflect.Append(vs, v))
		} else {
			vs.Set(reflect.Append(vs, v.Elem()))
		}
	}
}

func (this *Query) generateSelectSQL(tbl *table) (string, []interface{}) {

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
	if len(this.where) > 0 {

		sql.WriteString(" WHERE ")

		//create where keys
		pos = 0
		for cond, attr := range this.where {
			if pos > 0 {
				sql.WriteString(" AND ")
			}
			sql.WriteString(cond)

			bindVars = append(bindVars, attr...)
			pos++
		}
	}

	//add order
	if len(this.order) > 0 {
		sql.WriteString(" ORDER BY ")
		pos = 0
		for col, dir := range this.order {
			if pos > 0 {
				sql.WriteString(", ")
			}
			sql.WriteString(fmt.Sprintf("`%s` %s", col, dir))
			pos++
		}
	}

	//add limit
	if this.limit > 0 {
		sql.WriteString(fmt.Sprintf(" LIMIT %d", this.limit))
	}

	//add offset
	if this.offset > 0 {
		sql.WriteString(fmt.Sprintf(" OFFSET %d", this.offset))
	}

	return sql.String(), bindVars
}

func (this *Query) generateCountSQL(tbl *table) (string, []interface{}) {

	var bindVars []interface{}
	var sql bytes.Buffer
	var pos int

	//add table name
	sql.WriteString(fmt.Sprintf("SELECT COUNT(*) FROM %s", this.ctx.Dialect().Quote(tbl.tableName)))

	//add where
	if len(this.where) > 0 {

		sql.WriteString(" WHERE ")

		//create where keys
		pos = 0
		for cond, attr := range this.where {
			if pos > 0 {
				sql.WriteString(" AND ")
			}
			sql.WriteString(cond)

			bindVars = append(bindVars, attr...)
			pos++
		}
	}

	return sql.String(), bindVars
}
