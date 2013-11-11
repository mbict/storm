package storm

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	//"database/sql"
)

type SortDirection string

const (
	ASC  SortDirection = "ASC"
	DESC SortDirection = "DESC"
)

type QueryInterface interface {
	Column(columnNames ...string) *QueryInterface
	Order(column string, direction SortDirection) *QueryInterface
	Where(condition string, bindAttr ...interface{}) *QueryInterface
	Limit(limit int) *QueryInterface
	Offset(offset int) *QueryInterface
	Select(i interface{}) ([]interface{}, error)
	Count() (int64, error)
}

type Query struct {
	tblMap *TableMap
	storm  *Storm

	columns []string
	where   map[string][]interface{}
	order   map[string]SortDirection
	offset  int
	limit   int
}

func NewQuery(tblMap *TableMap, connection *Storm) *Query {
	q := &Query{
		tblMap: tblMap,
		storm:  connection,
	}

	//init
	q.order = make(map[string]SortDirection)
	q.where = make(map[string][]interface{})

	return q
}

//
func (q *Query) Column(columnNames ...string) *Query {
	if len(columnNames) > 0 {
		q.columns = append(q.columns, columnNames...)
	}
	return q
}

//
func (q *Query) Limit(limit int) *Query {
	q.limit = limit
	return q
}

//
func (q *Query) Offset(offset int) *Query {
	q.offset = offset
	return q
}

// Set the order
func (q *Query) Order(column string, direction SortDirection) *Query {
	q.order[column] = direction
	return q
}

//
func (q *Query) Where(condition string, bindAttr ...interface{}) *Query {
	q.where[condition] = bindAttr
	return q
}

//Selectute a select into a slice structure
func (q *Query) Select(i interface{}) ([]interface{}, error) {

	var destIsPointer bool = false
	if i != nil {
		t := reflect.TypeOf(i)

		if t.Kind() != reflect.Ptr {
			return nil, errors.New(fmt.Sprintf("storm: passed value is not of a pointer type but %v", t.Kind()))
		}

		if t.Elem().Kind() != reflect.Slice {
			return nil, errors.New(fmt.Sprintf("storm: passed value is not a slice type but a %v", t.Elem().Kind()))
		}

		if t.Elem().Elem().Kind() == reflect.Ptr {
			destIsPointer = true

			if t.Elem().Elem().Elem() != q.tblMap.goType {
				return nil, errors.New(fmt.Sprintf("storm: passed slice type is not of the type %v where this query is based upon but its a %v", q.tblMap.goType, t.Elem().Elem().Elem()))
			}
		} else if t.Elem().Elem() != q.tblMap.goType {
			return nil, errors.New(fmt.Sprintf("storm: passed slice type is not of the type %v where this query is based upon but its a %v", q.tblMap.goType, t.Elem().Elem()))
		}
	}

	sql, bind := q.generateSelectSQL()
	stmt, err := q.storm.db.Prepare(sql)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	rows, err := stmt.Query(bind...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var vSlice reflect.Value
	var list []interface{} = nil
	if i != nil {
		vSlice = reflect.ValueOf(i).Elem()
	} else {
		list = make([]interface{}, 0)
	}

	for {
		if !rows.Next() {
			// if error occured return rawselect
			if rows.Err() != nil {
				return nil, rows.Err()
			}

			return list, nil
		}

		v := reflect.New(q.tblMap.goType)
		dest := make([]interface{}, len(q.tblMap.columns))
		for key, col := range q.tblMap.columns {
			dest[key] = v.Elem().FieldByIndex(col.goIndex).Addr().Interface()
		}
		err = rows.Scan(dest...)
		if err != nil {
			return nil, err
		}

		if i == nil { //append to the list
			list = append(list, v.Interface())
		} else {
			if false == destIsPointer {
				vSlice.Set(reflect.Append(vSlice, v.Elem()))
			} else {
				vSlice.Set(reflect.Append(vSlice, v))
			}
		}
	}
}

//Selectute a count
func (q *Query) Count() (int64, error) {

	var bindVars []interface{}
	var sql bytes.Buffer

	//add table name
	sql.WriteString(fmt.Sprintf("SELECT COUNT(*) FROM `%v`", q.tblMap.Name))

	//add where
	if len(q.where) > 0 {

		sql.WriteString(" WHERE ")

		//create where keys
		pos := 0
		for cond, attr := range q.where {
			if pos > 0 {
				sql.WriteString(" AND ")
			}
			sql.WriteString(cond)

			bindVars = append(bindVars, attr...)
			pos++
		}
	}

	stmt, err := q.storm.db.Prepare(sql.String())
	if err != nil {
		return 0, err
	}

	var count int64
	row := stmt.QueryRow(bindVars...)
	err = row.Scan(&count)
	if err != nil {
		return 0, err
	}

	return count, nil
}

//perpare a select statement
func (q *Query) generateSelectSQL() (string, []interface{}) {

	var bindVars []interface{}
	var sql bytes.Buffer
	var pos int

	sql.WriteString("SELECT ")

	//create columns
	pos = 0
	if len(q.columns) > 0 {
		for _, col := range q.columns {
			if pos > 0 {
				sql.WriteString(", ")
			}
			sql.WriteString(fmt.Sprintf("`%v`", col))
			pos++
		}
	} else {
		for _, col := range q.tblMap.columns {
			if pos > 0 {
				sql.WriteString(", ")
			}
			sql.WriteString(fmt.Sprintf("`%v`", col.Name))
			pos++
		}
	}

	//add table name
	sql.WriteString(fmt.Sprintf(" FROM `%v`", q.tblMap.Name))

	//add where
	if len(q.where) > 0 {

		sql.WriteString(" WHERE ")

		//create where keys
		pos = 0
		for cond, attr := range q.where {
			if pos > 0 {
				sql.WriteString(" AND ")
			}
			sql.WriteString(cond)

			bindVars = append(bindVars, attr...)
			pos++
		}
	}

	//add order
	if len(q.order) > 0 {
		sql.WriteString(" ORDER BY ")
		pos = 0
		for col, dir := range q.order {
			if pos > 0 {
				sql.WriteString(", ")
			}
			sql.WriteString(fmt.Sprintf("`%s` %s", col, dir))
			pos++
		}
	}

	//add limit
	if q.limit > 0 {
		sql.WriteString(fmt.Sprintf(" LIMIT %d", q.limit))
	}

	//add offset
	if q.offset > 0 {
		sql.WriteString(fmt.Sprintf(" OFFSET %d", q.offset))
	}

	return sql.String(), bindVars
}
