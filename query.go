package storm

import (
	//"errors"
	"bytes"
	"fmt"
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
	Exec() ([]interface{}, error)
	Count() (int, error)
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

func (q *Query) Exec() ([]interface{}, error) {
	return nil, nil
}

func (q *Query) Count() (int, error) {
	return 0, nil
}

//perpare a select statement
func (q *Query) prepareSelect() (string, []interface{}) {

	var bindVars []interface{}
	var sql bytes.Buffer
	var pos int

	sql.WriteString("SELECT ")

	//create columns
	pos = 0
	for _, col := range q.tblMap.columns {
		if pos > 0 {
			sql.WriteString(", ")
		}
		sql.WriteString(fmt.Sprintf("`%v`", col.Name))
		pos++
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

func (q *Query) prepareDelete() (string, []interface{}) {

	var bindVars []interface{}
	var sql bytes.Buffer
	var pos int

	sql.WriteString(fmt.Sprintf("DELETE FROM `%s` WHERE ", q.tblMap.Name))

	//bindPks := make([]interface{}, len(q.tblMap.keys))
	pos = 0
	for cond, attr := range q.where {
		if pos > 0 {
			sql.WriteString(" AND ")
		}
		sql.WriteString(cond)

		bindVars = append(bindVars, attr...)
		pos++
	}

	//add limit
	//#define SQLITE_ENABLE_UPDATE_DELETE_LIMIT
	/*if q.limit > 0 {
		sql.WriteString(fmt.Sprintf(" LIMIT %d", q.limit))
	}*/

	return sql.String(), bindVars
}

func (q *Query) prepareInsert() (string, []interface{}) {
	//var bindVars []interface{}
	var sql bytes.Buffer
	var sqlValues bytes.Buffer
	var pos int

	sql.WriteString(fmt.Sprintf("INSERT INTO `%s`(", q.tblMap.Name))

	if len(q.columns) > 0 {
		for _, col := range q.columns {
			if pos > 0 {
				sql.WriteString(", ")
				sqlValues.WriteString(", ")
			}

			sqlValues.WriteString("?")
			sql.WriteString(fmt.Sprintf("`%s`", col))
			pos++
		}
	} else {
		for _, col := range q.tblMap.columns {
			for _, pk := range q.tblMap.keys {
				if col != pk {
					if pos > 0 {
						sql.WriteString(", ")
						sqlValues.WriteString(", ")
					}

					sqlValues.WriteString("?")
					sql.WriteString(fmt.Sprintf("`%s`", col.Name))
					pos++
				}
			}
		}
	}
	sql.WriteString(fmt.Sprintf(") VALUES (%s)", sqlValues.String()))

	return sql.String(), nil
}

func (q *Query) prepareUpdate() (string, []interface{}) {

	var bindVars []interface{}
	var sql bytes.Buffer
	var pos int

	sql.WriteString(fmt.Sprintf("UPDATE `%s` SET ", q.tblMap.Name))

	if len(q.columns) > 0 {
		for _, col := range q.columns {
			if pos > 0 {
				sql.WriteString(", ")
			}

			sql.WriteString(fmt.Sprintf("`%s` = ?", col))
			pos++
		}
	} else {
		for _, col := range q.tblMap.columns {
			for _, pk := range q.tblMap.keys {
				if col != pk {
					if pos > 0 {
						sql.WriteString(", ")
					}

					sql.WriteString(fmt.Sprintf("`%s` = ?", col.Name))
					pos++
				}
			}
		}
	}

	sql.WriteString(" WHERE ")
	pos = 0
	for cond, attr := range q.where {
		if pos > 0 {
			sql.WriteString(" AND ")
		}
		sql.WriteString(cond)

		bindVars = append(bindVars, attr...)
		pos++
	}

	return sql.String(), bindVars
}
