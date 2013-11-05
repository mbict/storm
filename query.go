package storm

import (
	"bytes"
	"fmt"
	//"database/sql"
)

type SortDirection string

const (
	ASC SortDirection = "ASC"
	DESC SortDirection = "DESC"
)

type QueryInterface interface {
	Order(string, SortDirection) (*QueryInterface)	
	Where(string, ...interface{}) (*QueryInterface)
	Limit(int) (*QueryInterface)
	Offset(int) (*QueryInterface)
}

type Query struct {
	
	tblMap  *TableMap
	storm	*Storm
	
	where map[string][]interface{}	
	order  map[string]SortDirection
	offset int
	limit  int
}

func NewQuery( tblMap *TableMap, connection *Storm ) (*Query){
	q := &Query{
		tblMap: tblMap,
		storm: connection,
	}

	//init
	q.order =  make(map[string]SortDirection)
	q.where =  make(map[string][]interface{})
	
	return q
}

//
func (q *Query) Limit(value int) *Query {
	q.limit = value
	return q
}

//
func (q *Query) Offset(value int) *Query {
	q.offset = value
	return q
}


// Set the order
func (q *Query) Order(column string, direction SortDirection) *Query {
	q.order[column] = direction
	return q
}

//
func (q *Query) Where(sql string, bindAttr ...interface{}) *Query {
	q.where[sql] = bindAttr
	return q
}

//perpare a select statement
func (q *Query) prepareSelect() (string, []interface{}) {
	
	var bindVars []interface{}
	var sql bytes.Buffer
	var pos int

	sql.WriteString( "SELECT " )
	
	//create columns
	pos = 0
	for _, col := range q.tblMap.columns {
		if pos > 0 {
			sql.WriteString( ", " )
		}
		sql.WriteString( fmt.Sprintf( "`%v`", col.Name ) )
		pos++
	}
	
	//add table name
	sql.WriteString( fmt.Sprintf( " FROM `%v`", q.tblMap.Name ) )
	
	//add where
	if len(q.where) > 0 {

		sql.WriteString( " WHERE " )
		
		//create where keys
		pos = 0
		for cond, attr := range q.where {
			if pos > 0 {
				sql.WriteString( " AND " )
			}
			sql.WriteString( cond )
			
			bindVars = append(bindVars, attr...)
			pos++
		}
	}
	
	//add order
	if len(q.order) > 0 {
		sql.WriteString( " ORDER BY " )
		pos = 0
		for col, dir := range q.order {
			if pos > 0 {
				sql.WriteString( ", " )
			}
			sql.WriteString( fmt.Sprintf("`%s` %s", col, dir) )
			pos++
		}
	}
	
	//add limit
	if q.limit > 0 {
		sql.WriteString( fmt.Sprintf( " LIMIT %d", q.limit ) )
	}
	
	//add offset
	if q.offset > 0 {
		sql.WriteString( fmt.Sprintf( " OFFSET %d", q.offset ) )
	}
		
	return sql.String(), bindVars
}