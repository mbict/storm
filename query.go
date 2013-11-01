package storm

import ()

type SortDirection int

type QueryInterface interface {
	Limit(value int) *QueryInterface
	Order(column string, direction SortDirection) *QueryInterface
	Offset(value int) *QueryInterface
}

type Query struct {
	table  *TableMap
	order  map[string]SortDirection
	offset int
	limit  int
}

func (q *Query) Limit(value int) *Query {
	q.limit = value
	return q
}

func (q *Query) Offset(value int) *Query {
	q.offset = value
	return q
}

// Set the order
func (q *Query) Order(column string, direction SortDirection) *Query {
	q.order[column] = direction
	return q
}
