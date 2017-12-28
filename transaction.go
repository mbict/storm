package storm

import (
	"database/sql"
)

type Transaction interface {
	dbContext
	CRUD
	Query

	DB() *sql.Tx
	Commit() error
	Rollback() error
}

//Transaction structure
type transaction struct {
	dbContext
	tx *sql.Tx
}

func newTransaction(storm Storm, tx *sql.Tx) Transaction {
	return &transaction{
		dbContext: storm,
		tx:        tx,
	}
}

//DB will return the current connection
func (t *transaction) DB() *sql.Tx {
	return t.tx
}

/*****************************************
  Implementation dbContext interface
 *****************************************/

//DB will return the current connection
func (t *transaction) db() sqlCommon {
	return t.tx
}

/*****************************************
  Implementation Query interface
 *****************************************/

//Query Creates a clone of the current query object
func (t *transaction) Query() Query {
	return newQuery(t, nil)
}

//Order will set the order
func (t *transaction) Order(column string, direction SortDirection) Query {
	return t.Query().Order(column, direction)
}

//Where adds new where conditions to the query
func (t *transaction) Where(condition string, bindAttr ...interface{}) Query {
	return t.Query().Where(condition, bindAttr...)
}

//Limit sets the limit for select
func (t *transaction) Limit(limit int) Query {
	return t.Query().Limit(limit)
}

//Offset sets the offset for select
func (t *transaction) Offset(offset int) Query {
	return t.Query().Offset(offset)
}

//Find will try to retrieve the matching structure/entity based on your where statement
//You can priovide a slice or a single element
func (t *transaction) Find(i interface{}, where ...interface{}) error {
	return t.Query().Find(i, where...)
}

func (t *transaction) FetchRelated(columns ...string) Query {
	return t.Query().FetchRelated(columns...)
}

func (t *transaction) Count(i interface{}) (int64, error) {
	return t.Query().Count(i)
}

func (t *transaction) FindRelated(i interface{}, columns ...string) error {
	return t.Query().FindRelated(i, columns...)
}

func (t *transaction) First(i interface{}) error {
	return t.Query().First(i)
}

//Delete will delete the provided structure from the datastore
func (t *transaction) Delete(i interface{}) error {
	//return t.storm().deleteEntity(i, t)
	panic("implement me")
}

//Save will insert or update the provided structure in the datastore
func (t *transaction) Save(i interface{}) error {
	//return t.storm().saveEntity(i, t)
	panic("implement me")
}

//Commit will commit the current transaction and closes
func (t *transaction) Commit() error {
	return t.tx.Commit()
}

//Rollback will undo all mutations to the datastore in this transaction and closes
func (t *transaction) Rollback() error {
	return t.tx.Rollback()
}
