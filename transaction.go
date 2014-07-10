package storm

import (
	"database/sql"
	"log"
	"reflect"

	"github.com/mbict/storm/dialect"
)

//Transaction structure
type Transaction struct {
	storm *Storm
	tx    *sql.Tx
}

func newTransaction(s *Storm) *Transaction {

	tx, err := s.db.Begin()
	if err != nil {
		panic(err)
	}

	return &Transaction{
		storm: s,
		tx:    tx,
	}
}

//DB will return the current connection
func (transaction *Transaction) DB() sqlCommon {
	return transaction.tx
}

//Storm will return the storm instance
func (transaction *Transaction) Storm() *Storm {
	return transaction.storm
}

//Dialect returns the current dialect used by the connection
func (transaction *Transaction) Dialect() dialect.Dialect {
	return transaction.storm.Dialect()
}

//Query Creates a clone of the current query object
func (transaction *Transaction) Query() *Query {
	return newQuery(transaction, nil)
}

//Order will set the order
func (transaction *Transaction) Order(column string, direction SortDirection) *Query {
	return transaction.Query().Order(column, direction)
}

//Where adds new where conditions to the query
func (transaction *Transaction) Where(condition string, bindAttr ...interface{}) *Query {
	return transaction.Query().Where(condition, bindAttr...)
}

//Limit sets the limit for select
func (transaction *Transaction) Limit(limit int) *Query {
	return transaction.Query().Limit(limit)
}

//Offset sets the offset for select
func (transaction *Transaction) Offset(offset int) *Query {
	return transaction.Query().Offset(offset)
}

//Find will try to retreive the matching structure/entity based on your where statement
//You can priovide a slice or a single element
func (transaction *Transaction) Find(i interface{}, where ...interface{}) error {
	return transaction.Query().Find(i, where...)
}

//Delete will delete the provided structure from the datastore
func (transaction *Transaction) Delete(i interface{}) error {
	return transaction.storm.deleteEntity(i, transaction)
}

//Save will insert or update the provided structure in the datastore
func (transaction *Transaction) Save(i interface{}) error {
	return transaction.storm.saveEntity(i, transaction)
}

//Commit will commit the current transaction and closes
func (transaction *Transaction) Commit() error {
	return transaction.tx.Commit()
}

//Rollback will undo all mutations to the datastore in this transaction and closes
func (transaction *Transaction) Rollback() error {
	return transaction.tx.Rollback()
}

func (transaction *Transaction) table(t reflect.Type) (tbl *table, ok bool) {
	return transaction.storm.table(t)
}

func (transaction *Transaction) logger() *log.Logger {
	return transaction.storm.log
}
