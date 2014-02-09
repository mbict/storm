package storm

import (
	"database/sql"
	"reflect"

	"github.com/mbict/storm/dialect"
)

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

func (this *Transaction) DB() sqlCommon {
	return this.tx
}

func (this *Transaction) Dialect() dialect.Dialect {
	return this.storm.Dialect()
}

// create a new query subobject
func (this *Transaction) Query() *Query {
	return newQuery(this, nil)
}

func (this *Transaction) Order(column string, direction SortDirection) *Query {
	return this.Query().Order(column, direction)
}

func (this *Transaction) Where(condition string, bindAttr ...interface{}) *Query {
	return this.Query().Where(condition, bindAttr...)
}

func (this *Transaction) Limit(limit int) *Query {
	return this.Query().Limit(limit)
}

func (this *Transaction) Offset(offset int) *Query {
	return this.Query().Offset(offset)
}

func (this *Transaction) Find(i interface{}, where ...interface{}) error {
	return this.Query().fetchRow(i, this.tx, where...)
}

func (this *Transaction) Delete(i interface{}) error {
	return this.storm.deleteEntity(i, this.tx)
}

func (this *Transaction) Save(i interface{}) error {
	return this.storm.saveEntity(i, this.tx)
}

func (this *Transaction) Select(i []interface{}) error {
	return this.Query().fetchAll(i, this.tx)
}

func (this *Transaction) SelectRow(i interface{}) error {
	return this.Find(i)
}

func (this *Transaction) Count(i interface{}) (int64, error) {
	return this.Query().Count(i)
}

// commit transaction
func (this *Transaction) Commit() error {
	return this.tx.Commit()
}

// rollback the transaction
func (this *Transaction) Rollback() error {
	return this.tx.Rollback()
}

func (this *Transaction) table(t reflect.Type) (tbl *table, ok bool) {
	return this.storm.table(t)
}
