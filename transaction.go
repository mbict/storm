package storm

import "database/sql"

type Transaction struct {
	query *Query
	tx    *sql.Tx
}

func newTransaction(query *Query, tx *sql.Tx) *Transaction {
	return &Transaction{
		query: query,
		tx:    tx,
	}
}

// create a new query subobject
func (this *Transaction) Query() *Transaction {
	return newTransaction(this.query.Query(), this.tx)
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
	return this.query.fetchRow(i, this.tx)
}

func (this *Transaction) Delete(i interface{}) error {
	return this.query.storm.deleteEntity(i, this.tx)
}

func (this *Transaction) Save(i interface{}) error {
	return this.query.storm.saveEntity(i, this.tx)
}

func (this *Transaction) Select(i []interface{}) error {
	return this.query.fetchAll(i, this.tx)
}

func (this *Transaction) SelectRow(i interface{}) error {
	return this.Find(i)
}

func (this *Transaction) Count(*int64) error {
	return nil
}

// commit transaction
func (this *Transaction) Commit() error {
	return this.tx.Commit()
}

// rollback the transaction
func (this *Transaction) Rollback() error {
	return this.tx.Rollback()
}
