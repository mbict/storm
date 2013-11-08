package storm

import (
	"fmt"
	"testing"
)

func TestQuery_generateSelect(t *testing.T) {
	storm := newTestStorm()
	q := NewQuery(storm.repository.getTableMap("customer"), storm)

	sql, bind := q.generateSelectSQL()

	if len(bind) != 0 {
		t.Errorf("Expected to get 0 columns to bind but got %v columns back", len(bind))
	}

	sqlExpected := "SELECT `id`, `name` FROM `customer`"
	if sql != sqlExpected {
		t.Errorf("Expected to get query \"%v\" but got the query \"%v\"", sqlExpected, sql)
	}
}

func TestQuery_generateSelectWhereOffsetLimitOrder(t *testing.T) {
	storm := newTestStorm()
	q := NewQuery(storm.repository.getTableMap("customer"), storm)
	q.Where("id = ?", 1).
		Where("name = ?", "test").
		Limit(10).
		Offset(5).
		Order("id", ASC).
		Order("name", DESC)

	sql, bind := q.generateSelectSQL()

	if len(bind) != 2 {
		t.Errorf("Expected to get 2 columns to bind but got %v columns back", len(bind))
	}

	sqlExpected := "SELECT `id`, `name` FROM `customer` WHERE id = ? AND name = ? ORDER BY `id` ASC, `name` DESC LIMIT 10 OFFSET 5"
	if sql != sqlExpected {
		t.Errorf("Expected to get query \"%v\" but got the query \"%v\"", sqlExpected, sql)
	}
}

func TestQuery_generateInsert(t *testing.T) {
	storm := newTestStorm()
	q := NewQuery(storm.repository.getTableMap("product"), storm)
	q.Column("name")

	sql := q.generateInsertSQL()

	sqlExpected := "INSERT INTO `product`(`name`) VALUES (?)"
	if sql != sqlExpected {
		t.Errorf("Expected to get query \"%v\" but got the query \"%v\"", sqlExpected, sql)
	}
}

func TestQuery_generateInsertAutoColumns(t *testing.T) {
	storm := newTestStorm()
	q := NewQuery(storm.repository.getTableMap("product"), storm)

	sql := q.generateInsertSQL()

	sqlExpected := "INSERT INTO `product`(`name`, `price`) VALUES (?, ?)"
	if sql != sqlExpected {
		t.Errorf("Expected to get query \"%v\" but got the query \"%v\"", sqlExpected, sql)
	}
}

func TestQuery_generateUpdate(t *testing.T) {
	storm := newTestStorm()
	q := NewQuery(storm.repository.getTableMap("product"), storm)
	q.Column("name").Where("id = ?", 1)

	sql, bind := q.generateUpdateSQL()

	if len(bind) != 1 {
		t.Errorf("Expected to get 1 column back to bind but got %v columns back", len(bind))
	}

	sqlExpected := "UPDATE `product` SET `name` = ? WHERE id = ?"
	if sql != sqlExpected {
		t.Errorf("Expected to get query \"%v\" but got the query \"%v\"", sqlExpected, sql)
	}
}

func TestQuery_generateUpdateAutoColumns(t *testing.T) {
	storm := newTestStorm()
	q := NewQuery(storm.repository.getTableMap("product"), storm)
	q.Where("id = ?", 1)

	sql, bind := q.generateUpdateSQL()
	if len(bind) != 1 {
		t.Errorf("Expected to get 1 column back to bind but got %v columns back", len(bind))
	}

	sqlExpected := "UPDATE `product` SET `name` = ?, `price` = ? WHERE id = ?"
	if sql != sqlExpected {
		t.Errorf("Expected to get query \"%v\" but got the query \"%v\"", sqlExpected, sql)
	}
}

func TestQuery_generateDelete(t *testing.T) {
	storm := newTestStorm()
	q := NewQuery(storm.repository.getTableMap("customer"), storm)
	q.Where("id = ?", 1).
		Where("name = ?", "test")

	sql, bind := q.generateDeleteSQL()

	if len(bind) != 2 {
		t.Errorf("Expected to get 2 columns to bind but got %v columns back", len(bind))
	}

	sqlExpected := "DELETE FROM `customer` WHERE id = ? AND name = ?"
	if sql != sqlExpected {
		t.Errorf("Expected to get query \"%v\" but got the query \"%v\"", sqlExpected, sql)
	}
}

func TestQuery_Exec(t *testing.T) {
	storm := newTestStorm()
	q := NewQuery(storm.repository.getTableMap("product"), storm)

	result, err := q.Exec()
	if err != nil {
		t.Fatalf("Returned a error with message \"%v\" while getting the element", err)
	}

	if result == nil {
		t.Fatalf("Expected a result slice but got nil")
	}

	count := len(result)
	if count != 3 {
		t.Errorf("Expected to get \"%d\" rows but got  \"%d\" rows", 3, count)
	}

	//with one where
	q.Where("id > ?", 1)
	result, err = q.Exec()
	if err != nil {
		t.Fatalf("Returned a error with message \"%v\" while getting the element", err)
	}

	if result == nil {
		t.Fatalf("Expected a result slice but got nil")
	}

	count = len(result)
	if count != 2 {
		t.Errorf("Expected to get \"%d\" rows but got  \"%d\" rows", 2, count)
	}
}

func TestQuery_Count(t *testing.T) {
	storm := newTestStorm()
	q := NewQuery(storm.repository.getTableMap("product"), storm)

	count, err := q.Count()

	if err != nil {
		t.Fatalf("Returned a error with message \"%v\" while getting the element", err)
	}

	if count != 3 {
		t.Errorf("Expected to get \"%d\" rows but got  \"%d\"", 3, count)
	}

	//with one where
	q.Where("id > ?", 1)
	count, err = q.Count()
	if err != nil {
		t.Fatalf("Returned a error with message \"%v\" while getting the element", err)
	}

	if count != 2 {
		t.Errorf("Expected to get \"%d\" rows but got  \"%d\"", 2, count)
	}
}
