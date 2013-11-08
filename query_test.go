package storm

import (
	"testing"
	//"bytes"
)

func TestQuery_prepareSelect(t *testing.T) {
	storm := newTestStorm()
	q := NewQuery(storm.repository.getTableMap("customer"), storm)

	sql, bind := q.prepareSelect()

	if len(bind) != 0 {
		t.Errorf("Expected to get 0 columns to bind but got %v columns back", len(bind))
	}

	sqlExpected := "SELECT `id`, `name` FROM `customer`"
	if sql != sqlExpected {
		t.Errorf("Expected to get query \"%v\" but got the query \"%v\"", sqlExpected, sql)
	}
}

func TestQuery_prepareSelectWhereOffsetLimitOrder(t *testing.T) {
	storm := newTestStorm()
	q := NewQuery(storm.repository.getTableMap("customer"), storm)
	q.Where("id = ?", 1).
		Where("name = ?", "test").
		Limit(10).
		Offset(5).
		Order("id", ASC).
		Order("name", DESC)

	sql, bind := q.prepareSelect()

	if len(bind) != 2 {
		t.Errorf("Expected to get 2 columns to bind but got %v columns back", len(bind))
	}

	sqlExpected := "SELECT `id`, `name` FROM `customer` WHERE id = ? AND name = ? ORDER BY `id` ASC, `name` DESC LIMIT 10 OFFSET 5"
	if sql != sqlExpected {
		t.Errorf("Expected to get query \"%v\" but got the query \"%v\"", sqlExpected, sql)
	}
}

func TestQuery_prepareInsert(t *testing.T) {
	storm := newTestStorm()
	q := NewQuery(storm.repository.getTableMap("product"), storm)
	q.Column("name")

	sql, bind := q.prepareInsert()

	if len(bind) != 0 {
		t.Errorf("Expected to get no column back to bind but got %v columns back", len(bind))
	}

	sqlExpected := "INSERT INTO `product`(`name`) VALUES (?)"
	if sql != sqlExpected {
		t.Errorf("Expected to get query \"%v\" but got the query \"%v\"", sqlExpected, sql)
	}
}

func TestQuery_prepareInsertAutoColumns(t *testing.T) {
	storm := newTestStorm()
	q := NewQuery(storm.repository.getTableMap("product"), storm)

	sql, bind := q.prepareInsert()

	if len(bind) != 0 {
		t.Errorf("Expected to get no columns back to bind but got %v columns back", len(bind))
	}

	sqlExpected := "INSERT INTO `product`(`name`, `price`) VALUES (?, ?)"
	if sql != sqlExpected {
		t.Errorf("Expected to get query \"%v\" but got the query \"%v\"", sqlExpected, sql)
	}
}

func TestQuery_prepareUpdate(t *testing.T) {
	storm := newTestStorm()
	q := NewQuery(storm.repository.getTableMap("product"), storm)
	q.Column("name").Where("id = ?", 1)

	sql, bind := q.prepareUpdate()

	if len(bind) != 1 {
		t.Errorf("Expected to get 1 column back to bind but got %v columns back", len(bind))
	}

	sqlExpected := "UPDATE `product` SET `name` = ? WHERE id = ?"
	if sql != sqlExpected {
		t.Errorf("Expected to get query \"%v\" but got the query \"%v\"", sqlExpected, sql)
	}
}

func TestQuery_prepareUpdateAutoColumns(t *testing.T) {
	storm := newTestStorm()
	q := NewQuery(storm.repository.getTableMap("product"), storm)
	q.Where("id = ?", 1)

	sql, bind := q.prepareUpdate()
	if len(bind) != 1 {
		t.Errorf("Expected to get 1 column back to bind but got %v columns back", len(bind))
	}

	sqlExpected := "UPDATE `product` SET `name` = ?, `price` = ? WHERE id = ?"
	if sql != sqlExpected {
		t.Errorf("Expected to get query \"%v\" but got the query \"%v\"", sqlExpected, sql)
	}
}

func TestQuery_prepareDelete(t *testing.T) {
	storm := newTestStorm()
	q := NewQuery(storm.repository.getTableMap("customer"), storm)
	q.Where("id = ?", 1).
		Where("name = ?", "test")

	sql, bind := q.prepareDelete()

	if len(bind) != 2 {
		t.Errorf("Expected to get 2 columns to bind but got %v columns back", len(bind))
	}

	sqlExpected := "DELETE FROM `customer` WHERE id = ? AND name = ?"
	if sql != sqlExpected {
		t.Errorf("Expected to get query \"%v\" but got the query \"%v\"", sqlExpected, sql)
	}
}

func TestQuery_Exec(t *testing.T) {
	t.Fatalf("Not implemented")
}

func TestQuery_Count(t *testing.T) {
	t.Fatalf("Not implemented")
}
