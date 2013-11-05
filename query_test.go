package storm

import (
	"testing"
	//"bytes"
)

func TestQuery_PrepareSelectBare(t *testing.T) {
	storm := newTestStorm()
	q := NewQuery( storm.repository.getTableMap("customer"), storm)
	
	sql, bind := q.prepareSelect()
	
	if len( bind ) != 0 {
		t.Errorf("Expected to get 0 columns to bind but go %v columns back", len(bind) )
	}
	
	sqlExpected := "SELECT `id`, `name` FROM `customer`"
	if sql != sqlExpected {
		t.Errorf("Expected to get query \"%v\" but got the query \"%v\"", sqlExpected, sql )
	}
}

func TestQuery_PrepareSelectWhereOffsetLimitOrder(t *testing.T) {
	storm := newTestStorm()
	q := NewQuery( storm.repository.getTableMap("customer"), storm)
	q.Where("id = ?", 1).
		Where("name = ?", "test").
		Limit(10).
		Offset(5).
		Order("id", ASC).
		Order("name", DESC)
		
	sql, bind := q.prepareSelect()
	
	if len( bind ) != 2 {
		t.Errorf("Expected to get 2 columns to bind but go %v columns back", len(bind) )
	}
	
	sqlExpected := "SELECT `id`, `name` FROM `customer` WHERE id = ? AND name = ? ORDER BY `id` ASC, `name` DESC LIMIT 10 OFFSET 5"
	if sql != sqlExpected {
		t.Errorf("Expected to get query \"%v\" but got the query \"%v\"", sqlExpected, sql )
	}
}