package storm

import "testing"


//Test where passtrough
func TestTransaction_Where(t *testing.T) {
	var (
		s  = newTestStorm()
		tx = s.Begin()
		q  = tx.Where("id = ?", 1)
		v  []interface{}
		ok bool
	)

	if v, ok = q.where["id = ?"]; ok != true {
		t.Fatalf("Where statement not found in query")
	}

	if len(v) != 1 && v[0].(int) != 1 {
		t.Fatalf("Expected where statement value")
	}
}

//Test order passtrough
func TestTransaction_Order(t *testing.T) {
	var (
		s  = newTestStorm()
		tx = s.Begin()
		q  = tx.Order("test", ASC)
		v  SortDirection
		ok bool
	)

	if v, ok = q.order["test"]; ok != true {
		t.Fatalf("Order statement not found in query")
	}

	if v != ASC {
		t.Fatalf("Expected order statement value")
	}
}

//Test limit passtrough
func TestTransaction_Limit(t *testing.T) {
	var (
		s = newTestStorm()
		tx = s.Begin()
		q  = tx.Limit(123)
	)



	if q.limit != 123 {
		t.Fatalf("Expected limit value of 123 but got %d", q.limit)
	}
}

//Test offset passtrough
func TestTransaction_Offset(t *testing.T) {
	var (
		s = newTestStorm()
		tx = s.Begin()
		q  = tx.Offset(123)
	)

	if q.offset != 123 {
		t.Fatalf("Expected offset value of 123 but got %d", q.offset)
	}
}