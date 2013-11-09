package storm

import (
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

func TestQuery_Exec(t *testing.T) {
	storm := newTestStorm()
	q := NewQuery(storm.repository.getTableMap("product"), storm)

	//fetch all with out slice
	result, err := q.Exec(nil)
	if err != nil {
		t.Fatalf("Returned a error with message \"%v\" while getting the element", err)
	}

	if result == nil {
		t.Fatalf("Expected a result slice but got nil")
	}

	count := len(result)
	if count != 3 {
		t.Fatalf("Expected to get \"%d\" rows but got  \"%d\" rows", 3, count)
	}

	if product, ok := result[0].(*Product); !ok || product.Id != 1 || product.Name != "product1" || product.Price != 12.01 {
		t.Errorf("Entity data mismatch, expected a %v but got %v", Product{1, ProductDescription{"product1", 12.01}}, product)
	}

	if product, ok := result[1].(*Product); !ok || product.Id != 2 || product.Name != "product2" || product.Price != 12.02 {
		t.Errorf("Entity data mismatch, expected a %v but got %v", Product{2, ProductDescription{"product2", 12.02}}, product)
	}

	if product, ok := result[2].(*Product); !ok || product.Id != 3 || product.Name != "product3" || product.Price != 12.03 {
		t.Errorf("Entity data mismatch, expected a %v but got %v", Product{3, ProductDescription{"product3", 12.03}}, product)
	}

	//fetch all with normal typed slice
	var result1 []Product
	result, err = q.Exec(&result1)
	if err != nil {
		t.Fatalf("Returned a error with message \"%v\" while getting the element", err)
	}

	if result1 == nil {
		t.Fatalf("Expected a result slice but got nil")
	}

	//we provided a interface so result should be nil
	if result != nil {
		t.Fatalf("Expected the return i[]interface{} should be nil")
	}

	count = len(result1)
	if count != 3 {
		t.Fatalf("Expected to get \"%d\" rows but got  \"%d\" rows", 3, count)
	}

	if result1[0].Id != 1 || result1[0].Name != "product1" || result1[0].Price != 12.01 {
		t.Errorf("Entity data mismatch, expected a %v but got %v", Product{1, ProductDescription{"product1", 12.01}}, result1[0])
	}

	if result1[1].Id != 2 || result1[1].Name != "product2" || result1[1].Price != 12.02 {
		t.Errorf("Entity data mismatch, expected a %v but got %v", Product{2, ProductDescription{"product2", 12.02}}, result1[1])
	}

	if result1[2].Id != 3 || result1[2].Name != "product3" || result1[2].Price != 12.03 {
		t.Errorf("Entity data mismatch, expected a %v but got %v", Product{3, ProductDescription{"product3", 12.03}}, result1[2])
	}

	//with one where and pointer slice
	q.Where("id > ?", 1)
	var result2 []*Product
	result, err = q.Exec(&result2)
	if err != nil {
		t.Fatalf("Returned a error with message \"%v\" while getting the element", err)
	}

	if result2 == nil {
		t.Fatalf("Expected a result slice but got nil")
	}

	count = len(result2)
	if count != 2 {
		t.Fatalf("Expected to get \"%d\" rows but got  \"%d\" rows", 2, count)
	}

	if result2[0].Id != 2 || result2[0].Name != "product2" || result2[0].Price != 12.02 {
		t.Errorf("Entity data mismatch, expected a %v but got %v", Product{2, ProductDescription{"product2", 12.02}}, result2[0])
	}

	if result2[1].Id != 3 || result2[1].Name != "product3" || result2[1].Price != 12.03 {
		t.Errorf("Entity data mismatch, expected a %v but got %v", Product{3, ProductDescription{"product3", 12.03}}, result2[1])
	}
}

func TestQuery_ExecErrors(t *testing.T) {
	storm := newTestStorm()
	q := NewQuery(storm.repository.getTableMap("product"), storm)

	//no slice input error
	var val int = 1245
	_, err := q.Exec(&val)
	if nil == err {
		t.Fatalf("Expected to get a error but no error given")
	}

	expectedError := "storm: passed value is not a slice type but a int"
	if err.Error() != expectedError {
		t.Errorf("Expected to get a error with the message \"%s\", but got message: \"%s\"", expectedError, err)
	}

	var sliceNoPointer []int
	_, err = q.Exec(sliceNoPointer)
	if nil == err {
		t.Fatalf("Expected to get a error but no error given")
	}

	expectedError = "storm: passed value is not of a pointer type but slice"
	if err.Error() != expectedError {
		t.Errorf("Expected to get a error with the message \"%s\", but got message: \"%s\"", expectedError, err)
	}

	//no  mismatch type slice
	var sliceMismatch []Customer
	_, err = q.Exec(&sliceMismatch)
	if nil == err {
		t.Fatalf("Expected to get a error but no error given")
	}

	expectedError = "storm: passed slice type is not of the type storm.Product where this query is based upon but its a storm.Customer"
	if err.Error() != expectedError {
		t.Errorf("Expected to get a error with the message \"%s\", but got message: \"%s\"", expectedError, err)
	}

}
