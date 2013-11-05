package storm

import (
	"testing"
)

func TestStorm_Get(t *testing.T) {

	storm := newTestStorm()
	entity, err := storm.Get("customer",1)
	
	if err != nil {
		t.Fatalf("Returned a error with message \"%v\" while getting the element", err)
	}
		
	if entity == nil {
		t.Fatalf("Returned an empty entity")
	}
	
	customer, ok := entity.(*Customer)
	if !ok {
		t.Fatalf("Conversion of returned entity failed to *Customer")
	}
	
	if customer.Id != 1 || customer.Name != "customer1" {
		t.Errorf("Entity data mismatch, expected a customer{Id:1, Name:'customer1'}")
	}
}

func TestStorm_GetWithEmbeddedStruct(t *testing.T) {

	storm := newTestStorm()
	entity, err := storm.Get("product",2)
	
	if err != nil {
		t.Fatalf("Returned a error with message \"%v\" while getting the element", err)
	}
		
	if entity == nil {
		t.Fatalf("Returned an empty entity")
	}
	
	product, ok := entity.(*Product)
	if !ok {
		t.Fatalf("Conversion of returned entity failed to *Product")
	}
	
	if product.Id != 2 || product.Name != "product2" || product.Price != 12.02 {
		t.Errorf("Entity data mismatch, expected a %v but got %v", Product{2, ProductDescription{"product2", 12.02} }, product)
	}
}

func TestStorm_GetNonExistingEntityError(t *testing.T) {

	storm := newTestStorm()
	entity, err := storm.Get("notExisting",1)
	
	if err == nil {
		t.Fatalf("Expected to get a error  but got no error")
	} 
	
	if err.Error() != "No entity with the name 'notExisting' found" {
		t.Errorf("Expected to get a error with the message \"No entity with the name 'notExisting' found\", but got message: \"%v\"", err)
	}
	
	if entity != nil {
		t.Fatalf("No entity should be returned but got something back")
	}
	
}