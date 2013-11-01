package storm

import (
	"testing"
)

func TestGet(t *testing.T) {

	storm := newTestStorm()
	entity, err := storm.Get("customer",1)
	
	if err != nil {
		t.Errorf("Returned a error with message \"%v\" while adding a element", err)
	} 
	
	if entity == nil {
		t.Fatalf("Returned an empty entity")
	}
	
	customer, ok := entity.(*Customer)
	if !ok {
		t.Fatalf("Conversion of returned entity failed to *TestStructureWithTags")
	}
	
	if customer.Id != 1 || customer.Name != "customer1" {
		t.Errorf("Entity data mismatch, expected a customer{Id:1, Name:'customer1'}")
	}
	
}

func TestGetNonExistingEntityError(t *testing.T) {

	storm := newTestStorm()
	entity, err := storm.Get("notExisting",1)
	
	if err == nil {
		t.Fatalf("Expected to get a error  but got no error")
	} 
	
	if err.Error() != "No entity with the name 'notExisting' found" {
		t.Errorf("Expected to get a error with the message \"No entity with the name 'notExisting' found\", but got message: \"%v\"", err)
	}
	
	if entity != nil {
		t.Errorf("No entity should be returned but got something back")
	}
	
}