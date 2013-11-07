package storm

import (
	"testing"
)

func TestStorm_Get(t *testing.T) {

	storm := newTestStorm()
	entity, err := storm.Get("customer", 1)

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

func TestStorm_GetNoResult(t *testing.T) {

	storm := newTestStorm()
	entity, err := storm.Get("customer", 9999)

	if err != nil {
		t.Fatalf("Returned a error with message \"%v\" while getting the element", err)
	}

	if entity != nil {
		t.Fatalf("Returned an entity while expecting nil")
	}
}

func TestStorm_GetWithEmbeddedStruct(t *testing.T) {

	storm := newTestStorm()
	entity, err := storm.Get("product", 2)

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
		t.Errorf("Entity data mismatch, expected a %v but got %v", Product{2, ProductDescription{"product2", 12.02}}, product)
	}
}

func TestStorm_GetNonExistingEntityError(t *testing.T) {

	storm := newTestStorm()
	entity, err := storm.Get("notExisting", 1)

	if err == nil {
		t.Fatalf("Expected to get a error but got no error")
	}

	if err.Error() != "No entity with the name 'notExisting' found" {
		t.Errorf("Expected to get a error with the message \"No entity with the name 'notExisting' found\", but got message: \"%v\"", err)
	}

	if entity != nil {
		t.Fatalf("No entity should be returned but got something back")
	}

}

func TestStorm_GetNoPKDefined(t *testing.T) {
	storm := newTestStorm()
	_, err := storm.Get("productdescription")

	if err == nil {
		t.Fatalf("Expected to get a error but got no error")
	}

	expectedError := "No primary key defined"
	if err.Error() != expectedError {
		t.Errorf("Expected to get a error with the message \"%v\", but got message: \"%v\"", expectedError, err)
	}
}

func TestStorm_GetNotEnoughAttributesProvided(t *testing.T) {
	storm := newTestStorm()
	_, err := storm.Get("product")

	if err == nil {
		t.Fatalf("Expected to get a error but got no error")
	}

	expectedError := "Not engough arguments for provided for primary keys, need 1 attributes"
	if err.Error() != expectedError {
		t.Errorf("Expected to get a error with the message \"%v\", but got message: \"%v\"", expectedError, err)
	}
}

func TestStorm_Delete(t *testing.T) {

	storm := newTestStorm()

	testEntity, err := storm.Get("product", 1)
	if nil == testEntity || nil != err {
		t.Fatalf("Cannot query for target entity or not found")
	}

	product := Product{1, ProductDescription{"test", 12.01}}
	err = storm.Delete(product)

	if err != nil {
		t.Fatalf("Returned a error with message \"%v\" while trying to delete the entity", err)
	}

	testEntity, err = storm.Get("product", 1)
	if nil != err {
		t.Fatalf("Cannot query for target entity, error '%v'", err)
	}

	if nil != testEntity {
		t.Fatalf("Target entity does still exists in datastore, not deleted")
	}
}

func TestStorm_DeleteNotRegisteredStructure(t *testing.T) {

	storm := newTestStorm()
	notRegisteredStructure := TestStructureWithTags{}
	err := storm.Delete(notRegisteredStructure)

	if err == nil {
		t.Fatalf("Expected a error but no error returned")
	}

	expectedError := "No structure registered in repository of type 'storm.TestStructureWithTags'"
	if err.Error() != expectedError {
		t.Fatalf("Expected error '%v' but got error '%v'", expectedError, err)
	}
}

func TestStorm_DeleteNoPKDefined(t *testing.T) {
	storm := newTestStorm()
	productDescription := ProductDescription{}
	err := storm.Delete(productDescription)

	if err == nil {
		t.Fatalf("Expected to get a error but got no error")
	}

	expectedError := "No primary key defined"
	if err.Error() != expectedError {
		t.Errorf("Expected to get a error with the message \"%v\", but got message: \"%v\"", expectedError, err)
	}
}

func TestStorm_SaveNewEntity(t *testing.T) {

	storm := newTestStorm()
	entity, err := storm.Get("product", 4)

	if nil != err {
		t.Fatalf("Returned a error with message \"%v\" while getting the element", err)
	}

	if entity != nil {
		t.Fatalf("Should not return an entity, next new entity should be id:4")
	}

	productNew := Product{0, ProductDescription{"product4", 11.22}}
	err = storm.Save(productNew)

	if nil != err {
		t.Fatalf("Returned a error with message \"%v\" while saving the element", err)
	}

	entity, _ = storm.Get("product", 4)

	if entity == nil {
		t.Fatalf("Enity not saved, database returned no result")
	}

	product, _ := entity.(*Product)
	if product.Id != 4 || product.Name != "product4" || product.Price != 11.22 {
		t.Errorf("Entity expected data after update mismatch, expected a %v but got %v", Product{4, ProductDescription{"product4", 11.22}}, product)
	}
}

func TestStorm_SaveNewEntityWithPointer(t *testing.T) {

	storm := newTestStorm()
	entity, err := storm.Get("product", 4)

	if nil != err {
		t.Fatalf("Returned a error with message \"%v\" while getting the element", err)
	}

	if entity != nil {
		t.Fatalf("Should not return an entity, next new entity should be id:4")
	}

	productNew := Product{0, ProductDescription{"product4", 11.22}}
	err = storm.Save(&productNew)

	if nil != err {
		t.Fatalf("Returned a error with message \"%v\" while saving the element", err)
	}

	entity, _ = storm.Get("product", 4)

	if entity == nil {
		t.Fatalf("Enity not saved, database returned no result")
	}

	product, _ := entity.(*Product)
	if product.Id != 4 || product.Name != "product4" || product.Price != 11.22 {
		t.Errorf("Entity expected data after update mismatch, expected a %v but got %v", Product{4, ProductDescription{"product4", 11.22}}, product)
	}
}

func TestStorm_SaveExistingEntity(t *testing.T) {
	storm := newTestStorm()
	entity, err := storm.Get("product", 1)

	if nil != err {
		t.Fatalf("Returned a error with message \"%v\" while getting the element", err)
	}

	if entity == nil {
		t.Fatalf("Returned an empty entity")
	}

	product, ok := entity.(*Product)
	if !ok {
		t.Fatalf("Conversion of returned entity failed to *Product")
	}

	if product.Id != 1 || product.Name != "product1" || product.Price != 12.01 {
		t.Errorf("Entity start data mismatch, expected a %v but got %v", Product{1, ProductDescription{"product1", 12.01}}, product)
	}

	product.Name = "product1updated"
	product.Price = 11.33
	err = storm.Save(*product)

	if nil != err {
		t.Fatalf("Returned a error with message \"%v\" while saving the element", err)
	}

	entity, _ = storm.Get("product", 1)
	product, _ = entity.(*Product)

	if product.Id != 1 || product.Name != "product1updated" || product.Price != 11.33 {
		t.Errorf("Entity expected data after update mismatch, expected a %v but got %v", Product{1, ProductDescription{"product1updated", 11.33}}, product)
	}
}

func TestStorm_SaveExistingEntityPointer(t *testing.T) {
	storm := newTestStorm()
	entity, err := storm.Get("product", 1)

	if nil != err {
		t.Fatalf("Returned a error with message \"%v\" while getting the element", err)
	}

	if entity == nil {
		t.Fatalf("Returned an empty entity")
	}

	product, ok := entity.(*Product)
	if !ok {
		t.Fatalf("Conversion of returned entity failed to *Product")
	}

	if product.Id != 1 || product.Name != "product1" || product.Price != 12.01 {
		t.Errorf("Entity start data mismatch, expected a %v but got %v", Product{1, ProductDescription{"product1", 12.01}}, product)
	}

	product.Name = "product1updated"
	product.Price = 11.33
	err = storm.Save(product)

	if nil != err {
		t.Fatalf("Returned a error with message \"%v\" while saving the element", err)
	}

	entity, _ = storm.Get("product", 1)
	product, _ = entity.(*Product)

	if product.Id != 1 || product.Name != "product1updated" || product.Price != 11.33 {
		t.Errorf("Entity expected data after update mismatch, expected a %v but got %v", Product{1, ProductDescription{"product1updated", 11.33}}, product)
	}
}
