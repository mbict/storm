package storm

import (
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
)


type TestStructureWithTags struct {
        Id     int    `db:"name(xId),pk", json:"id"`
        Name   string `json:"name"`
        Hidden string `db:"ignore", json:"-"`
}

type TestStructure struct {
        Id   int
        Name string
}

type Customer struct {
        Id     int    `db:"pk", json:"id"`
        Name   string
}

type Order struct {
        Id     int    `db:"pk"`
}

type ProductDescription struct {
	Name string
    Price float64
}

type Product struct {
    Id     int    `db:"pk"`
    ProductDescription
}

func newTestStorm() (*Storm) {
	storm := NewStorm( newTestDb(), newTestRepository() )
	
	return storm
}

func newTestRepository() (*Repository) {
	repo := NewRepository( &Dialect{} )
	
	//default test objects
	repo.AddStructure(TestStructure{}, "test")
	repo.AddStructure(TestStructureWithTags{}, "testTags")
	repo.AddStructure(Customer{}, "customer")
	repo.AddStructure(Order{},"order")
	repo.AddStructure(Product{},"product")
	
	return repo
}

func newTestDb() (*sql.DB) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		panic("Cannot open database for testing error" + err.Error() )
	}
	
	//where to put this
	
	_, err = db.Exec( "CREATE TABLE customer (id INTEGER NOT NULL PRIMARY KEY, name VARCHAR(255));" )
	_, err = db.Exec( "INSERT INTO customer(`id`,`name`) VALUES (1,'customer1');" )	
	_, err = db.Exec( "INSERT INTO customer(`id`,`name`) VALUES (2,'customer2');" )	
	_, err = db.Exec( "INSERT INTO customer(`id`,`name`) VALUES (3,'customer3');" )	

    if err != nil {
     	db.Close()
     	panic("Cannot execute insert query got error "+err.Error())
     }

	_, err = db.Exec( "CREATE TABLE order (id INTEGER NOT NULL PRIMARY KEY);" )	
	_, err = db.Exec( "CREATE TABLE product (id INTEGER NOT NULL PRIMARY KEY, name VARCHAR(255), price REAL);" )	
	_, err = db.Exec( "INSERT INTO product(`id`,`name`,`price`) VALUES (1,'product1', 12.01);" )
	_, err = db.Exec( "INSERT INTO product(`id`,`name`,`price`) VALUES (2,'product2', 12.02);" )
	_, err = db.Exec( "INSERT INTO product(`id`,`name`,`price`) VALUES (3,'product3', 12.03);" )

	
	return db;
}
