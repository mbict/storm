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

type Product struct {
        Id     int    `db:"pk"`
        Name string
        Price float64
}

func newTestStorm() (*Storm) {
	storm := NewStorm( newTestDb(), newTestRepository() )
	
	return storm
}

func newTestRepository() (*Repository) {
	repo := NewRepository( &Dialect{} )
	
	//default test objects
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
	sql := `
CREATE TABLE customer (id INTEGER NOT NULL PRIMARY KEY, name VARCHAR(255));
INSERT INTO customer(id,name) VALUES (1,'customer1') (2,'customer2') (3,'customer3');
CREATE TABLE order (id INTEGER NOT NULL PRIMARY KEY);
CREATE TABLE product (id INTEGER NOT NULL PRIMARY KEY, name VARCHAR(255), price REAL);
`
	 _, err = db.Exec( sql )
     if err != nil {
     	db.Close()
     	panic("Cannot execute query '"+sql+"' got error "+err.Error())
     }
	
	return db;
}
