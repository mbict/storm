[![Build Status](https://drone.io/github.com/mbict/storm/status.png)](https://drone.io/github.com/mbict/storm/latest)
[![Build Status](https://travis-ci.org/mbict/storm.png?branch=master)](https://travis-ci.org/mbict/storm)
[![Coverage Status](https://coveralls.io/repos/mbict/storm/badge.png)](https://coveralls.io/r/mbict/storm)
[![GoDoc](https://godoc.org/github.com/mbict/storm?status.png)](http://godoc.org/github.com/mbict/storm)

Storm 
=====

Storm is yet another orm implementation for the go language.

Storm stands for **ST**ructure **O**riented **R**elation **M**odel

Usage
=====

**Create a storm instance and add a structure**
```GO
//example structure
type Customer struct {
	Id               int    `db:"name(id),pk"
	Firstname	     string 
	Lastname	     string
	Hidden           string `db:"ignore"
}

db, err := storm.Open("sqlite3", ":memory:")
```

**Set connection limits**
```GO
db.SetMaxIdleConns(10)
db.SetMaxOpenConns(100)
````

**Add query logging**
```GO
db.Log(log.New(os.Stdout, "[storm] ", 0))
```

**Check connection**
```GO
db.Ping()
````

**Add table/structures**
```GO
//object instance
db.AddStructure(Customer{}, "customer")

//or with a null pointer instance
db.AddStructure((*Customer)(nil), "customer")
```

**Entity callbacks/events**

The following events are will be triggered if they are defined in the entity

* OnInsert
* OnPostInsert
* OnUpdate
* OnPostUpdate
* OnDelete
* OnPostDelete
* OnInit

You can define the optional error type, if you return a error the current method save/delete/select will stop what is was dooing and return the error.

You can also define a function attribute to get the current transaction context.

Valid callback notations

```GO
OnInit(){
	...
}

//error return only
OnInit() error{
	...
}

//context only
OnInit(ctx *Context) {
	...
}

//context and error return
OnInit(ctx *Context) error {
	...
}
```

**Insert a new entity**

Pass the new structure and leave all pks zero
After a successful save all pks will be filled
```GO
newCustomer := Customer{0, "Firstname", "Lastname"}
err := db.Save(&newCustomer)
```

**Get one entity by its primary key**
```GO
var customer Customer
obj, err := db.Find(&customer, 1)
```

**Update a entity**
```GO
customer.Lastname = "LastlastName"
err := db.Save(&customer)
```

**Delete a entity**
```GO
err := db.Delete(&customer)
```

**Get all the entities method **
```GO
q := db.Query()
var customers []Customer
err := q.Where("name LIKE ?", "%test%").Select(&customers)
```

**Get one entity method **
```GO
q := db.Query()
var customer Customer
err := q.Where("name LIKE ?", "%test%").SelectRow(&customer)
```

**Get the count**
```GO
q := db.Query()
count, err := q.Where("name LIKE ?", "%test%").Count((*Customer)(nil))
```

**Start transaction, commit or rollback**
```GO
tx := db.Begin()
tx.Save(...... etc
tx.Commit()
//or
tx.Rollback()
```

**Create table**
```GO
db.CreateTable((*Customer)(nil))
```

**Drop table**
```GO
db.DropTable((*Customer)(nil))
```

Thats it for now, Enjoy