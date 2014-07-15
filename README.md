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
type Address struct {
	Id int
	CustomerId int
	AddressLine string
}

type Customer struct {
	Id               int    `db:"name(id),pk"
	Firstname	     string 
	Lastname	     string
	Adresses		 []Adress
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
Storm requires that you register the used model before you can query them. This is because of the cache model for reflection and to resolve the relations between the model.
When you register a new structure all the structures will be checked if they are related by any ids
```GO
//object instance
db.AddStructure(Customer{})

//or with a null pointer instance
db.AddStructure((*Address)(nil))
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

If you return a error on a callback, the current method that triggered the event (save/delete/select) will stop what is was dooing and return the error.

When you need the current working context (transaction or non transactional) you can define a the storm.Context type as first function attribute.

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
OnInit(ctx *storm.Context) {
	...
}

//context and error return
OnInit(ctx *storm.Context) error {
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
err := db.Find(&customer, 1)

//or

err := db.Where("id = ?", 1).First(&customer)
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
err := q.Where("name LIKE ?", "%test%").Find(&customers)

//or with inline condition
var customers []Customer
err := db.Find(&customers, "name LIKE ?", "%test%")

//or with inline id
err := db.Find(&customers, 1)

//or with inline relation
var customer Customer{Id: 1}
var addresses []Address
err := db.Find(&addresses, customer)

```

**Get one/first entity method **
```GO
q := db.Query()
var customer Customer
err := q.Where("name LIKE ?", "%test%").First(&customer)

//or find inline condition
var customer Customer
err := db.Find(&customer, "name LIKE ?", "%test%")

//or find inline id
err := db.Find(&customer, 1)

//or find with related record
var customer Customer{Id: 1}
var address Address
err := db.Find(&address, customer)
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



**TODO**
- Create a function to fetch all the related fields(slices)
- Depends() function who wil take a entity as input and fillin all the related structures.
- Give some work to the 1 on 1 relations/structures (now only one on many are supported)

Thats it for now, Enjoy