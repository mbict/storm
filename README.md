[![Build Status](https://drone.io/github.com/mbict/storm/status.png)](https://drone.io/github.com/mbict/storm/latest)
[![Build Status](https://travis-ci.org/mbict/storm.png?branch=master)](https://travis-ci.org/mbict/storm)
[![Coverage Status](https://coveralls.io/repos/mbict/storm/badge.png)](https://coveralls.io/r/mbict/storm)
[![GoDoc](https://godoc.org/github.com/mbict/storm?status.png)](http://godoc.org/github.com/mbict/storm)
[![GoCover](http://gocover.io/_badge/github.com/mbict/storm)](http://gocover.io/github.com/mbict/storm)

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

type Telephone struct {
	Id int
	Number string
}

type Customer struct {
	Id               int    `db:"name(id),pk"
	Firstname	     string 
	Lastname	     string
	Adresses		 []Adresses //oneToMany
	Telephone		 Telephone
	TelephoneId		 int64 //oneToOne
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
db.RegisterStructure(Customer{})

//or with a null pointer instance
db.RegisterStructure((*Address)(nil))
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


**Get relational/dependent records **
You can populate related fields oneToOne and oneToMany relations automatic
```GO
q := db.Query()
var customer Customer

//fills in the dependent fields after the fetch
err := q.Where("id = ?", 1).First(&customer)
q.Dependent(&customer, "Addresses", "Telephone")

//or direct by specifying the columns to populate 
var customers []Customer
err := q.DependentColumns("Adresses", "Telephone").Find(&customers)
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

**Auto joins when related columns are queried **
The next stament will join the customer table on the address table
```GO
q := db.Query()
var address Address
err := q.Where("customer.name = ?", "piet").First(&address)
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
- Refactor refactor refactor
- Rewrite this documentation, this is outdated 


Thats it for now, Enjoy
