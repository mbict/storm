[![Build Status](https://travis-ci.org/mbict/storm.png?branch=master)](https://travis-ci.org/mbict/storm)

Storm 
=====

Storm is yet another orm implementation for the go language.

Storm stands for **ST**ructure **O**riented **R**elation **M**odel

Usage
=====


**Create a repository and add a structure**
```GO
//example structure
type Customer struct {
	Id               int    `db:"name(id),pk"
	Firstname	     string 
	Lastname	     string
	Hidden           string `db:"ignore"
}

repository := NewRepository(&SqliteDialect{})
repo.AddStructure(Customer{}, "customer")
```

**Create a storm instance**
```GO
db, err := sql.Open("sqlite3", ":memory:")
if err != nil {
	panic("Cannot open database")
}

storm := NewStorm(db, respository)
```

**Insert a new entity**

Pass the new structure and leave all pks zero
After a successful save all pks will be filled
```GO
newCustomer := Customer{0, "Firstname", "Lastname"}
err := storm.Save(&newCustomer)
```

**Get one entity by its primary key**
```GO
obj, err := storm.Get("customer", 1)

//convert to structure
customer, ok := obj.(*Customer)
```

**Update a entity**
```GO
customer.Lastname = "LastlastName"
err := storm.Save(&customer)
```

**Delete a entity**
```GO
err := storm.Delete(&customer)
```

**Get all the enties method 1**
```GO
query, err := storm.Query("customer")
var customers []Customer
_, err := query.Where("name LIKE ?", "%test%").Select(&customers)
```

**Get all the enties method 2**
```GO
query, err := storm.Query("customer")
var data []interface{}
data, err := query.Where("name LIKE ?", "%test%").Select()
```

**Get the count**
```GO
query, err := storm.Query("customer")
count, err := query.Where("name LIKE ?", "%test%").Count()
```
