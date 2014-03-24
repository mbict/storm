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

s, err := storm.Open("sqlite3", ":memory:")
s.AddStructure(Customer{}, "customer")
s.AddStructure((*Customer)(nil), "customer")
```

**Insert a new entity**

Pass the new structure and leave all pks zero
After a successful save all pks will be filled
```GO
newCustomer := Customer{0, "Firstname", "Lastname"}
err := s.Save(&newCustomer)
```

**Get one entity by its primary key**
```GO
var customer Customer
obj, err := s.Find(&customer, 1)
```

**Update a entity**
```GO
customer.Lastname = "LastlastName"
err := s.Save(&customer)
```

**Delete a entity**
```GO
err := s.Delete(&customer)
```

**Get all the enties method 1**
```GO
q := s.Query()
var customers []Customer
_, err := q.Where("name LIKE ?", "%test%").Select(&customers)
```



**Get the count**
```GO
q := s.Query()
count, err := q.Where("name LIKE ?", "%test%").Count((*Customer)(nil))
```


**Start transaction, commit or rollback**
```GO
tx := s.Begin()
tx.Save(...... etc
tx.Commit()
or
tx.Rollback()
```


**Create table**
```GO
s.CreateTable((*Customer)(nil))
```


**Drop table**
```GO
s.DropTable((*Customer)(nil))
```
