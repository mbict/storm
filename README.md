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

type User struct {
	Id int
	Name string
}

type Address struct {
	Id Int
	Street string
	City string
}

type Item struct {
	Id int
	Description string
}

type Order struct {
	Id      int
	Items []*Item
}

type Message struct {
	Id int
	OrderId int	
	
   	ReportedByUserId int
   	ReportedByUser   *User
}


### Get all messages for a specific order by id
```
var messages []*Message

db.Where("Order = ?", 1234).Find(&messages)
db.Where("Order = ?", 1234).Find(&messages)
```

### Get all messages reported by a specific user
```
var Messages []*Message

db.Where("ReportedByUser = ?", 1234).Find(&messages)
```


