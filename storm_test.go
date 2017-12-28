package storm

import (
	"reflect"
	"github.com/mbict/storm/example"
)

func testSchemes() schemes {
	schemes := schemes{}
	schemes.add(reflect.TypeOf(example.Order{}))
	schemes.add(reflect.TypeOf(example.Note{}))
	schemes.add(reflect.TypeOf(example.Item{}))
	schemes.add(reflect.TypeOf(example.Tag{}))
	schemes.add(reflect.TypeOf(example.User{}))
	schemes.add(reflect.TypeOf(example.Address{}))

	return schemes
}