package storm

import (
	"database/sql"
	"reflect"
	"time"
)

//indirect returns the non pointer variant of a type
func indirect(t reflect.Type) reflect.Type {
	if t.Kind() == reflect.Ptr {
		return t.Elem()
	}
	return t
}

func isScanner(t reflect.Type) bool {
	_, isScanner := reflect.New(t).Interface().(sql.Scanner)
	return isScanner
}

var timeType = reflect.TypeOf((*time.Time)(nil)).Elem()

func isTime(t reflect.Type) bool {
	return t == timeType || t.AssignableTo(timeType)
}
