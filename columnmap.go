package storm

import (
	"reflect"
)

type ColumnMap struct {
	Name string

	fieldName string
	goType    reflect.Type
}

func (a *ColumnMap) String() string {
	return "ColumnMap:" + a.Name + "(" + a.goType.String() + ")"
}
