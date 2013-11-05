package storm

import (
	"reflect"
	"fmt"
)

type ColumnMap struct {
	//name of the field in the datastore
	Name string

	//name of the variable in the struct
	varName string
	goType    reflect.Type
	goIndex []int
}

func (a *ColumnMap) String() string {
	return fmt.Sprintf("ColumnMap: %s (type: %v, index: %v)", a.Name, a.goType.String(), a.goIndex)
}
