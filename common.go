package storm

import "reflect"

//typeIndirect returns the non pointer variant of a type
func typeIndirect(t reflect.Type) reflect.Type {
	if t.Kind() == reflect.Ptr {
		return t.Elem()
	}
	return t
}
