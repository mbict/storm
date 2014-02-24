package storm

import (
	"fmt"
	"reflect"
)

type callback map[string]reflect.Method

// -- helper functions
var (
	contextType = reflect.TypeOf((*Context)(nil)).Elem()
	errorType   = reflect.TypeOf((error)(nil))
)

func (this callback) invoke(v reflect.Value, callMethod string, ctx Context) error {
	t, ok := this[callMethod]
	if !ok {
		return nil
	}

	//because we took the type variant we ignore the first element (structure pointer)
	var in = make([]reflect.Value, t.Type.NumIn()-1)
	for i := 0; i < t.Type.NumIn()-1; i++ {
		argType := t.Type.In(i + 1)
		switch argType {
		case contextType:
			in[i] = reflect.ValueOf(ctx)
		default:
			return fmt.Errorf("Value for callback argument not found for type %v", argType)
		}
	}

	r := v.Method(t.Index).Call(in)
	if t.Type.NumOut() >= 1 {
		if err, ok := r[0].Interface().(error); ok {
			return err
		}
	}
	return nil
}

func (this callback) registerCallback(v reflect.Value, callMethod string) bool {

	method, ok := v.Type().MethodByName(callMethod)
	if !ok || method.PkgPath != "" {
		return false
	}

	//check if all the argumetns are settable
	for i := 0; i < method.Type.NumIn()-1; i++ {
		argType := method.Type.In(i + 1)
		switch argType {
		case contextType:
			//we know this type
		default:
			return false
		}
	}

	this[callMethod] = method
	return true
}
