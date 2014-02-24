package storm

import (
	"fmt"
	"reflect"
)

type callback map[string]reflect.Method

// -- helper functions
var (
	transactionType = reflect.TypeOf((*Transaction)(nil))
	queryType       = reflect.TypeOf((*Query)(nil))
	stormType       = reflect.TypeOf((*Storm)(nil))
	errorType       = reflect.TypeOf((error)(nil))
)

func (this callback) invoke(v reflect.Value, callMethod string, tx *Transaction, q *Query, s *Storm) error {
	t, ok := this[callMethod]
	if !ok {
		return nil
	}

	//because we took the type variant we ignore the first element (structure pointer)
	var in = make([]reflect.Value, t.Type.NumIn()-1)
	for i := 0; i < t.Type.NumIn()-1; i++ {
		argType := t.Type.In(i + 1)
		switch argType {
		case transactionType:
			in[i] = reflect.ValueOf(tx)
		case queryType:
			in[i] = reflect.ValueOf(q)
		case stormType:
			in[i] = reflect.ValueOf(s)
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
		case transactionType:
		case queryType:
		case stormType:
		default:
			return false
		}
	}

	this[callMethod] = method
	return true
}
