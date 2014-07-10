package storm

import (
	"errors"
	"reflect"
	"testing"
)

type testCallbackStructure struct {
	ctx Context

	noReturnCallbackInvoked bool
	noErrorCallbackInvoked  bool
	errorCallbackInvoked    bool

	noErrorCallbackGotContextArg bool
}

func (t *testCallbackStructure) InvalidArgument(test int) {
}

func (t *testCallbackStructure) NoReturnCallback() {
	t.noReturnCallbackInvoked = true
}

func (t *testCallbackStructure) NoErrorCallback(ctx Context) error {
	t.noErrorCallbackInvoked = true

	if t.ctx == ctx {
		t.noErrorCallbackGotContextArg = true
	}
	return nil
}

func (t *testCallbackStructure) ErrorCallback() error {
	t.errorCallbackInvoked = true
	return errors.New("error returned")
}

func (t *testCallbackStructure) notExportedThusNotRegistable() {}

func TestCallback_RegisterCallback(t *testing.T) {
	v := reflect.ValueOf((*testCallbackStructure)(nil))
	c := make(callback)

	//non existing
	if c.registerCallback(v, "test") == true {
		t.Errorf("Expected false, function `test` is not a valid callback")
	}

	//non exported
	if c.registerCallback(v, "notExportedThusNotRegistable") == true {
		t.Errorf("Expected false, function `notExportedThusNotRegistable` is not a valid callback")
	}

	//invalid argument exported
	if c.registerCallback(v, "InvalidArgument") == true {
		t.Errorf("Expected false, function `InvalidArgument` is not a valid callback")
	}

	//no params no return
	if c.registerCallback(v, "NoReturnCallback") == false {
		t.Errorf("Expected true, function `NoReturnCallback` is a valid callback")
	}

	//params and return
	if c.registerCallback(v, "ErrorCallback") == false {
		t.Errorf("Expected true, function `ErrorCallback` is a valid callback")
	}
}

func TestCallback_Invoke(t *testing.T) {

	s := newTestStorm()
	st := &testCallbackStructure{ctx: s}
	v := reflect.ValueOf(st)
	c := make(callback)
	if !c.registerCallback(v, "NoReturnCallback") || !c.registerCallback(v, "NoErrorCallback") || !c.registerCallback(v, "ErrorCallback") {
		t.Fatalf("Cannot register test callbacks for test")
	}

	//no arguments no return
	err := c.invoke(v, "NoReturnCallback", nil)
	if err != nil {
		t.Errorf("Did not expected a error but got one `%v`", err)
	}

	if st.noReturnCallbackInvoked != true {
		t.Errorf("Expected invoked method but its not invoked")
	}

	//nil error returned test
	err = c.invoke(v, "NoErrorCallback", s)
	if err != nil {
		t.Errorf("Did not expected a error but got one `%v`", err)
	}

	if st.noErrorCallbackInvoked != true {
		t.Errorf("Expected invoked method but its not invoked")
	}

	if st.noErrorCallbackGotContextArg != true {
		t.Errorf("Expected query non nil argument")
	}

	//with return and arguments
	err = c.invoke(v, "ErrorCallback", s)
	if err == nil {
		t.Error("Did expected a error but got none")
	}

	if st.errorCallbackInvoked != true {
		t.Errorf("Expected invoked method but its not invoked")
	}
}
