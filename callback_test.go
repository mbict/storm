package storm

import (
	"errors"
	"reflect"
	"testing"
)

type testCallbackStructure struct {
	s  *Storm
	q  *Query
	tx *Transaction

	noReturnCallbackInvoked bool
	noErrorCallbackInvoked  bool
	errorCallbackInvoked    bool

	noErrorCallbackGotQueryArg       bool
	noErrorCallbackGotTransactionArg bool
	noErrorCallbackGotStormArg       bool
}

func (t *testCallbackStructure) InvalidArgument(test int) {
}

func (t *testCallbackStructure) NoReturnCallback() {
	t.noReturnCallbackInvoked = true
}

func (t *testCallbackStructure) NoErrorCallback(q *Query, tx *Transaction, s *Storm) error {
	t.noErrorCallbackInvoked = true

	if t.s == s {
		t.noErrorCallbackGotStormArg = true
	}

	if t.q == q {
		t.noErrorCallbackGotQueryArg = true
	}

	if t.tx == tx {
		t.noErrorCallbackGotTransactionArg = true
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
	q := s.Query()
	tx := s.Begin()

	st := &testCallbackStructure{s: s, q: q, tx: tx}
	v := reflect.ValueOf(st)
	c := make(callback)
	if !c.registerCallback(v, "NoReturnCallback") || !c.registerCallback(v, "NoErrorCallback") || !c.registerCallback(v, "ErrorCallback") {
		t.Fatalf("Cannot register test callbacks for test")
	}

	//no arguments no return
	err := c.invoke(v, "NoReturnCallback", nil, nil, nil)
	if err != nil {
		t.Errorf("Did not expected a error but got one `%v`", err)
	}

	if st.noReturnCallbackInvoked != true {
		t.Errorf("Expected invoked method but its not invoked")
	}

	//nil error returned test
	err = c.invoke(v, "NoErrorCallback", tx, q, s)
	if err != nil {
		t.Errorf("Did not expected a error but got one `%v`", err)
	}

	if st.noErrorCallbackInvoked != true {
		t.Errorf("Expected invoked method but its not invoked")
	}

	if st.noErrorCallbackGotQueryArg != true {
		t.Errorf("Expected query non nil argument")
	}

	if st.noErrorCallbackGotTransactionArg != true {
		t.Errorf("Expected transaction non nil argument")
	}

	if st.noErrorCallbackGotStormArg != true {
		t.Errorf("Expected storm argument non nil")
	}

	//with return and arguments
	err = c.invoke(v, "ErrorCallback", tx, q, s)
	if err == nil {
		t.Errorf("Did expected a error but got none", err)
	}

	if st.errorCallbackInvoked != true {
		t.Errorf("Expected invoked method but its not invoked")
	}
}
