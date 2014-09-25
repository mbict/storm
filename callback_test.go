package storm

import (
	"errors"
	"reflect"

	. "gopkg.in/check.v1"
)

//test structure
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

//suite
type callbackSuite struct{}

var _ = Suite(&callbackSuite{})

//tests
func (s *callbackSuite) TestRegisterCallback(c *C) {
	v := reflect.ValueOf((*testCallbackStructure)(nil))
	cb := make(callback)

	c.Assert(cb.registerCallback(v, "test"), Equals, false)                         //non existing
	c.Assert(cb.registerCallback(v, "notExportedThusNotRegistable"), Equals, false) //not exported
	c.Assert(cb.registerCallback(v, "InvalidArgument"), Equals, false)              //invalid arguments exported
	c.Assert(cb.registerCallback(v, "NoReturnCallback"), Equals, true)              //no params and return type OK
	c.Assert(cb.registerCallback(v, "NoErrorCallback"), Equals, true)               //params and return
	c.Assert(cb.registerCallback(v, "ErrorCallback"), Equals, true)                 //only return
}

func (s *callbackSuite) TestInvoke(c *C) {
	db := newTestStorm()
	st := &testCallbackStructure{ctx: db}
	v := reflect.ValueOf(st)
	cb := make(callback)

	//register test callbacks
	c.Assert(cb.registerCallback(v, "NoReturnCallback"), Equals, true)
	c.Assert(cb.registerCallback(v, "NoErrorCallback"), Equals, true)
	c.Assert(cb.registerCallback(v, "ErrorCallback"), Equals, true)

	//check no return callback is called
	c.Assert(cb.invoke(v, "NoReturnCallback", nil), IsNil)
	c.Assert(st.noReturnCallbackInvoked, Equals, true)

	//nil error returned test
	c.Assert(cb.invoke(v, "NoErrorCallback", db), IsNil)
	c.Assert(st.noErrorCallbackInvoked, Equals, true)
	c.Assert(st.noErrorCallbackGotContextArg, Equals, true)

	//with return and arguments
	c.Assert(cb.invoke(v, "ErrorCallback", db), NotNil)
	c.Assert(st.errorCallbackInvoked, Equals, true)
}
