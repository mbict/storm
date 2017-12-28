package storm

import (
	"testing"
)

//on item

func TestBuildQuery(t *testing.T) {
	testCases := []struct {
		description string
		model       string
		where       []string
		bindings    []interface{}
		expected    string
	}{
		{
			description: `simple where (no table prefix)`,
			model:       "order",
			where:       []string{`Id = ?`},
			expected:    `SELECT FROM order AS _order WHERE _order.id = ?`,
		}, {
			description: `simple where (with table prefix)`,
			model:       "order",
			where:       []string{`Order.Id = ?`},
			expected:    `SELECT FROM order AS _order WHERE _order.id = ?`,
		},

		//one to many
		{
			description: `one to many where (on pk)`,
			model:       "order",
			where:       []string{`Address.Id = ?`},
			expected:    `SELECT FROM order AS _order JOIN address as _address ON _address.order_id = _order.id WHERE _address.id = ? GROUP BY _order.id`,
		}, {
			description: `one to many where (on pk, with table prefix)`,
			model:       "order",
			where:       []string{`Order.Address.Id = ?`},
			expected:    `SELECT FROM order AS _order JOIN address as _address ON _address.order_id = _order.id WHERE _address.id = ? GROUP BY _order.id`,
		}, {
			description: `one to many where (any field)`,
			model:       "order",
			where:       []string{`Address.Street = ?`},
			expected:    `SELECT FROM order AS _order JOIN address as _address ON _address.order_id = _order.id WHERE _address.street = ? GROUP BY _order.id`,
		}, {
			description: `one to many where (any field, with table prefix)`,
			model:       "order",
			where:       []string{`Address.Street = ?`},
			expected:    `SELECT FROM order AS _order JOIN address as _address ON _address.order_id = _order.id WHERE _address.street = ? GROUP BY _order.id`,
		},

		//one to many reversed
		{
			description: `one to many where (on rel pk column, reversed)`,
			model:       "address",
			where:       []string{`OrderId = ?`},
			expected:    `SELECT FROM address AS _address WHERE _address.order_id = ?`,
		}, {
			description: `one to many where (on rel pk column, reversed, table prefix)`,
			model:       "address",
			where:       []string{`Address.OrderId = ?`},
			expected:    `SELECT FROM address AS _address WHERE _address.order_id = ?`,
		}, {
			description: `one to many where (on rel pk column, reversed, referenced table)`,
			model:       "address",
			where:       []string{`Order.Id = ?`},
			expected:    `SELECT FROM address AS _address WHERE _address.order_id = ?`,
		}, {
			description: `one to many where (on rel pk column, reversed, referenced table, table prefix)`,
			model:       "address",
			where:       []string{`Address.Order.Id = ?`},
			expected:    `SELECT FROM address AS _address WHERE _address.order_id = ?`,
		}, {
			description: `one to many where (any column, reversed, referenced table)`,
			model:       "address",
			where:       []string{`Order.Name = ?`},
			expected:    `SELECT FROM address AS _address JOIN order as _order ON _order.id = _address.order_id WHERE _order.name = ? GROUP BY _address.id`,
		}, {
			description: `one to many where (any column, reversed, referenced table, table prefix)`,
			model:       "address",
			where:       []string{`Address.Order.Name = ?`},
			expected:    `SELECT FROM address AS _address JOIN order as _order ON _order.id = _address.order_id WHERE _order.name = ? GROUP BY _address.id`,
		},

		//many to many, pivot tables
		{
			description: `join trough pivot table (on pk)`,
			model:       "order",
			where:       []string{`Item.Id = ?`},
			expected:    `SELECT FROM order AS _order JOIN rel_item_order AS _rel_item_order ON _rel_item_order.order_id = _order.id AND _rel_item_order.item_id = ? GROUP BY _order.id`,
		}, {
			description: `join trough pivot table (on pk, table prefix)`,
			model:       "order",
			where:       []string{`Order.Item.Id = ?`},
			expected:    `SELECT FROM order AS _order JOIN rel_item_order AS _rel_item_order ON _rel_item_order.order_id = _order.id AND _rel_item_order.item_id = ? GROUP BY _order.id`,
		}, {
			description: `join trough pivot table (on any field)`,
			model:       "order",
			where:       []string{`Item.Name = ?`},
			expected:    `SELECT FROM order AS _order JOIN rel_item_order AS _rel_item_order ON _rel_item_order.order_id = _order.id JOIN item as _item ON item.id = _rel_item_order.item_id WHERE _item.name = ? GROUP BY _order.id`,
		}, {
			description: `join trough pivot table (on any field, table prefix)`,
			model:       "order",
			where:       []string{`Order.Item.Name = ?`},
			expected:    `SELECT FROM order AS _order JOIN rel_item_order AS _rel_item_order ON _rel_item_order.order_id = _order.id JOIN item as _item ON item.id = _rel_item_order.item_id WHERE _item.name = ? GROUP BY _order.id`,
		},

		//many to many reversed
		{
			description: `join trough pivot table (on pk, reversed)`,
			model:       "item",
			where:       []string{`Order.Id = ?`},
			expected:    `SELECT FROM item as _item JOIN rel_item_order as _rel_item_order ON _rel_item_order.item_id = _item.id AND _rel_item_order.order_id = ? GROUP BY _item.id`,
		}, {
			description: `join trough pivot table (on pk, reversed, table prefix)`,
			model:       "item",
			where:       []string{`Item.Order.Id = ?`},
			expected:    `SELECT FROM item as _item JOIN rel_item_order as _rel_item_order ON _rel_item_order.item_id = _item.id AND _rel_item_order.order_id = ? GROUP BY _item.id`,
		}, {
			description: `join trough pivot table (any field, reversed)`,
			model:       "item",
			where:       []string{`Order.Name = ?`},
			expected:    `SELECT FROM item as _item JOIN rel_item_order as _rel_item_order ON _rel_item_order.item_id = _item.id JOIN order AS _order ON _order.id = _rel_item_order.order_id WHERE _order.name = ? GROUP BY _item.id`,
		}, {
			description: `join trough pivot table (any field, reversed, table prefix)`,
			model:       "item",
			where:       []string{`Item.Order.Name = ?`},
			expected:    `SELECT FROM item as _item JOIN rel_item_order as _rel_item_order ON _rel_item_order.item_id = _item.id JOIN order AS _order ON _order.id = _rel_item_order.order_id WHERE _order.name = ? GROUP BY _item.id`,
		},
	}

	testSchemes := testSchemes()
	for _, tc := range testCases {
		schema, err := testSchemes.findByName(tc.model)
		if err != nil {
			t.Error(err)
		}

		buildWhere(DefaultNamingStrategy, schema, tc.where[0], tc.bindings, testSchemes)
	}
}



func TestBuildWhere(t *testing.T) {
	testCases := []struct {
		description string
		model       string
		where       string
		bindings    []interface{}
		expected    string
		expectedError error
	}{
		{
			description: `simple where (no table prefix)`,
			model:       "order",
			where:       `Id = ?`,
			expected:    `_order.id = ?`,
		}, {
			description: `simple where (with table prefix)`,
			model:       "order",
			where:       `Order.Id = ?`,
			expected:    `_order.id = ?`,
		},

		//one to many
		{
			description: `one to many where (on pk)`,
			model:       "order",
			where:       `Address.Id = ?`,
			expected:    `_address.id = ?`,
		}, {
			description: `one to many where (on pk, with table prefix)`,
			model:       "order",
			where:       `Order.Address.Id = ?`,
			expected:    `_address.id = ?`,
		}, {
			description: `one to many where (any field)`,
			model:       "order",
			where:       `Address.Street = ?`,
			expected:    `_address.street = ?`,
		}, {
			description: `one to many where (any field, with table prefix)`,
			model:       "order",
			where:       `Address.Street = ?`,
			expected:    `_address.street = ?`,
		},

		//one to many reversed
		{
			description: `one to many where (on rel pk column, reversed)`,
			model:       "address",
			where:       `OrderId = ?`,
			expected:    `_address.order_id = ?`,
		}, {
			description: `one to many where (on rel pk column, reversed, table prefix)`,
			model:       "address",
			where:       `Address.OrderId = ?`,
			expected:    `_address.order_id = ?`,
		}, {
			description: `one to many where (on rel pk column, reversed, referenced table)`,
			model:       "address",
			where:       `Order.Id = ?`,
			expected:    `_order.id = ?`,
		}, {
			description: `one to many where (on rel pk column, reversed, referenced table, table prefix)`,
			model:       "address",
			where:       `Address.Order.Id = ?`,
			expected:    `_order.id = ?`,
		}, {
			description: `one to many where (any column, reversed, referenced table)`,
			model:       "address",
			where:       `Order.Name = ?`,
			expected:    `_order.name = ?`,
		}, {
			description: `one to many where (any column, reversed, referenced table, table prefix)`,
			model:       "address",
			where:       `Address.Order.Name = ?`,
			expected:    `_order.name = ?`,
		},

		//many to many, pivot tables
		{
			description: `join trough pivot table (on pk)`,
			model:       "order",
			where:       `Item.Id = ?`,
			expected:    `_rel_item_order.item_id = ?`,
		}, {
			description: `join trough pivot table (on pk, table prefix)`,
			model:       "order",
			where:       `Order.Item.Id = ?`,
			expected:    `_rel_item_order.item_id = ?`,
		}, {
			description: `join trough pivot table (on any field)`,
			model:       "order",
			where:       `Item.Name = ?`,
			expected:    `_item.name = ?`,
		}, {
			description: `join trough pivot table (on any field, table prefix)`,
			model:       "order",
			where:       `Order.Item.Name = ?`,
			expected:    `_item.name = ?`,
		},

		//many to many reversed
		{
			description: `join trough pivot table (on pk, reversed)`,
			model:       "item",
			where:       `Order.Id = ?`,
			expected:    `_rel_item_order.order_id = ?`,
		}, {
			description: `join trough pivot table (on pk, reversed, table prefix)`,
			model:       "item",
			where:       `Item.Order.Id = ?`,
			expected:    `_rel_item_order.order_id = ?`,
		}, {
			description: `join trough pivot table (any field, reversed)`,
			model:       "item",
			where:       `Order.Name = ?`,
			expected:    `_order.name = ?`,
		}, {
			description: `join trough pivot table (any field, reversed, table prefix)`,
			model:       "item",
			where:       `Item.Order.Name = ?`,
			expected:    `_order.name = ?`,
		},
	}

	testSchemes := testSchemes()
	for _, tc := range testCases {
		schema, err := testSchemes.findByName(tc.model)
		if err != nil {
			t.Error(err)
		}

		result, err := buildWhere(DefaultNamingStrategy, schema, tc.where, tc.bindings, testSchemes)
		if err != tc.expectedError {
			t.Errorf("[%s] not expecting error `%v` but got: `%v`", tc.description, tc.expectedError, err)
		}

		if result != tc.expected {
			t.Errorf("[%s] expected where `%s` but got: `%s`", tc.description, tc.expected, result)
		}
	}
}
