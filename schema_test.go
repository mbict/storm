package storm

import (
	"fmt"
	"github.com/mbict/storm/example"
	"reflect"
	"testing"
)

func TestExtractStructColumns(t *testing.T) {
	cases := []struct {
		description       string
		obj               interface{}
		expectedColumns   int
		expectedRelations int
	}{
		{
			description:       "simplest object",
			obj:               example.Item{},
			expectedColumns:   2,
			expectedRelations: 0,
		},
		{
			description:       "simplest object",
			obj:               example.Note{},
			expectedColumns:   4,
			expectedRelations: 1,
		},
		{
			description:       "embedded object",
			obj:               example.Address{},
			expectedColumns:   3,
			expectedRelations: 0,
		},
		{
			description:       "scanner object",
			obj:               example.Order{},
			expectedColumns:   3,
			expectedRelations: 4,
		},
	}

	for _, testcase := range cases {
		v := reflect.ValueOf(testcase.obj)
		cols, rels := extractStructColumns(v, nil)

		if len(cols) != testcase.expectedColumns {

			for _, col := range cols {
				fmt.Println(col)
			}
			t.Errorf("[%s] expected %d columns but got %d columns", testcase.description, testcase.expectedColumns, len(cols))
		}

		if len(rels) != testcase.expectedRelations {
			t.Errorf("[%s] expected %d relations but got %d relations", testcase.description, testcase.expectedRelations, len(rels))
		}

		//fmt.Println("testing", testcase.description)
		//for _, c := range cols {
		//	fmt.Println("col", c)
		//}
		//
		//for _, c := range rels {
		//	fmt.Println("relational", c.name)
		//}
	}
}

func TestResolveRelations(t *testing.T) {
	orderType := reflect.TypeOf(example.Order{})
	noteType := reflect.TypeOf(example.Note{})
	itemType := reflect.TypeOf(example.Item{})

	tbls := schemes{}
	tbls.add(orderType)
	tbls.add(noteType)
	tbls.add(itemType)
	tbls.add(reflect.TypeOf(example.Tag{}))
	tbls.add(reflect.TypeOf(example.User{}))
	tbls.add(reflect.TypeOf(example.Address{}))

	expectedResults := map[reflect.Type]map[string]struct {
		description       string
		relColumn         string
		relTable          string
		isReverseRelation bool
		isOneToOne        bool
		isOneToMany       bool
		isManyToMany      bool
		isResolved        bool
	}{
		orderType: {
			"items": {
				relTable:     "item",
				isManyToMany: true,
				isResolved:   true,
			},
			"notes": {
				relColumn:   "order_id",
				relTable:    "note",
				isOneToMany: true,
				isResolved:  true,
			},
			"address": {
				relColumn:  "address_id",
				isOneToOne: true,
				isResolved: true,
			},
			"tags": {
				isResolved: false,
			},
		},

		itemType: {
			"order": {
				description:       "reversed relation",
				relTable:          "order",
				isManyToMany:      true,
				isReverseRelation: true,
				isResolved:        true,
			},
		},

		noteType: {
			"reported_by_user": {
				description: "field name is not the same as type name",
				relColumn:   "reported_by_user_id",
				isOneToOne:  true,
				isResolved:  true,
			},
		},
	}

	for tt, expectedRelations := range expectedResults {
		for _, r := range tbls[tt].relations {

			//find the relation
			relationName := ""
			if r.isReverseRelation() {
				relationName = r.relSchema.name
			} else {
				relationName = r.column.name
			}

			if expected, ok := expectedRelations[relationName]; ok {

				//check on rel schema
				if expected.relTable == "" && r.relSchema != nil {
					t.Errorf("[%s] expected relSchema to be empty but is '%s'", r.column.name, r.relSchema.name)
				} else if expected.relTable != "" {
					if r.relSchema == nil {
						t.Errorf("[%s] expected relSchema to be '%s' but is empty", r.column.name, expected.relTable)
					} else if expected.relTable != r.relSchema.name {
						t.Errorf("[%s] expected relSchema to match '%s' but is '%s'", r.column.name, expected.relTable, r.relSchema.name)
					}
				}

				//check on rel column
				if expected.relColumn == "" && r.relColumn != nil {
					t.Errorf("[%s] expected relColumn to be empty but is '%s'", r.column.name, r.relColumn.name)
				} else if expected.relColumn != "" {
					if r.relColumn == nil {
						t.Errorf("[%s] expected relColumn to be '%s' but is empty", r.column.name, expected.relColumn)
					} else if expected.relColumn != r.relColumn.name {
						t.Errorf("[%s] expected relColumn to match '%s' but is '%s'", r.column.name, expected.relColumn, r.relColumn.name)
					}
				}

				//check on relation types
				if expected.isManyToMany != r.isManyToMany() {
					t.Errorf("[%s] expected manyToMany to be %s but was %s", r.column.name, expected.isManyToMany, r.isManyToMany())
				}

				if expected.isOneToMany != r.isOneToMany() {
					t.Errorf("[%s] expected ontToMany to be %s but was %s", r.column.name, expected.isOneToMany, r.isOneToMany())
				}

				if expected.isOneToOne != r.isOneToOne() {
					t.Errorf("[%s] expected oneToOne to be %s but was %s", r.column.name, expected.isOneToOne, r.isOneToOne())
				}

				if expected.isResolved != r.isResolved() {
					t.Errorf("[%s] expected isResolved should be %v but was %v", r.column.name, expected.isResolved, r.isResolved())
				}

				if expected.isReverseRelation != r.isReverseRelation() {
					t.Errorf("[%s] expected isReverseRelation should be %v but was %v", r.column.name, expected.isReverseRelation, r.isReverseRelation())
				}
			} else {
				t.Errorf("[%s] unexpected relation", r.column.name)
			}

		}
	}
}
