package storm

import (
	"reflect"
	"strings"
)

type TableMap struct {
	//table name
	Name string

	goType  reflect.Type
	columns []*ColumnMap
	keys    []*ColumnMap
}

//create a string representation of this structure
func (a TableMap) String() string {
	r := "TableMap:" + a.Name + "(" + a.goType.String() + ") : \n"

	for i := range a.columns {
		r = r + "\t" + a.columns[i].String() + "\n"
	}

	r = r + "Keys: "
	for i := range a.keys {
		r = r + a.keys[i].Name + " "
	}

	return r
}

//returns all column names
func (a TableMap) columnNames() []string {
	var colNames []string
	for _, column := range a.columns {
		colNames = append(colNames, column.Name)
	}
	return colNames
}

//returns all the key colummns
func (a TableMap) keyNames() []string {
	var colNames []string
	for _, column := range a.keys {
		colNames = append(colNames, column.Name)
	}
	return colNames
}

// -- helper functions

// Parse structure tags like "tagname, tagname(property)" into a map
func parseTags(s string) map[string]string {
	tags := strings.Split(s, ",")
	tagMap := make(map[string]string)
	for _, tag := range tags {
		if len(tag) == 0 {
			continue
		}
		prop := strings.Split(tag, "(")
		if len(prop) == 2 && len(prop[1]) > 1 {
			tagMap[prop[0]] = prop[1][:len(prop[1])-1]
		} else {
			tagMap[tag] = ""
		}
	}
	return tagMap
}

// read out the structure and return the column map
func readStructColumns(in interface{}, depth []int) (cols []*ColumnMap, keys []*ColumnMap) {

	v := reflect.ValueOf(in)
	t := v.Type()

	//no pointers
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
		v = v.Elem()
	}

	n := t.NumField()
	for i := 0; i < n; i++ {
		f := t.Field(i)
		if f.Anonymous && f.Type.Kind() == reflect.Struct {
			//if the embeded element is a structure ignore it for now
			subcols, subkeys := readStructColumns(v.Field(i).Interface(), append(depth, f.Index...))
			cols = append(cols, subcols...)
			keys = append(keys, subkeys...)
			continue
		} else {
			index := append(depth, f.Index...)

			tags := parseTags(f.Tag.Get("db"))

			//ignore tag, or when not exported we ignore it
			_, ok := tags["ignore"]
			if ok || !v.Field(i).CanInterface() {
				continue
			}

			var columnName string = tags["name"]
			if columnName == "" {
				columnName = strings.ToLower(f.Name)
			}

			colMap := &ColumnMap{
				Name:    columnName,
				varName: f.Name,
				goType:  f.Type,
				goIndex: index,
			}
			cols = append(cols, colMap)

			//if this is the primary key we add it
			if _, ok := tags["pk"]; ok {
				keys = append(keys, colMap)
			}
		}
	}

	//try to determine auto pk if no one is defined in a tag
	if len(keys) == 0 {
		for _, col := range cols {

			if col.goType.Kind() == reflect.Int && strings.ToLower(col.Name) == "id" {
				keys = append(keys, col)
				break
			}

		}
	}

	return cols, keys
}
