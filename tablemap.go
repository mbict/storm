package storm

import (
	"reflect"
	"strings"
)

type TableMap struct {
	Name string

	goType  reflect.Type
	columns []*ColumnMap
	keys    []*ColumnMap
}

func (a *TableMap) String() string {
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
func readStructColumns(t reflect.Type) (cols []*ColumnMap, keys []*ColumnMap) {
	n := t.NumField()
	for i := 0; i < n; i++ {
		f := t.Field(i)
		if f.Anonymous && f.Type.Kind() == reflect.Struct {
			//if the embeded element is a structure ignore it for now
			//subcols, subkeys := readStructColumns(f.Type)
			//cols = append(cols, subcols...)
			//keys = append(keys, subkeys...)
			continue
		} else {
			tags := parseTags(f.Tag.Get("db"))

			if _, ok := tags["ignore"]; ok {
				continue
			}

			var columnName string = tags["name"]
			if columnName == "" {
				columnName = strings.ToLower(f.Name)
			}
			colMap := &ColumnMap{
				Name:      columnName,
				fieldName: f.Name,
				goType:    f.Type,
			}
			cols = append(cols, colMap)

			//if this is the primary key we add it
			if _, ok := tags["pk"]; ok {
				keys = append(keys, colMap)
			}
		}
	}
	return cols, keys
}
