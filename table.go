package storm

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"
)

type column struct {
	columnName string
	settings   map[string]string
	goType     reflect.Type
	goIndex    []int
}

type table struct {
	tableName string
	goType    reflect.Type
	columns   []*column
	keys      []*column
	aiColumn  *column
	callbacks callback
}

func newTable(v reflect.Value, name string) *table {

	//read the structure
	cols := extractStructColumns(v, nil)
	pks := findPKs(cols)

	//scan for callbacks
	cb := make(callback)
	cb.registerCallback(v, "BeforeInsert")
	cb.registerCallback(v, "AfterInsert")
	cb.registerCallback(v, "BeforeUpdate")
	cb.registerCallback(v, "AfterUpdate")
	cb.registerCallback(v, "BeforeDelete")
	cb.registerCallback(v, "AfterDelete")
	cb.registerCallback(v, "BeforeFind")
	cb.registerCallback(v, "AfterFind")

	//create the table structure
	return &table{
		tableName: name,
		goType:    v.Type(),
		columns:   cols,
		keys:      pks,
		aiColumn:  findAI(cols, pks),
		callbacks: cb,
	}
}

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
func extractStructColumns(v reflect.Value, index []int) (cols []*column) {

	t := v.Type()
	n := t.NumField()
	for i := 0; i < n; i++ {
		f := t.Field(i)

		if f.Anonymous && f.Type.Kind() == reflect.Struct {
			//if the embeded element is a structure ignore it for now
			subcols := extractStructColumns(v.Field(i), append(index, f.Index...))
			cols = append(cols, subcols...)
			continue
		} else {
			tags := parseTags(f.Tag.Get("db"))

			//ignore tag, or when not exported we ignore it
			if _, ok := tags["ignore"]; ok || !v.Field(i).CanInterface() {
				continue
			}

			var columnName string = tags["name"]
			if columnName == "" {
				columnName = camelToSnake(f.Name)
			}

			//ignore tag, or when not exported we ignore it
			t := f.Type
			if overideType, ok := tags["type"]; ok {
				switch overideType {
				case "int":
					t = reflect.TypeOf(int(0))
				case "string":
					t = reflect.TypeOf(string(""))
				default:
					panic(fmt.Sprintf("Unkown override type `%s`", overideType))
				}

				if !f.Type.ConvertibleTo(t) {
					panic(fmt.Sprintf("cannot override type `%s` with `%s`", f.Type, t))
				}
			}

			col := &column{
				columnName: columnName,
				settings:   tags,
				goType:     t,
				goIndex:    append(index, f.Index...),
			}
			cols = append(cols, col)
		}
	}

	return cols
}

//find primary keys
func findPKs(cols []*column) (pks []*column) {

	for _, col := range cols {
		if _, ok := col.settings["pk"]; ok && col.goType.Kind() == reflect.Int {
			pks = append(pks, col)
		}
	}

	//bail out early when we found pks in the structure
	if len(pks) > 0 {
		return
	}

	//try to determine auto pk if no one is defined in a tag
	for _, col := range cols {

		if col.goType.Kind() == reflect.Int && strings.ToLower(col.columnName) == "id" {
			pks = append(pks, col)
			return
		}
	}
	return
}

//find auto increment keys
func findAI(cols []*column, pks []*column) *column {

	for _, col := range cols {
		if _, ok := col.settings["ai"]; ok && col.goType.Kind() == reflect.Int {
			return col
		}
	}

	//fallback
	if len(pks) == 1 {
		return pks[0]
	}
	return nil
}

func camelToSnake(u string) string {

	buf := bytes.NewBufferString("")
	for i, v := range u {
		if i > 0 && v >= 'A' && v <= 'Z' {
			buf.WriteRune('_')
		}
		buf.WriteRune(v)
	}

	return strings.ToLower(buf.String())
}

func snakeToCamel(s string) string {

	buf := bytes.NewBufferString("")
	for _, v := range strings.Split(s, "_") {
		if len(v) > 0 {
			buf.WriteString(strings.ToUpper(v[:1]))
			buf.WriteString(v[1:])
		}
	}

	return buf.String()
}
