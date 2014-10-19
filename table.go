package storm

import (
	"bytes"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"
)

type column struct {
	columnName string
	settings   map[string]string
	goType     reflect.Type
	goIndex    []int
	isScanner  bool
}

type relation struct {
	name           string
	relTable       *table
	relColumn      *column
	goType         reflect.Type
	goSingularType reflect.Type
	goIndex        []int
}

type table struct {
	tableName string
	goType    reflect.Type
	columns   []*column
	relations []*relation
	keys      []*column
	aiColumn  *column
	callbacks callback
}

func newTable(v reflect.Value) *table {

	//read the structure
	cols, rels := extractStructColumns(reflect.Indirect(v), nil)
	pks := findPKs(cols)

	//scan for callbacks
	cb := make(callback)
	cb.registerCallback(v, "OnInsert")
	cb.registerCallback(v, "OnPostInsert")
	cb.registerCallback(v, "OnUpdate")
	cb.registerCallback(v, "OnPostUpdate")
	cb.registerCallback(v, "OnDelete")
	cb.registerCallback(v, "OnPostDelete")
	cb.registerCallback(v, "OnInit")

	t := reflect.Indirect(v).Type()

	//create the table structure
	return &table{
		tableName: camelToSnake(t.Name()),
		goType:    t,
		columns:   cols,
		relations: rels,
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
func extractStructColumns(v reflect.Value, index []int) (cols []*column, rels []*relation) {

	t := v.Type()
	n := t.NumField()
	for i := 0; i < n; i++ {
		f := t.Field(i)

		if f.Anonymous && f.Type.Kind() == reflect.Struct {
			//if the embeded element is a structure ignore it for now
			subcols, subrels := extractStructColumns(v.Field(i), append(index, f.Index...))
			cols = append(cols, subcols...)
			rels = append(rels, subrels...)
			continue
		} else {
			tags := parseTags(f.Tag.Get("db"))

			//ignore tag, or when not exported we ignore it
			if _, ok := tags["ignore"]; ok || !v.Field(i).CanInterface() {
				continue
			}

			var columnName = tags["name"]
			if columnName == "" {
				columnName = camelToSnake(f.Name)
			}
			t := f.Type

			//slices are threat like relational one to many (expect byte slices)
			isScannerCol := isScanner(f.Type)
			if f.Type.Kind() == reflect.Slice && f.Type != reflect.TypeOf([]byte{}) {

				//get the singular type for table lookup
				bt := t.Elem()
				if bt.Kind() == reflect.Ptr {
					bt = bt.Elem()
				}

				rels = append(rels, &relation{
					name:           columnName,
					goType:         t,
					goSingularType: bt,
					goIndex:        append(index, f.Index...),
				})
				continue

				//all structs are handled as relations / except when they implements the scanner interface
			} else if !isScannerCol && !isTime(f.Type) && (f.Type.Kind() == reflect.Struct || (f.Type.Kind() == reflect.Ptr && f.Type.Elem().Kind() == reflect.Struct)) {

				rels = append(rels, &relation{
					name:           columnName,
					goType:         t,
					goSingularType: t,
					goIndex:        append(index, f.Index...),
				})
				continue
			}

			//ignore tag, or when not exported we ignore it
			if overideType, ok := tags["type"]; ok {
				switch overideType {
				case "int":
					t = reflect.TypeOf(int(0))
				case "string":
					t = reflect.TypeOf(string(""))
				default:
					panic(fmt.Sprintf("unkown override type `%s`", overideType))
				}

				if !f.Type.ConvertibleTo(t) {
					panic(fmt.Sprintf("cannot override type `%s` with `%s`", f.Type, t))
				}
			}

			cols = append(cols, &column{
				columnName: columnName,
				settings:   tags,
				goType:     t,
				goIndex:    append(index, f.Index...),
				isScanner:  isScannerCol,
			})
		}
	}

	return
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

func isScanner(t reflect.Type) bool {
	_, isScanner := reflect.New(t).Interface().(sql.Scanner)
	return isScanner
}

var timeType = reflect.TypeOf(time.Time{})

func isTime(t reflect.Type) bool {
	return t == timeType || t.AssignableTo(timeType)
}
