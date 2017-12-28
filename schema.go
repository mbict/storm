package storm

import (
	"fmt"
	"github.com/mbict/go-dry/strings/caseing"
	"github.com/mbict/go-tags"
	"reflect"
	"strings"
)

type (
	column struct {
		name      string
		settings  map[string][]string
		goType    reflect.Type
		goIndex   []int
		isScanner bool
	}

	relation struct {
		column         *column
		relSchema      *schema
		relColumn      *column
		goSingularType reflect.Type
	}

	schema struct {
		name      string
		goType    reflect.Type
		columns   []*column
		relations []*relation
		keys      []*column
		aiColumn  *column
	}

	schemes map[reflect.Type]*schema
)


/*****************************************
  Implementation relation structure
 *****************************************/

// isManyToMany indicates a ManyToMany relation that needs a junction schema
func (r *relation) isManyToMany() bool {
	return r.relSchema != nil && r.relColumn == nil
}

// isReverseRelation indicates that the relation is a reverse schema without a column present
func (r *relation) isReverseRelation() bool {
	return r.column == nil
}

func (r *relation) isOneToMany() bool {
	return r.relSchema != nil && r.relColumn != nil
}

func (r *relation) isOneToOne() bool {
	return r.relSchema == nil && r.relColumn != nil
}

func (r *relation) isResolved() bool {
	return r.relSchema != nil || r.relColumn != nil
}

/*****************************************
  Implementation schemes structure
 *****************************************/

func (tbls *schemes) find(t reflect.Type) (*schema, error) {
	if tbl, exists := (*tbls)[t]; exists {
		return tbl, nil
	}
	return nil, fmt.Errorf("no registered structure for `%s` found", t.String())
}

//find table by name
func (tbls *schemes) findByName(name string) (*schema, error) {
	for _, tbl := range *tbls {
		if strings.EqualFold(tbl.name, name) {
			return tbl, nil
		}
	}
	return nil, fmt.Errorf("no registered structure for `%s` found", name)
}

func (tbls *schemes) add(t reflect.Type) error {
	if _, exists := (*tbls)[t]; exists {
		return fmt.Errorf("duplicate structure, '%s' already exists", t.String())
	}

	(*tbls)[t] = newTable(reflect.New(t))
	tbls.resolveRelations()
	tbls.resolveReverseRelations()

	return nil
}

func (tbls *schemes) resolveRelations() {
	for _, tbl := range *tbls {
		for _, rel := range tbl.relations {

			//skip already found relations
			if rel.relSchema != nil || rel.relColumn != nil {
				continue
			}

			//colName := strings.SnakeCase(rel.goSingularType.Name())+ "_id"
			colName := rel.column.name + "_id"

			//find related columns One To One
			for _, relCol := range tbl.columns {
				if strings.EqualFold(relCol.name, colName) {
					rel.relColumn = relCol
					break
				}
			}

			if relTbl, ok := (*tbls)[rel.goSingularType]; ok == true {

				//find related columns One To Many
				for _, relCol := range relTbl.columns {
					if relCol.name == tbl.name+"_id" {
						rel.relSchema = relTbl
						rel.relColumn = relCol
						break
					}
				}

				//find related Many to many
				//no more options this is a manyToMany
				rel.relSchema = relTbl
			}
		}
	}
}

func (tbls *schemes) resolveReverseRelations() {
	findReverseTable := func(tbl *schema, relations []*relation) bool {
		for _, revRel := range relations {
			if revRel.relSchema == tbl {
				return true
			}
		}
		return false
	}

	for _, tbl := range *tbls {
		for _, rel := range tbl.relations {
			//skip all non manyToMany relations
			if !rel.isManyToMany() {
				continue
			}

			if false == findReverseTable(tbl, rel.relSchema.relations) {
				fmt.Println("found reverse and added one")
				rel.relSchema.relations = append(rel.relSchema.relations, &relation{
					column:         nil,
					relSchema:      tbl,
					relColumn:      nil,
					goSingularType: nil,
				})
			}
		}
	}
}

func newTable(v reflect.Value) *schema {

	//read the structure
	cols, rels := extractStructColumns(reflect.Indirect(v), nil)
	pks := findPKs(cols)

	t := reflect.Indirect(v).Type()

	//create the schema structure
	return &schema{
		name:      caseing.SnakeCase(t.Name()),
		goType:    t,
		columns:   cols,
		relations: rels,
		keys:      pks,
		aiColumn:  findAI(cols, pks),
	}
}

// read out the structure and return the column map
func extractStructColumns(v reflect.Value, index []int) (cols []*column, rels []*relation) {
	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)

		//embedded structure
		if f.Anonymous && f.Type.Kind() == reflect.Struct {
			subcols, subrels := extractStructColumns(v.Field(i), append(index, f.Index...))
			cols = append(cols, subcols...)
			rels = append(rels, subrels...)
			continue
		}

		//extract the structure tags
		dbTags, err := tags.ParseMap(f.Tag.Get(TAG))
		if err != nil {
			panic(err)
		}

		//ignore tag, or when not exported we ignore it
		if dbTags == nil || !v.Field(i).CanInterface() {
			continue
		}

		columnName := caseing.SnakeCase(f.Name)
		if len(dbTags["name"]) >= 1 {
			columnName = caseing.SnakeCase(dbTags["name"][0])
		}
		t := f.Type

		isScannerCol := isScanner(f.Type)

		//slices are threat like relational one to many (expect byte slices and scanners)
		if !isScannerCol && f.Type.Kind() == reflect.Slice && f.Type != reflect.TypeOf([]byte{}) {

			//get the singular type for schema lookup
			singularType := indirect(t.Elem())
			rels = append(rels, &relation{
				column: &column{
					name:    columnName,
					goType:  t,
					goIndex: append(index, f.Index...),
				},
				goSingularType: singularType,
			})
			continue
		}

		//all structs are handled as relations / except when they implements the scanner interface
		if !isScannerCol && !isTime(f.Type) && (f.Type.Kind() == reflect.Struct || (f.Type.Kind() == reflect.Ptr && f.Type.Elem().Kind() == reflect.Struct)) {
			rels = append(rels, &relation{
				column: &column{
					name:    columnName,
					goType:  t,
					goIndex: append(index, f.Index...),
				},
				goSingularType: t,
			})
			continue
		}

		if overrideType, ok := dbTags["type"]; ok && len(overrideType) >= 1 {
			switch overrideType[0] {
			case "int":
				t = reflect.TypeOf(int(0))
			case "string":
				t = reflect.TypeOf(string(""))
			default:
				panic(fmt.Sprintf("unkown override type `%s`", overrideType))
			}

			if !f.Type.ConvertibleTo(t) {
				panic(fmt.Sprintf("cannot override type `%s` with `%s`", f.Type, t))
			}
		}

		cols = append(cols, &column{
			name:      columnName,
			settings:  dbTags,
			goType:    t,
			goIndex:   append(index, f.Index...),
			isScanner: isScannerCol,
		})
	}
	return
}

//find primary keys
func findPKs(cols []*column) (pks []*column) {
	for _, col := range cols {
		if _, ok := col.settings["pk"]; ok && (col.goType.Kind() == reflect.Int || col.goType.Kind() == reflect.Int64) {
			pks = append(pks, col)
		}
	}

	//bail out early when we found pks in the structure
	if len(pks) > 0 {
		return
	}

	//try to determine auto pk if no one is defined in a tag
	for _, col := range cols {

		if (col.goType.Kind() == reflect.Int || col.goType.Kind() == reflect.Int64) && strings.ToLower(col.name) == "id" {
			pks = append(pks, col)
			return
		}
	}
	return
}

//find auto increment keys
func findAI(cols []*column, pks []*column) *column {
	for _, col := range cols {
		if _, ok := col.settings["ai"]; ok && (col.goType.Kind() == reflect.Int || col.goType.Kind() == reflect.Int64) {
			return col
		}
	}

	//fallback
	if len(pks) == 1 {
		return pks[0]
	}
	return nil
}
