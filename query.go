package storm

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"github.com/mbict/go-dry/strings/caseing"
	"reflect"
	"regexp"
	"strings"
)

//ASC ascending order
//DESC descending order
const (
	ASC  SortDirection = "ASC"
	DESC SortDirection = "DESC"
)

//SortDirection indicates the sort direction used in Order
type (
	SortDirection string

	where struct {
		Statement string
		Bindings  []interface{}
	}

	order struct {
		Statement string
		Direction SortDirection
	}

	Query interface {
		Query() Query

		Order(column string, direction SortDirection) Query
		Limit(int) Query
		Offset(int) Query

		FetchRelated(columns ...string) Query
		Where(condition string, bindAttr ...interface{}) Query

		Count(interface{}) (int64, error)
		//CountContext(context.Context, interface{}) (int64, error)
		Find(i interface{}, where ...interface{}) error
		//FindContext(ctx context.Context, i interface{}, where ...interface{}) error
		FindRelated(i interface{}, columns ...string) error
		//FindRelatedContext(ctx context.Context, i interface{}, columns ...string) error
		First(interface{}) error
		//FirstContext(context.Context, interface{}) error
	}

	//Query structure
	query struct {
		ctx    dbContext
		where  []where
		order  []order
		offset int
		limit  int

		relatedColumnFetch bool //not needed
		relatedColumns     []string

		joins   map[string]*schema
		groupby bool //not needed
	}

	related struct {
		index          [][]int
		relatedColumns []string
		rel            *relation
	}

	scanObject struct {
		index [][]int
		tbl   *schema
	}
)

func newQuery(ctx dbContext, parent *query) *query {

	q := &query{
		ctx:    ctx,
		offset: -1,
		limit:  -1,
	}

	if parent != nil {
		//clone parent
		q.where = make([]where, len(parent.where))
		copy(q.where, parent.where)
		q.order = make([]order, len(parent.order))
		copy(q.order, parent.order)
		q.relatedColumns = make([]string, len(parent.relatedColumns))
		copy(q.relatedColumns, parent.relatedColumns)
		q.joins = make(map[string]*schema, len(parent.joins))
		for key, value := range parent.joins {
			q.joins[key] = value
		}
		q.offset = parent.offset
		q.limit = parent.limit
		q.relatedColumnFetch = parent.relatedColumnFetch
		q.groupby = parent.groupby
	} else {
		q.where = make([]where, 0)
		q.order = make([]order, 0)
	}
	return q
}

//Query Creates a clone of the current query object
func (q *query) Query() Query {
	return newQuery(q.ctx, q)
}

//Order will set the order
//Example:
// q.Order("columnnname", storm.ASC)
// q.Order("columnnname", storm.DESC)
func (q *query) Order(column string, direction SortDirection) Query {
	q.order = append(q.order, order{column, direction})
	return q
}

//Where adds new where conditions to the query
//Example:
// q.Where("column = 1") //textual condition
// q.Where("column = ?", 1) //bind params
// q.Where("(column = ? OR other = ?)",1,2) //multiple bind params
func (q *query) Where(condition string, bindAttr ...interface{}) Query {

	var bindVars []interface{}
	for _, val := range bindAttr {
		switch val.(type) {
		case string, int:
			bindVars = append(bindVars, val)
			continue
		}

		//if known structure we probably know how to extract the pk
		v := reflect.Indirect(reflect.ValueOf(val))
		if v.Kind() == reflect.Struct {
			if tbl, ok := q.ctx.table(v.Type()); ok {
				if nil != tbl.aiColumn {
					bindVars = append(bindVars, v.FieldByIndex(tbl.aiColumn.goIndex).Int())
					continue
				} else if len(tbl.keys) >= 1 {
					bindVars = append(bindVars, v.FieldByIndex(tbl.keys[0].goIndex).Int())
					continue
				}
			}
		}
		bindVars = append(bindVars, val)
	}
	q.where = append(q.where, where{Statement: condition, Bindings: bindVars})
	return q
}

//Limit sets the limit for select
func (q *query) Limit(limit int) Query {
	q.limit = limit
	return q
}

//Offset sets the offset for select
func (q *query) Offset(offset int) Query {
	q.offset = offset
	return q
}

//FetchRelated will set the dependent fetch mode for Find and First.
//When set all or only the provided columns who are dependent will be populated when fetched
func (q *query) FetchRelated(columns ...string) Query {
	q.relatedColumnFetch = true
	q.relatedColumns = append(q.relatedColumns, columns...)
	return q
}

//Find will try to retreive the matching structure/entity based on your where statement
//you can provide a slice or a single element
func (q *query) Find(i interface{}, where ...interface{}) error {
	if len(where) >= 1 {
		q = newQuery(q.ctx, q)
	}

	//slice given
	if reflect.Indirect(reflect.ValueOf(i)).Kind() == reflect.Slice {
		return q.fetchAll(i, where...)
	}

	//single dbContext
	return q.fetchRow(i, where...)
}

//First will execute the query and return one result to i
//Example:
// var result *TestModel
// q.First(&result)
func (q *query) First(i interface{}) error {
	return q.fetchRow(i)
}

//Count will execute a query and return the resulting rows Select will return
//Example:
// count, err := q.Count((*TestModel)(nil))
func (q *query) Count(i interface{}) (int64, error) {
	return q.fetchCount(i)
}

//Dependent will try to fetch all the related enities and populate the dependent fields (slice and single values)
//You can provide a list with column names if you only want those fields to be populated
func (q *query) FindRelated(i interface{}, columns ...string) error {
	v := reflect.ValueOf(i)
	if v.Kind() != reflect.Ptr {
		return errors.New("provided input is not by reference")
	}

	v = v.Elem()
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return errors.New("Cannot get dependent fields on nil struct")
		}
	}
	v = reflect.Indirect(v)
	if v.Kind() != reflect.Struct || !v.CanSet() {
		return errors.New("provided input is not a structure type")
	}

	//find the schema
	tbl, ok := q.ctx.table(v.Type())
	if !ok {
		return fmt.Errorf("no registered structure for `%s` found", v.Type().String())
	}

	//group similar related
	related := make(map[string][]string)
	for _, col := range columns {
		parts := strings.Split(col, ".")
		col = caseing.SnakeCase(parts[0])

		if len(parts) > 1 {
			related[col] = append(related[col], strings.Join(parts[1:], "."))
		} else {
			//init a empty related array if not present
			if _, ok := related[col]; !ok {
				related[col] = []string{}
			}
		}
	}

	//fetch the related fields
	for col, relatedColumns := range related {
		for _, rel := range tbl.relations {
			if strings.EqualFold(rel.column.name, col) {
				if err := q.fetchRelatedColumn(v, tbl, rel, relatedColumns); err != nil {
					return err
				}
				break
			}
		}
	}
	return nil
}

func (q *query) fetchRelatedColumn(v reflect.Value, tbl *schema, rel *relation, relatedColumns []string) error {
	elm := v.FieldByIndex(rel.column.goIndex)
	dst := elm.Addr().Interface()
	if rel.isOneToOne() {
		relVal := v.FieldByIndex(rel.relColumn.goIndex)

		//on nil value early out no lookup
		if relVal.Kind() == reflect.Ptr && relVal.IsNil() {
			if elm.Kind() == reflect.Ptr && elm.IsNil() == false {
				elm.Set(reflect.Zero(elm.Type()))
			}
			return nil
		}
		val := relVal.Interface()

		//check if val is not empty or 0 to avoid lookups who will result in no rows
		if valuer, ok := val.(driver.Valuer); ok {
			val, _ = valuer.Value()
			if val == nil {
				//empty valuer
				if elm.Kind() == reflect.Ptr && elm.IsNil() == false {
					elm.Set(reflect.Zero(elm.Type()))
				}
				return nil
			}
		}

		if val, ok := val.(int64); ok {
			if val == 0 {
				//empty int
				if elm.Kind() == reflect.Ptr && elm.IsNil() == false {
					elm.Set(reflect.Zero(elm.Type()))
				}
				return nil
			}
		}

		err := newQuery(q.ctx, nil).
			FetchRelated(relatedColumns...).
			Where("id = ?", val).
			Find(dst)

		if err == sql.ErrNoRows {
			//if there are no results we reset the column if its a pointer to nil
			if elm.Kind() == reflect.Ptr && elm.IsNil() == false {
				elm.Set(reflect.Zero(elm.Type()))
			}
		} else if err != nil {
			return err
		}
	} else if rel.isOneToMany() {
		val := v.FieldByIndex(tbl.aiColumn.goIndex).Interface()
		err := newQuery(q.ctx, nil).
			FetchRelated(relatedColumns...).
			Where(tbl.name+"_id = ?", val).
			Find(dst)
		if err != nil && err != sql.ErrNoRows {
			return err
		}
	} else if rel.isManyToMany() {
		/* @todo implement the fetch manyToMany */

		//pivotTable := "rel_"+tbl.name+"_"+rel.relSchema.name

		fmt.Println(">>>", tbl.name)
		fmt.Println("xxx", rel.column)
		fmt.Println("xxx", rel.relSchema)

		val := v.FieldByIndex(tbl.aiColumn.goIndex).Interface()
		err := newQuery(q.ctx, nil).
			FetchRelated(relatedColumns...).
			//Where(pivotTable+"."+tbl.name+"_id = ?", val).
			//Where(rel.relSchema.name+"."+tbl.name+"_id = ?", val).
			Where(tbl.name+".id = ?", val).


			Find(dst)
		if err != nil && err != sql.ErrNoRows {
			return err
		}



	}
	return nil
}

//create additional where stements from arguments
func (q *query) applyWhere(tbl *schema, where ...interface{}) error {
	switch t := where[0].(type) {
	case string:
		q.Where(t, where[1:]...)
	case int, int8, int16, int32, uint, uint8, uint16, uint32, int64, uint64, sql.NullInt64:
		if len(tbl.keys) == 1 {
			if len(where) == 1 {
				q.Where(fmt.Sprintf("%s = ?", tbl.keys[0].name), where...)
			} else {
				return errors.New("not implemented having multiple pk values for find")
			}
		} else {
			return errors.New("not implemented having multiple pks for find")
		}
	default:
		v := reflect.Indirect(reflect.ValueOf(t))
		if v.Kind() == reflect.Struct {
			if tbl, ok := q.ctx.table(v.Type()); ok {
				condition := fmt.Sprintf("%s = ?", tbl.name+"_id")
				if nil != tbl.aiColumn {
					q.Where(condition, v.FieldByIndex(tbl.aiColumn.goIndex).Int())
					return nil
				} else if len(tbl.keys) >= 1 {
					q.Where(condition, v.FieldByIndex(tbl.keys[0].goIndex).Int())
					return nil
				}
			}
		}
		return errors.New("unsupported pk find type")
	}
	return nil
}

//fetch a single row into a element
func (q *query) fetchCount(i interface{}) (cnt int64, err error) {
	t := reflect.TypeOf(i)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct {
		return 0, errors.New("provided input is not a structure type")
	}

	//find the schema
	tbl, ok := q.ctx.table(t)
	if !ok {
		return 0, fmt.Errorf("no registered structure for `%s` found", t)
	}

	//generate sql and prepare
	sqlQuery, bind, err := q.generateCountSQL(tbl)
	if err != nil {
		return 0, err
	}

	if q.ctx.logger() != nil {
		q.ctx.logger().Printf("`%s` binding : %v", sqlQuery, bind)
	}

	stmt, err := q.ctx.db().Prepare(sqlQuery)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	//q the row
	row := stmt.QueryRow(bind...)

	//create destination and scan
	err = row.Scan(&cnt)
	return cnt, err
}

//fetch a single row into a element
func (q *query) fetchRow(i interface{}, where ...interface{}) (err error) {
	v := reflect.ValueOf(i)
	if v.Kind() != reflect.Ptr {
		return errors.New("provided input is not by reference")
	}

	//reset element to zero variant
	v_ptr := v.Elem()
	var dst reflect.Value
	hasEmpty := v_ptr.Kind() == reflect.Ptr && v_ptr.CanSet()
	if hasEmpty {
		dst = reflect.New(v_ptr.Type().Elem())
		v = reflect.Indirect(dst)
	} else {
		v = reflect.Indirect(v_ptr)
	}

	if v.Kind() != reflect.Struct || !v.CanSet() {
		return errors.New("provided input is not a structure type")
	}

	//find the schema
	tbl, ok := q.ctx.table(v.Type())
	if !ok {
		return fmt.Errorf("no registered structure for `%s` found", v.Type().String())
	}

	//add the last minute where
	if len(where) >= 1 {
		if err = q.applyWhere(tbl, where...); err != nil {
			return err
		}
	}

	//generate sql and prepare
	sqlQuery, bind, remainingRelatedColumns, scanObjects, err := q.generateSelectSQL(tbl)
	fmt.Println("0>>>", remainingRelatedColumns)
	if err != nil {
		return err
	}
	if q.ctx.logger() != nil {
		q.ctx.logger().Printf("`%s` binding : %v", sqlQuery, bind)
	}

	stmt, err := q.ctx.db().Prepare(sqlQuery)
	if err != nil {
		return err
	}
	defer stmt.Close()

	//q the row
	row := stmt.QueryRow(bind...)

	//create scan destination
	dest := make([]interface{}, len(tbl.columns))
	for key, col := range tbl.columns {
		dest[key] = v.FieldByIndex(col.goIndex).Addr().Interface()
	}

	//create dependent scan destination
	for _, scanObj := range scanObjects {
		vc := reflect.New(scanObj.tbl.goType)

		//find path and assign
		target := reflect.Indirect(v).FieldByIndex(scanObj.index[0])
		for _, path := range scanObj.index[1:] {
			target = target.Elem().FieldByIndex(path)
		}
		target.Set(vc)

		for _, col := range scanObj.tbl.columns {
			dest = append(dest, vc.Elem().FieldByIndex(col.goIndex).Addr().Interface())
		}
	}

	err = row.Scan(dest...)

	if err != nil {
		return err
	}

	/* @todo implement dbContext */
	if cb, ok := v.Addr().Interface().(OnInitCallback); ok {
		if err := cb.OnInit(nil, v.Addr().Interface()); err != nil {
			return err
		}
	}

	//overwrite input with new values
	if hasEmpty {
		v_ptr.Set(dst)
	}

	//if input was a nil pointer we overwrite the nil with the binded value
	if q.relatedColumnFetch == true && len(remainingRelatedColumns) > 0 {
		for _, relatedColumn := range remainingRelatedColumns {
			currentTbl := tbl
			vTarget := v.Addr()

			//walk the structures to find the depended structure (ignoring the last index)
			for _, index := range relatedColumn.index[:len(relatedColumn.index)-1] {
				vTarget = vTarget.Elem().FieldByIndex(index)
				if currentTbl, ok = q.ctx.table(indirect(vTarget.Type())); !ok {
					return fmt.Errorf("Depend cannot find schema, not registered %s, %s", indirect(vTarget.Type()))
				}
			}

			if err := q.fetchRelatedColumn(vTarget.Elem(), currentTbl, relatedColumn.rel, relatedColumn.relatedColumns); err != nil {
				return err
			}
		}
	}
	return nil
}

//fetch a all rows into a slice
func (q *query) fetchAll(i interface{}, where ...interface{}) (err error) {

	ts := reflect.TypeOf(i)
	if ts.Kind() != reflect.Ptr {
		return errors.New("provided input is not by reference")
	}

	if ts.Elem().Kind() != reflect.Slice {
		return errors.New("provided input is not a slice")
	}

	//get the element type
	t := ts.Elem().Elem()
	var sliceTypeIsPtr = false
	if t.Kind() == reflect.Ptr {
		sliceTypeIsPtr = true
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct {
		return errors.New("provided input slice has no structure type")
	}

	//find the schema
	tbl, ok := q.ctx.table(t)
	if !ok {
		return fmt.Errorf("no registered structure for `%s` found", t.String())
	}

	//add the last minute where
	if len(where) >= 1 {
		if err = q.applyWhere(tbl, where...); err != nil {
			return err
		}
	}

	//generate sql and prepare
	sqlQuery, bind, remainingDepends, scanObjects, err := q.generateSelectSQL(tbl)
	if err != nil {
		return err
	}

	if q.ctx.logger() != nil {
		q.ctx.logger().Printf("`%s` binding : %v", sqlQuery, bind)
	}

	stmt, err := q.ctx.db().Prepare(sqlQuery)
	if err != nil {
		return err
	}
	defer stmt.Close()

	//q for the results
	rows, err := stmt.Query(bind...)
	if err != nil {
		return err
	}
	defer rows.Close()

	vs := reflect.ValueOf(i).Elem()
	vs.SetLen(0)

	for {
		if !rows.Next() {
			// if error occurred return rawselect
			if rows.Err() != nil {
				return rows.Err()
			} /*else if vs.Len() == 0 {
				return sql.ErrNoRows
			}*/
			rows.Close()

			sliceLen := vs.Len()
			for i := 0; i < sliceLen; i++ {
				elem := reflect.Indirect(vs.Index(i)).Addr()

				/* @todo implement dbContext */
				if cb, ok := elem.Interface().(OnInitCallback); ok {
					if err := cb.OnInit(nil, elem.Interface()); err != nil {
						return err
					}
				}

				if q.relatedColumnFetch == true && len(remainingDepends) > 0 {
					for _, depend := range remainingDepends {
						currentTbl := tbl
						vTarget := elem

						//walk the structures to find the depended structure (ignoring the last index)
						for _, index := range depend.index[:len(depend.index)-1] {
							vTarget = vTarget.Elem().FieldByIndex(index)
							if currentTbl, ok = q.ctx.table(indirect(vTarget.Type())); !ok {
								return fmt.Errorf("Depend cannot find schema, not registered %s, %s", indirect(vTarget.Type()))
							}
						}

						if err := q.fetchRelatedColumn(vTarget.Elem(), currentTbl, depend.rel, depend.relatedColumns); err != nil {
							return err
						}
					}
				}
			}

			return nil
		}
		v := reflect.New(tbl.goType)

		//create scan destination
		dest := make([]interface{}, len(tbl.columns))
		for key, col := range tbl.columns {
			dest[key] = v.Elem().FieldByIndex(col.goIndex).Addr().Interface()
		}

		//create dependent scan destination
		for _, scanObj := range scanObjects {
			vc := reflect.New(scanObj.tbl.goType)

			//find path and assign
			target := v.Elem().FieldByIndex(scanObj.index[0])
			for _, path := range scanObj.index[1:] {
				target = target.Elem().FieldByIndex(path)
			}
			target.Set(vc)

			for _, col := range scanObj.tbl.columns {
				dest = append(dest, vc.Elem().FieldByIndex(col.goIndex).Addr().Interface())
			}
		}

		//scan
		if err = rows.Scan(dest...); err != nil {
			return err
		}

		if sliceTypeIsPtr == true {
			vs.Set(reflect.Append(vs, v))
		} else {
			vs.Set(reflect.Append(vs, v.Elem()))
		}
	}
}

func (q *query) generateSelectSQL(tbl *schema) (string, []interface{}, []related, []scanObject, error) {

	//generate statements
	where, bindVars := q.generateWhere()
	order := q.generateOrder()
	statements, joins, err := q.formatAndResolveStatement(tbl, where, order)
	if err != nil {
		return "", nil, nil, nil, err
	}

	//resolve related and column binder
	columnsSQL, dependsJoins, remainingDepends, scanObjects := q.resolveRelatedAndColumns(tbl)

	//write q
	tblName := "_" + tbl.name
	sql := bytes.NewBufferString(fmt.Sprintf("SELECT %s FROM %s AS %s%s%s%s", columnsSQL, q.ctx.dialect().Quote(tbl.name), tblName, joins, dependsJoins, statements[0]))

	if q.groupby {
		sql.WriteString(fmt.Sprintf(" GROUP BY _%s.%s", tbl.name, q.ctx.dialect().Quote(tbl.aiColumn.name)))
	}
	sql.WriteString(statements[1]) //optional order by

	if q.limit > 0 {
		sql.WriteString(fmt.Sprintf(" LIMIT %d", q.limit))
	}

	if q.offset > 0 {
		sql.WriteString(fmt.Sprintf(" OFFSET %d", q.offset))
	}

	return sql.String(), bindVars, remainingDepends, scanObjects, err
}

func (q *query) generateCountSQL(tbl *schema) (string, []interface{}, error) {
	where, bindVars := q.generateWhere()
	order := q.generateOrder()
	statements, joins, err := q.formatAndResolveStatement(tbl, where, order)
	if nil != err {
		return "", nil, err
	}

	//write the q
	tblName := "_" + tbl.name
	if q.groupby {
		return fmt.Sprintf("SELECT COUNT(DISTINCT %s.%s) FROM %s AS _%s%s%s", tblName, q.ctx.dialect().Quote(tbl.aiColumn.name), q.ctx.dialect().Quote(tbl.name), tblName, joins, statements[0]), bindVars, nil
	}
	return fmt.Sprintf("SELECT COUNT(*) FROM %s AS _%s%s%s", q.ctx.dialect().Quote(tbl.name), tblName, joins, statements[0]), bindVars, nil
}

func (q *query) generateWhere() (string, []interface{}) {
	var (
		sql      bytes.Buffer
		bindVars []interface{}
	)
	if len(q.where) > 0 {
		sql.WriteString(" WHERE ")

		//create where keys
		pos := 0
		for _, cond := range q.where {
			if pos > 0 {
				sql.WriteString(" AND ")
			}
			sql.WriteString(cond.Statement)
			bindVars = append(bindVars, cond.Bindings...)
			pos++
		}
	}

	return sql.String(), bindVars
}

func (q *query) generateOrder() string {
	var sql bytes.Buffer
	if len(q.order) > 0 {
		sql.WriteString(" ORDER BY ")
		pos := 0
		for _, col := range q.order {
			if pos > 0 {
				sql.WriteString(", ")
			}
			sql.WriteString(fmt.Sprintf("%s %s", col.Statement, col.Direction))
			pos++
		}
	}
	return sql.String()
}

// extractStatement extracts the statement
var (
	reExtract       = regexp.MustCompile("'.*'|([0-9A-Za-z\\][_\\-]+\\.)*[0-9A-Za-z_\\-]+")
	reReservedWords = regexp.MustCompile("^(ASC|DESC|ORDER|GROUP|BY|AS|WHERE|IN|NOT|COUNT|NULL|MAX|MIN|AND|OR|RAND|RANDOM|\\-?\\d+(.\\d+)?)$")
)

func (q *query) formatAndResolveStatement(tbl *schema, ins ...string) ([]string, string, error) {
	q.joins = make(map[string]*schema)
	var (
		joinSQL = ""
		out     = make([]string, 0, len(ins))
	)
	for _, in := range ins {
		matches := reExtract.FindAllStringIndex(in, -1)
		offsetCorrection := 0
		for _, match := range matches {
			tmp := in[(match[0] + offsetCorrection):(match[1] + offsetCorrection)]

			//filter out reserved words, strings and numeric values
			if tmp[0] == '\'' || reReservedWords.MatchString(tmp) {
				continue
			}

			parts := strings.Split(tmp, ".")
			colName := caseing.SnakeCase(parts[len(parts)-1])
			targetTbl := tbl
			alias := tbl.name

			//find schema in relations
			findRelationalTable := func(tbl *schema, columnName string) (*schema, *relation, bool) {
				for _, rel := range tbl.relations {

					//manyToMany do not have a column defined we skip it
					if rel.isManyToMany() {
						if strings.EqualFold(rel.relSchema.name, columnName) {
							return rel.relSchema, rel, true
						}
						continue
					}

					if strings.EqualFold(rel.column.name, columnName) {
						tbl, ok := q.ctx.table(indirect(rel.goSingularType))
						return tbl, rel, ok
					}
				}
				return nil, nil, false
			}

			//Find parent relation
			findParentTable := func(tbl *schema, tableName string) (*schema, *relation, bool) {
				//extract hint column (if used)
				tableParts := strings.Split(tableName, "[")
				tableName = tableParts[0]

				if joinTbl, ok := q.ctx.tableByName(tableName); ok {
					colName := ""
					if len(tableParts) >= 2 {
						colName = caseing.SnakeCase(tableParts[1][:len(tableParts[1])-1])
					}

					for _, rel := range joinTbl.relations {
						if indirect(rel.column.goType) == indirect(tbl.goType) &&
							(colName == "" || strings.EqualFold(colName, rel.column.name)) {
							return joinTbl, rel, true
						}
					}
				}
				return nil, nil, false
			}

			//if there are more than 1 parts we need to check
			if len(parts) >= 2 {

				//check if the first schema is not the current schema we are working with
				startOffset := 0
				if strings.EqualFold(caseing.SnakeCase(parts[0]), targetTbl.name) {
					startOffset = 1
				}

				for _, tblToJoin := range parts[startOffset : len(parts)-1] {
					tableJoinStatement := caseing.SnakeCase(tblToJoin)

					//normal join
					joinTbl, rel, ok := findRelationalTable(targetTbl, tableJoinStatement)
					if !ok {
						//no normal join can be resolved we do a search for a parent(reversed) join
						joinTbl, rel, ok = findParentTable(targetTbl, tableJoinStatement)

						if !ok {
							//many to many resolve


							return nil, "", fmt.Errorf("Cannot resolve schema `%s` in statement `%s`", tblToJoin, tmp)
						}
						nextAlias := alias + "_" + joinTbl.name + "_" + rel.column.name

						//only create join when not found
						if _, ok := q.joins[nextAlias]; !ok {
							q.joins[nextAlias] = joinTbl
							joinSQL = joinSQL + " JOIN " + q.ctx.dialect().Quote(joinTbl.name) + " AS _" + nextAlias + " ON _" + alias + ".id = _" + nextAlias + "." + q.ctx.dialect().Quote(rel.relColumn.name)

							//joining a parent schema many to one, need to add a group here
							q.groupby = true
						}
						alias = nextAlias
					} else {
						nextAlias := alias + "_" + rel.column.name
						switch indirect(rel.column.goType).Kind() {
						case reflect.Slice:
							//joining with a slice schema (many to one), need to add a group here
							q.groupby = true

							if _, ok := q.joins[nextAlias]; !ok { //only create join when not found
								q.joins[nextAlias] = joinTbl
								joinSQL = joinSQL + " JOIN " + q.ctx.dialect().Quote(joinTbl.name) + " AS _" + nextAlias + " ON _" + alias + ".id = _" + nextAlias + "." + targetTbl.name + "_id"
							}

						case reflect.Struct:
							//normal one to one
							if _, ok := q.joins[nextAlias]; !ok { //only create join when not found
								q.joins[nextAlias] = joinTbl
								joinSQL = joinSQL + " JOIN " + q.ctx.dialect().Quote(joinTbl.name) + " AS _" + nextAlias + " ON _" + alias + "." + q.ctx.dialect().Quote(rel.relColumn.name) + " = _" + nextAlias + ".id"
							}
						}
						alias = nextAlias
					}
					targetTbl = joinTbl
				}
			}

			//find if column exists in schema definition
			colFound := false
			for _, col := range targetTbl.columns {
				if strings.EqualFold(col.name, colName) {
					replacement := "_" + alias + "." + q.ctx.dialect().Quote(colName)

					in = in[:match[0]+offsetCorrection] + replacement + in[match[1]+offsetCorrection:]
					offsetCorrection = offsetCorrection + (len(replacement) - (match[1] - match[0]))

					colFound = true
					break
				}
			}

			if !colFound {
				return nil, "", fmt.Errorf("Cannot find column `%s` found in schema `%s` used in statement `%s`", colName, targetTbl.name, tmp)
			}
		}
		out = append(out, in)
	}

	return out, joinSQL, nil
}

func (q *query) resolveRelatedAndColumns(tbl *schema) (columnsSQL string, joinSQL string, remainingRelatedColumns []related, scanObjects []scanObject) {

	findRel := func(columnName string, tbl *schema) *relation {
		for _, rel := range tbl.relations {
			if strings.EqualFold(rel.column.name, columnName) {
				return rel
			}
		}
		return nil
	}

	genColumnSql := func(alias string, tbl *schema) string {
		sql := bytes.NewBufferString("")
		pos := 0
		for _, col := range tbl.columns {
			if pos > 0 {
				sql.WriteString(", ")
			}
			sql.WriteString(fmt.Sprintf("_%s.%s", alias, q.ctx.dialect().Quote(col.name)))
			pos++
		}
		return sql.String()
	}

	joinSQL = ""
	columnsSQL = genColumnSql(tbl.name, tbl)
	bindColumns := make(map[string]*schema)
	bindColumns[tbl.name] = tbl

	for _, relatedColumn := range q.relatedColumns {
		var (
			scanPath    = [][]int{}
			targetTbl   = tbl
			parts       = strings.Split(relatedColumn, ".")
			startOffset = 0
		)

		if strings.EqualFold(caseing.SnakeCase(parts[0]), targetTbl.name) {
			startOffset = 1
		}

		alias := tbl.name

		addRemaningDepend := func(scanPath [][]int, relatedColumn string, rel *relation) {
			for i, _ := range remainingRelatedColumns {
				if reflect.DeepEqual(remainingRelatedColumns[i].index, scanPath) {
					if len(relatedColumn) > 0 {
						remainingRelatedColumns[i].relatedColumns = append(remainingRelatedColumns[i].relatedColumns, relatedColumn)
					}

					return
				}
			}

			if len(relatedColumn) > 0 {
				remainingRelatedColumns = append(remainingRelatedColumns, related{index: scanPath, relatedColumns: []string{relatedColumn}, rel: rel})
			} else {
				remainingRelatedColumns = append(remainingRelatedColumns, related{index: scanPath, relatedColumns: []string{}, rel: rel})
			}
		}

		for i, _ := range parts[startOffset:len(parts)] {

			relColumnName := caseing.SnakeCase(parts[i])
			rel := findRel(relColumnName, targetTbl)
			//if no relation is found we stop searching further
			if rel == nil {
				break
			}

			//append scan path
			scanPath = append(scanPath, rel.column.goIndex)

			//we only allow 1 on 1, no slices etc
			if !rel.isOneToOne() {
				//add to separate depend call
				addRemaningDepend(scanPath, strings.Join(parts[i+1:], "."), rel)
				break
			}

			nextAlias := alias + "_" + rel.column.name
			//find registered schemes
			joinTbl, ok := q.ctx.table(indirect(rel.goSingularType))
			if !ok {
				break
			}
			//create join if not already one
			if _, ok := q.joins[nextAlias]; !ok {
				//we assume scanner valuer are optional and ptr types of ints, we do not include them in this q
				if rel.relColumn.isScanner == true || rel.relColumn.goType.Kind() == reflect.Ptr {
					//optional joins are fetched in a separate related call
					addRemaningDepend(scanPath, strings.Join(parts[i+1:], "."), rel)
					break
				}

				joinSQL = joinSQL + " JOIN " + q.ctx.dialect().Quote(joinTbl.name) + " AS _" + nextAlias + " ON _" + alias + "." + q.ctx.dialect().Quote(rel.relColumn.name) + " = _" + nextAlias + ".id"
				q.joins[nextAlias] = joinTbl
			}

			//generate the bind columns only once
			if _, ok := bindColumns[nextAlias]; !ok {
				scanObjects = append(scanObjects, scanObject{index: scanPath, tbl: joinTbl})
				bindColumns[nextAlias] = joinTbl
				columnsSQL = columnsSQL + ", " + genColumnSql(nextAlias, joinTbl)
			}
			alias = nextAlias
			targetTbl = joinTbl
		}
	}

	return columnsSQL, joinSQL, remainingRelatedColumns, scanObjects
}
