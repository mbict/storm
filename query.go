package storm

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"
)

//SortDirection indicates the sort direction used in Order
type (
	SortDirection string

	where struct {
		Statement string
		Table     string
		Bindings  []interface{}
	}

	order struct {
		Statement string
		Direction SortDirection
	}
)

//ASC ascending order
//DESC descending order
const (
	ASC  SortDirection = "ASC"
	DESC SortDirection = "DESC"
)

//Query structure
type Query struct {
	ctx    Context
	where  []where
	order  []order
	offset int
	limit  int

	dependentFetch   bool
	dependentColumns []string

	joins  map[string]*table
	groups map[string]string
}

func newQuery(ctx Context, parent *Query) *Query {

	q := Query{
		ctx:    ctx,
		offset: -1,
		limit:  -1,
	}

	if parent != nil {
		//clone parent
		q.where = parent.where
		q.order = parent.order
		q.offset = parent.offset
		q.limit = parent.limit
	} else {
		q.where = make([]where, 0)
		q.order = make([]order, 0)
	}

	return &q

}

//Query Creates a clone of the current query object
func (query *Query) Query() *Query {
	return newQuery(query.ctx, query)
}

//Order will set the order
//Example:
// q.Order("columnnname", storm.ASC)
// q.Order("columnnname", storm.DESC)
func (query *Query) Order(column string, direction SortDirection) *Query {
	query.order = append(query.order, order{column, direction})
	return query
}

//Where adds new where conditions to the query
//Example:
// q.Where("column = 1") //textual condition
// q.Where("column = ?", 1) //bind params
// q.Where("(column = ? OR other = ?)",1,2) //multiple bind params
func (query *Query) Where(condition string, bindAttr ...interface{}) *Query {

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
			if tbl, ok := query.ctx.table(v.Type()); ok {
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

	query.where = append(query.where, where{Statement: condition, Bindings: bindVars})
	return query
}

//Limit sets the limit for select
func (query *Query) Limit(limit int) *Query {
	query.limit = limit
	return query
}

//Offset sets the offset for select
func (query *Query) Offset(offset int) *Query {
	query.offset = offset
	return query
}

//DependentColumns will set the dependent fetch mode for Find and First.
//When set all or only the provided columns who are dependent will be populated when fetched
func (query *Query) DependentColumns(columns ...string) *Query {
	query.dependentFetch = true
	query.dependentColumns = append(query.dependentColumns, columns...)
	return query
}

//Find will try to retreive the matching structure/entity based on your where statement
//you can provide a slice or a single element
func (query *Query) Find(i interface{}, where ...interface{}) error {

	//slice given
	if reflect.Indirect(reflect.ValueOf(i)).Kind() == reflect.Slice {
		if len(where) >= 1 {
			return query.Query().fetchAll(i, where...)
		}
		return query.fetchAll(i)
	}

	//single context
	if len(where) >= 1 {
		return query.Query().fetchRow(i, where...)
	}
	return query.fetchRow(i)
}

//First will execute the query and return one result to i
//Example:
// var result *TestModel
// q.First(&result)
func (query *Query) First(i interface{}) error {
	return query.fetchRow(i)
}

//Count will execute a query and return the resulting rows Select will return
//Example:
// count, err := q.Count((*TestModel)(nil))
func (query *Query) Count(i interface{}) (int64, error) {
	return query.fetchCount(i)
}

//Dependent will try to fetch all the related enities and populate the dependent fields (slice and single values)
//You can provide a list with column names if you only want those fields to be populated
func (query *Query) Dependent(i interface{}, columns ...string) error {

	v := reflect.ValueOf(i)
	if v.Kind() != reflect.Ptr {
		return errors.New("provided input is not a pointer type")
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

	//find the table
	tbl, ok := query.ctx.table(v.Type())
	if !ok {
		return fmt.Errorf("no registered structure for `%s` found", v.Type().String())
	}

	for _, col := range columns {
		col = camelToSnake(col)
		for _, rel := range tbl.relations {
			if strings.EqualFold(rel.name, col) {
				elm := v.FieldByIndex(rel.goIndex)
				dst := elm.Addr().Interface()
				if rel.relColumn != nil && rel.relTable == nil {

					val := v.FieldByIndex(rel.relColumn.goIndex).Interface()

					//check if val is not empty or 0 to avoid lookups who will result in no rows
					if valuer, ok := val.(driver.Valuer); ok {
						val, _ = valuer.Value()
						if val == nil {
							//empty valuer
							if elm.Kind() == reflect.Ptr && elm.IsNil() == false {
								elm.Set(reflect.Zero(elm.Type()))
							}
							break
						}
					}

					if val, ok := val.(int64); ok {
						if val == 0 {
							//empty int
							if elm.Kind() == reflect.Ptr && elm.IsNil() == false {
								elm.Set(reflect.Zero(elm.Type()))
							}
							break
						}
					}

					err := query.ctx.Find(dst, "id = ?", val)

					//if there are no results we reset the column if its a pointer to nil
					if err == sql.ErrNoRows {
						if elm.Kind() == reflect.Ptr && elm.IsNil() == false {
							elm.Set(reflect.Zero(elm.Type()))
						}
					} else if err != nil {
						return err
					}
				} else if rel.relColumn != nil && rel.relTable != nil {
					val := v.FieldByIndex(tbl.aiColumn.goIndex).Interface()
					err := query.ctx.Find(dst, tbl.tableName+"_id = ?", val)
					if err != nil && err != sql.ErrNoRows {
						return err
					}
				}
				break
			}
		}
	}
	return nil
}

//create additional where stements from arguments
func (query *Query) applyWhere(tbl *table, where ...interface{}) error {
	switch t := where[0].(type) {
	case string:
		query.Where(t, where[1:]...)
	case int, int8, int16, int32, uint, uint8, uint16, uint32, int64, uint64, sql.NullInt64:
		if len(tbl.keys) == 1 {
			if len(where) == 1 {
				query.Where(fmt.Sprintf("%s = ?", tbl.keys[0].columnName), where...)
			} else {
				return errors.New("not implemented having multiple pk values for find")
			}
		} else {
			return errors.New("not implemented having multiple pks for find")
		}
	default:
		v := reflect.Indirect(reflect.ValueOf(t))
		if v.Kind() == reflect.Struct {
			if tbl, ok := query.ctx.table(v.Type()); ok {
				condition := fmt.Sprintf("%s = ?", tbl.tableName+"_id")
				if nil != tbl.aiColumn {
					query.Where(condition, v.FieldByIndex(tbl.aiColumn.goIndex).Int())
					return nil
				} else if len(tbl.keys) >= 1 {
					query.Where(condition, v.FieldByIndex(tbl.keys[0].goIndex).Int())
					return nil
				}
			}
		}
		return errors.New("unsupported pk find type")
	}

	return nil
}

//fetch a single row into a element
func (query *Query) fetchCount(i interface{}) (cnt int64, err error) {
	t := reflect.TypeOf(i)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct {
		return 0, errors.New("provided input is not a structure type")
	}

	//find the table
	tbl, ok := query.ctx.table(t)
	if !ok {
		return 0, fmt.Errorf("no registered structure for `%s` found", t)
	}

	//generate sql and prepare
	sqlQuery, bind := query.generateCountSQL(tbl)

	if query.ctx.logger() != nil {
		query.ctx.logger().Printf("`%s` binding : %v", sqlQuery, bind)
	}

	stmt, err := query.ctx.DB().Prepare(sqlQuery)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	//query the row
	row := stmt.QueryRow(bind...)

	//create destination and scan
	err = row.Scan(&cnt)
	return cnt, err
}

//fetch a single row into a element
func (query *Query) fetchRow(i interface{}, where ...interface{}) (err error) {
	v := reflect.ValueOf(i)
	if v.Kind() != reflect.Ptr {
		return errors.New("provided input is not a pointer type")
	}

	//reset element to zero variant
	v = v.Elem()
	if v.Kind() == reflect.Ptr {
		v.Set(reflect.New(v.Type().Elem()))
	}
	v = reflect.Indirect(v)

	if v.Kind() != reflect.Struct || !v.CanSet() {
		return errors.New("provided input is not a structure type")
	}

	//find the table
	tbl, ok := query.ctx.table(v.Type())
	if !ok {
		return fmt.Errorf("no registered structure for `%s` found", v.Type().String())
	}

	//add the last minute where
	if len(where) >= 1 {
		if err = query.applyWhere(tbl, where...); err != nil {
			return err
		}
	}

	//generate sql and prepare
	sqlQuery, bind := query.generateSelectSQL(tbl)
	if query.ctx.logger() != nil {
		query.ctx.logger().Printf("`%s` binding : %v", sqlQuery, bind)
	}

	stmt, err := query.ctx.DB().Prepare(sqlQuery)
	if err != nil {
		return err
	}
	defer stmt.Close()

	//query the row
	row := stmt.QueryRow(bind...)

	//create destination and scan
	dest := make([]interface{}, len(tbl.columns))
	for key, col := range tbl.columns {
		dest[key] = v.FieldByIndex(col.goIndex).Addr().Interface()
	}

	err = row.Scan(dest...)
	if err != nil {
		return err
	}

	if query.dependentFetch == true {
		query.Dependent(v.Addr().Interface(), query.dependentColumns...)
	}

	return tbl.callbacks.invoke(v.Addr(), "OnInit", query.ctx)
}

//fetch a all rows into a slice
func (query *Query) fetchAll(i interface{}, where ...interface{}) (err error) {

	ts := reflect.TypeOf(i)
	if ts.Kind() != reflect.Ptr {
		return errors.New("provided input is not a pointer type")
	}

	if ts.Elem().Kind() != reflect.Slice {
		return errors.New("provided input pointer is not a slice type")
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

	//find the table
	tbl, ok := query.ctx.table(t)
	if !ok {
		return fmt.Errorf("no registered structure for `%s` found", t.String())
	}

	//add the last minute where
	if len(where) >= 1 {
		if err = query.applyWhere(tbl, where...); err != nil {
			return err
		}
	}

	//generate sql and prepare
	sqlQuery, bind := query.generateSelectSQL(tbl)
	if query.ctx.logger() != nil {
		query.ctx.logger().Printf("`%s` binding : %v", sqlQuery, bind)
	}

	stmt, err := query.ctx.DB().Prepare(sqlQuery)
	if err != nil {
		return err
	}
	defer stmt.Close()

	//query for the results
	rows, err := stmt.Query(bind...)
	if err != nil {
		return err
	}
	defer rows.Close()

	vs := reflect.ValueOf(i).Elem()
	vs.SetLen(0)

	for {
		if !rows.Next() {
			// if error occured return rawselect
			if rows.Err() != nil {
				return rows.Err()
			} else if vs.Len() == 0 {
				return sql.ErrNoRows
			}
			return nil
		}

		v := reflect.New(tbl.goType)

		//create destination and scan
		dest := make([]interface{}, len(tbl.columns))
		for key, col := range tbl.columns {
			dest[key] = v.Elem().FieldByIndex(col.goIndex).Addr().Interface()
		}

		if err = rows.Scan(dest...); err != nil {
			return err
		}

		if err = tbl.callbacks.invoke(v, "OnInit", query.ctx); err != nil {
			return err
		}

		if query.dependentFetch == true {
			err = query.Dependent(v.Interface(), query.dependentColumns...)
			if err != nil {
				return err
			}
		}

		if sliceTypeIsPtr == true {
			vs.Set(reflect.Append(vs, v))
		} else {
			vs.Set(reflect.Append(vs, v.Elem()))
		}
	}
}

func (query *Query) generateSelectSQL(tbl *table) (string, []interface{}) {
	where, bindVars := query.generateWhere()
	order := query.generateOrder()
	statements, tbls := query.formatAndResolveStatement(tbl, where, order)
	joins, addGroupBy := query.generateJoins(tbls, tbl)

	//write query
	sql := bytes.NewBufferString("SELECT ")
	pos := 0
	for _, col := range tbl.columns {
		if pos > 0 {
			sql.WriteString(", ")
		}
		sql.WriteString(fmt.Sprintf("%s.%s", query.ctx.Dialect().Quote(tbl.tableName), query.ctx.Dialect().Quote(col.columnName)))
		pos++
	}
	sql.WriteString(fmt.Sprintf(" FROM %s%s%s", query.ctx.Dialect().Quote(tbl.tableName), joins, statements[0]))
	if addGroupBy == true {
		sql.WriteString(fmt.Sprintf(" GROUP BY %s.%s", query.ctx.Dialect().Quote(tbl.tableName), query.ctx.Dialect().Quote(tbl.aiColumn.columnName)))
	}
	sql.WriteString(statements[1]) //optional order by

	if query.limit > 0 {
		sql.WriteString(fmt.Sprintf(" LIMIT %d", query.limit))
	}

	if query.offset > 0 {
		sql.WriteString(fmt.Sprintf(" OFFSET %d", query.offset))
	}
	return sql.String(), bindVars
}

func (query *Query) generateCountSQL(tbl *table) (string, []interface{}) {
	where, bindVars := query.generateWhere()
	statement, tbls := query.formatAndResolveStatement(tbl, where)
	joins, addGroupBy := query.generateJoins(tbls, tbl)

	//write the query
	sql := bytes.NewBufferString(fmt.Sprintf("SELECT COUNT(*) FROM %s%s%s", query.ctx.Dialect().Quote(tbl.tableName), joins, statement[0]))

	if addGroupBy == true {
		sql.WriteString(fmt.Sprintf(" GROUP BY %s.%s", query.ctx.Dialect().Quote(tbl.tableName), query.ctx.Dialect().Quote(tbl.aiColumn.columnName)))
	}

	return sql.String(), bindVars
}

func (query *Query) generateWhere() (string, []interface{}) {
	var (
		sql      bytes.Buffer
		bindVars []interface{}
	)
	if len(query.where) > 0 {
		sql.WriteString(" WHERE ")

		//create where keys
		pos := 0
		for _, cond := range query.where {
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

func (query *Query) generateOrder() string {
	var sql bytes.Buffer
	if len(query.order) > 0 {
		sql.WriteString(" ORDER BY ")
		pos := 0
		for _, col := range query.order {
			if pos > 0 {
				sql.WriteString(", ")
			}
			sql.WriteString(fmt.Sprintf("%s %s", col.Statement, col.Direction))
			pos++
		}
	}
	return sql.String()
}

func (query *Query) generateJoins(tbls []*table, tbl *table) (string, bool) {
	var (
		sql        bytes.Buffer
		addGroupBy = false
	)

	for _, relatedTbl := range tbls {
		for _, relTblDef := range relatedTbl.relations {
			if relTblDef.relTable == tbl {
				sql.WriteString(fmt.Sprintf(" INNER JOIN %s ON %s.%s = %s.%s",
					query.ctx.Dialect().Quote(relatedTbl.tableName),
					query.ctx.Dialect().Quote(relatedTbl.tableName),
					query.ctx.Dialect().Quote(relatedTbl.aiColumn.columnName),
					query.ctx.Dialect().Quote(tbl.tableName),
					query.ctx.Dialect().Quote(relatedTbl.tableName+"_id")))

				break
			}
		}

		for _, relTblDef := range tbl.relations {
			if relTblDef.relTable == relatedTbl {
				sql.WriteString(fmt.Sprintf(" INNER JOIN %s ON %s.%s = %s.%s",
					query.ctx.Dialect().Quote(relatedTbl.tableName),
					query.ctx.Dialect().Quote(relatedTbl.tableName),
					query.ctx.Dialect().Quote(tbl.tableName+"_id"),
					query.ctx.Dialect().Quote(tbl.tableName),
					query.ctx.Dialect().Quote(tbl.aiColumn.columnName)))
				addGroupBy = true
				break
			}
		}
	}
	return sql.String(), addGroupBy
}

// extractStatment extracts the statement
var (
	reExtract       = regexp.MustCompile("([0-9A-Za-z\\][_-]+\\.)*[0-9A-Za-z_-]+")
	reReservedWords = regexp.MustCompile("^(WHERE|IN|NOT|COUNT|NULL|MAX|MIN|AND|OR|\\d+)$")
)

func (query *Query) formatAndResolveStatement(tbl *table, ins ...string) ([]string, []*table) {

	var (
		relatedTbls = make(map[string]*table)
		out         = make([]string, 0, len(ins))
	)
	for _, in := range ins {
		matches := reExtract.FindAllStringIndex(in, -1)
		offsetCorrection := 0
		for _, match := range matches {
			tmp := in[(match[0] + offsetCorrection):(match[1] + offsetCorrection)]
			if tmp[0] == '\'' || reReservedWords.MatchString(tmp) {
				continue
			}
			targetTbl := tbl
			colName := ""
			parts := strings.Split(tmp, ".")

			if len(parts) == 2 {
				ok := true

				//find table in relations
				findRelationalTable := func(tbl *table, columnName string) (*table, bool) {
					for _, rel := range tbl.relations {
						if strings.EqualFold(rel.name, columnName) {
							//find table
							return query.ctx.table(rel.goSingularType.Elem())
						}
					}
					return nil, false
				}

				targetTbl, ok = findRelationalTable(tbl, camelToSnake(parts[0]))
				if !ok || targetTbl == nil {
					continue
				}
				colName = camelToSnake(parts[1])
			} else {
				//use current table
				colName = camelToSnake(parts[0])
			}

			//find if column exists in table definition
			for _, col := range targetTbl.columns {
				if strings.EqualFold(col.columnName, colName) {
					if targetTbl != tbl {
						if _, ok := relatedTbls[targetTbl.tableName]; !ok {
							relatedTbls[targetTbl.tableName] = targetTbl
						}
					}

					replacement := query.ctx.Dialect().Quote(targetTbl.tableName) + "." + query.ctx.Dialect().Quote(col.columnName)
					in = in[:match[0]+offsetCorrection] + replacement + in[match[1]+offsetCorrection:]
					offsetCorrection = offsetCorrection + (len(replacement) - (match[1] - match[0]))
					break
				}
			}
		}
		out = append(out, in)
	}

	result := make([]*table, 0, len(relatedTbls))
	for _, tbl := range relatedTbls {
		result = append(result, tbl)
	}

	return out, result
}

func (query *Query) formatAndResolveStatement2(tbl *table, ins ...string) ([]string, string, map[string]*table, error) {

	query.joins = make(map[string]*table)
	query.groups = make(map[string]string)

	var (
		joins   = make(map[string]*table)
		joinSQL = ""
		out     = make([]string, 0, len(ins))
	)
	for _, in := range ins {
		matches := reExtract.FindAllStringIndex(in, -1)
		offsetCorrection := 0
		for _, match := range matches {
			tmp := in[(match[0] + offsetCorrection):(match[1] + offsetCorrection)]

			//filter out reserved words
			if tmp[0] == '\'' || reReservedWords.MatchString(tmp) {
				continue
			}

			parts := strings.Split(tmp, ".")
			colName := camelToSnake(parts[len(parts)-1])
			targetTbl := tbl
			alias := tbl.tableName

			//find table in relations
			findRelationalTable := func(tbl *table, columnName string) (*table, *relation, bool) {
				for _, rel := range tbl.relations {
					if strings.EqualFold(rel.name, columnName) {
						tbl, ok := query.ctx.table(rel.goSingularType.Elem())
						return tbl, rel, ok
					}
				}
				return nil, nil, false
			}

			//helper to make indirect
			typeIndirect := func(t reflect.Type) reflect.Type {
				if t.Kind() == reflect.Ptr {
					return t.Elem()
				}
				return t
			}

			//Find parent relation
			findParentTable := func(tbl *table, tableName string) (*table, *relation, bool) {
				//extract hint column (if used)
				tableParts := strings.Split(tableName, "[")
				tableName = tableParts[0]
	
				if joinTbl, ok := query.ctx.tableByName(tableName); ok {
					colName := ""
					if len(tableParts) >= 2 {
						colName = camelToSnake(tableParts[1][:len(tableParts[1])-1])
					}
		
					for _, rel := range joinTbl.relations {
						if typeIndirect(rel.goType) == typeIndirect(tbl.goType) &&
							(colName == "" || strings.EqualFold(colName, rel.name)) {
							return joinTbl, rel, true
						}
					}
				}
				return nil, nil, false
			}

			//if there are more than 1 parts we need to check
			if len(parts) >= 2 {

				//check if the first table is not the current table we are working with
				startOffset := 0
				if strings.EqualFold(camelToSnake(parts[0]), targetTbl.tableName) {
					startOffset = 1
				}

				for _, tblToJoin := range parts[startOffset : len(parts)-1] {
					tableJoinStatement := camelToSnake(tblToJoin)

					//normal join
					joinTbl, rel, ok := findRelationalTable(targetTbl, tableJoinStatement)
					if !ok {
						//no normal join can be resolved we do a search for a parent(reversed) join
						joinTbl, rel, ok = findParentTable(targetTbl, tableJoinStatement)
						if !ok {
							return nil, "", nil, fmt.Errorf("Cannot resolve table `%s` in statement `%s`", tblToJoin, tmp)
						}
						nextAlias := alias + "_" + joinTbl.tableName + "_" + rel.name

						//only create join when not found
						if _, ok := query.joins[nextAlias]; !ok {
							query.joins[nextAlias] = joinTbl
							joinSQL = joinSQL + " JOIN " + joinTbl.tableName + " AS " + nextAlias + " ON " + alias + ".id = " + nextAlias + "." + rel.name + "_id"

							//joining a parent table many to one, need to add a group here
							query.groups[tbl.tableName+"."+tbl.aiColumn.columnName] = tbl.tableName + "." + tbl.aiColumn.columnName
						}
						alias = nextAlias
					} else {

						nextAlias := alias + "_" + rel.name
						//only create join when not found
						if _, ok := query.joins[nextAlias]; !ok {
							query.joins[nextAlias] = joinTbl
							joinSQL = joinSQL + " JOIN " + joinTbl.tableName + " AS " + nextAlias + " ON " + alias + "." + rel.name + "_id = " + nextAlias + ".id"
						}
						alias = nextAlias
					}
					targetTbl = joinTbl
				}
			}

			//find if column exists in table definition
			colFound := false
			for _, col := range targetTbl.columns {
				if strings.EqualFold(col.columnName, colName) {
					replacement := query.ctx.Dialect().Quote(alias) + "." + query.ctx.Dialect().Quote(colName)

					in = in[:match[0]+offsetCorrection] + replacement + in[match[1]+offsetCorrection:]
					offsetCorrection = offsetCorrection + (len(replacement) - (match[1] - match[0]))

					colFound = true
					break
				}
			}

			if !colFound {
				return nil, "", nil, fmt.Errorf("Cannot find column `%s` found in table `%s` used in statement `%s`", colName, targetTbl.tableName, tmp)
			}
		}

		out = append(out, in)
	}

	return out, joinSQL, joins, nil
}

func (query *Query) generateJoin(name string, join *table, parent *table) string {
	for _, joinRels := range join.relations {
		if joinRels.relTable == parent {
			return fmt.Sprintf(" INNER JOIN %s ON %s.%s = %s.%s",
				query.ctx.Dialect().Quote(join.tableName),
				query.ctx.Dialect().Quote(join.tableName),
				query.ctx.Dialect().Quote(join.aiColumn.columnName),
				query.ctx.Dialect().Quote(parent.tableName),
				query.ctx.Dialect().Quote(join.tableName+"_id"))
		}
	}

	for _, parentRels := range parent.relations {
		if parentRels.relTable == join {
			return fmt.Sprintf(" INNER JOIN %s ON %s.%s = %s.%s",
				query.ctx.Dialect().Quote(join.tableName),
				query.ctx.Dialect().Quote(join.tableName),
				query.ctx.Dialect().Quote(parent.tableName+"_id"),
				query.ctx.Dialect().Quote(parent.tableName),
				query.ctx.Dialect().Quote(parent.aiColumn.columnName))
		}
	}

	return ""

}

func (query *Query) generateSelectSQL2(tbl *table) (string, []interface{}, error) {

	//generate statements
	where, bindVars := query.generateWhere()
	order := query.generateOrder()

	statements, joins, _, err := query.formatAndResolveStatement2(tbl, where, order)

	if err != nil {
		return "", nil, err
	}

	//write query
	sql := bytes.NewBufferString("SELECT ")
	pos := 0
	for _, col := range tbl.columns {
		if pos > 0 {
			sql.WriteString(", ")
		}
		sql.WriteString(fmt.Sprintf("%s.%s", query.ctx.Dialect().Quote(tbl.tableName), query.ctx.Dialect().Quote(col.columnName)))
		pos++
	}
	sql.WriteString(fmt.Sprintf(" FROM %s%s%s", query.ctx.Dialect().Quote(tbl.tableName), joins, statements[0]))
	
	if len(query.groups) >= 1 {
		sql.WriteString(fmt.Sprintf(" GROUP BY %s.%s", query.ctx.Dialect().Quote(tbl.tableName), query.ctx.Dialect().Quote(tbl.aiColumn.columnName)))
	}
	sql.WriteString(statements[1]) //optional order by

	if query.limit > 0 {
		sql.WriteString(fmt.Sprintf(" LIMIT %d", query.limit))
	}

	if query.offset > 0 {
		sql.WriteString(fmt.Sprintf(" OFFSET %d", query.offset))
	}

	return sql.String(), bindVars, err
}
