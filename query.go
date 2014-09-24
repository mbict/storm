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

	var bindVars []interface{}
	var sql bytes.Buffer
	var pos int

	sql.WriteString("SELECT ")

	//create columns
	pos = 0
	for _, col := range tbl.columns {
		if pos > 0 {
			sql.WriteString(", ")
		}
		sql.WriteString(fmt.Sprintf("%s.%s", query.ctx.Dialect().Quote(tbl.tableName), query.ctx.Dialect().Quote(col.columnName)))
		pos++
	}

	//add table name
	sql.WriteString(fmt.Sprintf(" FROM %s", query.ctx.Dialect().Quote(tbl.tableName)))

	//add where
	var sqlStatements bytes.Buffer
	if len(query.where) > 0 {
		sqlStatements.WriteString(" WHERE ")

		//create where keys
		pos = 0
		for _, cond := range query.where {
			if pos > 0 {
				sqlStatements.WriteString(" AND ")
			}
			sqlStatements.WriteString(cond.Statement)
			bindVars = append(bindVars, cond.Bindings...)
			pos++
		}
	}

	//add order
	if len(query.order) > 0 {
		sqlStatements.WriteString(" ORDER BY ")
		pos = 0
		for _, col := range query.order {
			if pos > 0 {
				sqlStatements.WriteString(", ")
			}
			sqlStatements.WriteString(fmt.Sprintf("%s %s", col.Statement, col.Direction))
			pos++
		}
	}
	
	statementsString, tbls := query.formatAndResolveStatement(sqlStatements.String(), tbl)
	
	//join the related tables if posible
	for _,relatedTbl := range tbls {
		sql.WriteString(fmt.Sprintf(" JOIN %s ON %s.%s = %s.%s", 
			query.ctx.Dialect().Quote(relatedTbl.tableName),
			query.ctx.Dialect().Quote(relatedTbl.tableName),
			"??",
			query.ctx.Dialect().Quote(tbl.tableName),
			query.ctx.Dialect().Quote(tbl.aiColumn.columnName) ) )
	}
	sql.WriteString(statementsString)

	//add limit
	if query.limit > 0 {
		sql.WriteString(fmt.Sprintf(" LIMIT %d", query.limit))
	}

	//add offset
	if query.offset > 0 {
		sql.WriteString(fmt.Sprintf(" OFFSET %d", query.offset))
	}
	return sql.String(), bindVars
}

func (query *Query) generateCountSQL(tbl *table) (string, []interface{}) {

	var bindVars []interface{}
	var sql bytes.Buffer
	var pos int

	//add table name
	sql.WriteString(fmt.Sprintf("SELECT COUNT(%s.*) FROM %s", query.ctx.Dialect().Quote(tbl.tableName), query.ctx.Dialect().Quote(tbl.tableName)))

	//add where
	var sqlStatements bytes.Buffer
	if len(query.where) > 0 {
		sqlStatements.WriteString(" WHERE ")

		//create where keys
		pos = 0
		for _, cond := range query.where {
			if pos > 0 {
				sqlStatements.WriteString(" AND ")
			}
			sqlStatements.WriteString(cond.Statement)
			bindVars = append(bindVars, cond.Bindings...)
			pos++
		}
	}
	statementsString, tbls := query.formatAndResolveStatement(sqlStatements.String(), tbl)
	
	//join the related tables if posible
	for _,relatedTbl := range tbls {
		sql.WriteString(fmt.Sprintf(" JOIN %s ON %s.%s = %s.%s", 
			query.ctx.Dialect().Quote(relatedTbl.tableName),
			query.ctx.Dialect().Quote(relatedTbl.tableName),
			"??",
			query.ctx.Dialect().Quote(tbl.tableName),
			query.ctx.Dialect().Quote(tbl.aiColumn.columnName) ) )
	}
	sql.WriteString(statementsString)
	

	return sql.String(), bindVars
}

// extractStatment extracts the statement
var (
	reExtract       = regexp.MustCompile("'?[0-9A-Za-z_-]+\\.[0-9A-Za-z_-]+|'?[0-9A-Za-z_-]+")
	reReservedWords = regexp.MustCompile("^IN|NOT|COUNT|NULL|MAX|MIN|AND|OR|\\d+$")
)

func (query *Query) formatAndResolveStatement(in string, tbl *table) (string, []*table) {

	var (
		relatedTbls      = make(map[string]*table)
		results          = reExtract.FindAllStringIndex(in, -1)
		offsetCorrection = 0
	)
	for _, match := range results {
		tmp := in[(match[0] + offsetCorrection):(match[1] + offsetCorrection)]
		if tmp[0] == '\'' || reReservedWords.MatchString(tmp) {
			continue
		}

		targetTbl := tbl
		colName := ""
		parts := strings.Split(tmp, ".")

		if len(parts) == 2 {
			ok := true
			targetTbl, ok = query.ctx.tableByName(camelToSnake(parts[0]))
			if !ok {
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
				continue
			}
		}
	}

	result := make([]*table, len(relatedTbls))
	i := 0
	for _, tbl := range relatedTbls {
		result[i] = tbl
		i++
	}

	return in, result
}
