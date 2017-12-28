package storm

import (
	"github.com/mbict/storm/ql"
	"strings"
	"bytes"
)

type (
	QueryBuilder interface {
		ApplyWhere(string, ...interface{}) error
		ApplyOrder(string, SortDirection) error
		SQL() (string, []interface{}, error)
	}

	queryBuilder struct {
		storm  Storm
		target *schema
		joins  map[string]*schema
	}
)

func (b *queryBuilder) ApplyOrder(string, SortDirection) error {
	panic("implement me")
}

func (b *queryBuilder) ApplyWhere(string, ...interface{}) error {
	panic("implement me")
}

func (b *queryBuilder) SQL() (string, []interface{}, error) {
	panic("implement me")
}

func buildSelectQuery(base *schema, query *query) {
	//for _, q := range query.where {
	//
	//}
}

func findXY( base *schema, tbls []string ) {

}


func buildWhere(naming NamingStrategy, currentSchema *schema, in string, bindings []interface{}, schemes schemes) (string, error) {
	s := ql.NewScanner(in)
	sql := bytes.NewBufferString("")

	for tok, v := s.Scan(); tok != ql.EOF; tok, v = s.Scan() {
		sql.WriteByte(' ')
		switch tok {
		case ql.IDENT:
			tbls, col := splitTablesColumn(v)
			var parentSchema *schema



			/*for _, tbl := range tbls {
				schema, err := schemes.findByName(tbl)



				if err != nil {
					return "", err
				}
				parentSchema = schema
			}



			//set parent schema if none defined in query
			if parentSchema == nil {
				parentSchema = currentSchema
			}*/





			sql.WriteString(naming.formatTableAlias(parentSchema.name))
			sql.WriteByte('.')
			sql.WriteString(naming.formatColumnName(col))
		default:
			sql.WriteString(v)
		}
	}

	return sql.String()[1:], nil
}

func splitTablesColumn(in string) ([]string, string) {
	elems := strings.Split(in, ".")
	if l := len(elems); l >= 1 {
		return elems[:l-1], elems[l-1]
	}
	return []string{}, in
}


