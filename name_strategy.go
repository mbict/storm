package storm

import (
	"github.com/mbict/go-dry/strings/caseing"
	"sort"
	"strings"
)

type NameStrategy func(string) string
type PivotTableNameStrategy func(...string) string

func AliasNameStrategy(in string) string {
	return "_" + in
}

func SnakeCaseNameStrategy(in string) string {
	return caseing.SnakeCase(in)
}

func SnakeCasePivotTableNameStrategy(prefix string) PivotTableNameStrategy {
	return func(names ...string) string {
		for i, name := range names {
			names[i] = caseing.SnakeCase(name)
		}
		sort.Strings(names)
		return prefix + "_" + strings.Join(names, "_")
	}
}

type NamingStrategy interface {
	formatTableName(string) string
	formatColumnName(string) string
	formatTableAlias(string) string
	formatPivotTable(...string) string
}

type namingStrategy struct {
	aliasStrategy          NameStrategy
	tableNameStrategy      NameStrategy
	columnNameStrategy     NameStrategy
	pivotTableNameStrategy PivotTableNameStrategy
}

func (f *namingStrategy) formatTableName(tableName string) string {
	return f.tableNameStrategy(tableName)
}

func (f *namingStrategy) formatColumnName(columnName string) string {
	return f.columnNameStrategy(columnName)
}

func (f *namingStrategy) formatTableAlias(tableName string) string {
	return f.aliasStrategy(f.tableNameStrategy(tableName))
}

func (f *namingStrategy) formatPivotTable(tableNames ...string) string {
	return f.pivotTableNameStrategy(tableNames...)
}

func NewNamingStrategy(aliasStrategy NameStrategy, tableNameStrategy NameStrategy, columnNameStrategy NameStrategy, pivotNameStrategy PivotTableNameStrategy) NamingStrategy {
	return &namingStrategy{
		aliasStrategy:          aliasStrategy,
		tableNameStrategy:      tableNameStrategy,
		columnNameStrategy:     columnNameStrategy,
		pivotTableNameStrategy: pivotNameStrategy,
	}
}

var DefaultNamingStrategy NamingStrategy = NewNamingStrategy(AliasNameStrategy, SnakeCaseNameStrategy, SnakeCaseNameStrategy, SnakeCasePivotTableNameStrategy("rel"))
