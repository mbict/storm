package storm

import "database/sql"

type Stmt struct {
	*sql.Stmt
}