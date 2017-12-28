package storm

type (
	OnInsertCallback interface {
		OnInsert(context dbContext, i interface{}) error
	}

	OnPostInsertCallback interface {
		OnPostInsert(context dbContext, i interface{}) error
	}

	OnUpdateCallback interface {
		OnUpdate(context dbContext, i interface{}) error
	}

	OnPostUpdateCallback interface {
		OnPostUpdate(context dbContext, i interface{}) error
	}

	OnDeleteCallback interface {
		OnDelete(context dbContext, i interface{}) error
	}

	OnPostDeleteCallback interface {
		OnPostDelete(context dbContext, i interface{}) error
	}

	OnInitCallback interface {
		OnInit(context dbContext, i interface{}) error
	}
)
