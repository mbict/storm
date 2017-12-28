package example

type User struct {
	Id   int
	Name string
}

type Item struct {
	Id   int
	Name string
}

type Note struct {
	Id      int
	OrderId int
	Message string

	//OneToMany
	ReportedByUserId int
	ReportedByUser   *User
}

type AddressEmbedded struct {
	Street  string
	Country string
}

type Address struct {
	Id int
	//embedded structs are also supported
	AddressEmbedded
}

type Tag struct {
	Id    int
	Value string `db:"name(tag)"`
}

// The junction table definition if using proxies
// you can now use this model for setting the relations
type OrderTagJunction struct {
	OrderId int
	TagId   int
}

type Order struct {
	Id int

	//ignored value
	Ignored string `db:"-"`

	// A scanner slice type column
	// not treated as a relation
	Options Options

	// ManyToMany with junction table
	Items []*Item

	// OneToMany with relation id in relation struct
	Notes []*Note

	// ManyToMany with proxy, need the separate junction table to update or add relations
	Tags []string `db:"rel(Tag),col(Value)"`

	// ManyToOne or OneToOne
	AddressId int
	Address   *Address
}

//scanner type as a slice (e.g. bitwise or comma separated values)
type Options []string

func (*Options) Scan(src interface{}) error {
	return nil
}
