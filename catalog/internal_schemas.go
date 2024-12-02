package catalog

type InternalSchema struct {
	Schema  string
	Catalog string
}

var InternalSchemas = struct {
	MySQL InternalSchema
}{
	MySQL: InternalSchema{
		Schema:  "mysql",
		Catalog: "mysql",
	},
}

var internalSchemas = []InternalSchema{
	InternalSchemas.MySQL,
}
