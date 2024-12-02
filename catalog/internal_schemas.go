package catalog

type InternalSchema struct {
	Schema string
}

var InternalSchemas = struct {
	MySQL InternalSchema
}{
	MySQL: InternalSchema{
		Schema: "mysql",
	},
}

var internalSchemas = []InternalSchema{
	InternalSchemas.MySQL,
}
