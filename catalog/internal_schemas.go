package catalog

type InternalSchema struct {
	Schema string
}

var InternalSchemas = struct {
	SYS   InternalSchema
	MySQL InternalSchema
}{
	SYS: InternalSchema{
		Schema: "__sys__",
	},
	MySQL: InternalSchema{
		Schema: "mysql",
	},
}

var internalSchemas = []InternalSchema{
	InternalSchemas.MySQL, InternalSchemas.SYS,
}
