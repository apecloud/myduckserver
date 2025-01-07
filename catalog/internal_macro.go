package catalog

type InternalMacro struct {
	Schema       string
	Name         string
	Params       []string
	IsTableMacro bool
	DDL          string
}

func (v *InternalMacro) QualifiedName() string {
	return v.Schema + "." + v.Name
}

var InternalMacros = []InternalMacro{
	{
		Schema:       "information_schema",
		Name:         "_pg_expandarray",
		Params:       []string{"a"},
		IsTableMacro: true,
		DDL: `
      SELECT STRUCT_PACK(
          x := unnest(a),
          n := generate_series(1, array_length(a))
      ) AS item;`,
	},
}
