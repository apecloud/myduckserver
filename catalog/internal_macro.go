package catalog

import "strings"

type MacroDefinition struct {
	Params []string
	DDL    string
}

var (
	SchemaNameSYS           string = "__sys__"
	MacroNameMyListContains string = "my_list_contains"

	MacroNameMySplitListStr string = "my_split_list_str"
)

type InternalMacro struct {
	Schema       string
	Name         string
	IsTableMacro bool
	// A macro can be overloaded with multiple definitions, each with a different set of parameters.
	// https://duckdb.org/docs/sql/statements/create_macro.html#overloading
	Definitions []MacroDefinition
}

func (v *InternalMacro) QualifiedName() string {
	if strings.ToLower(v.Schema) == "pg_catalog" {
		return "__sys__." + v.Name
	}
	return v.Schema + "." + v.Name
}

var InternalMacros = []InternalMacro{
	{
		Schema:       "information_schema",
		Name:         "_pg_expandarray",
		IsTableMacro: true,
		Definitions: []MacroDefinition{
			{
				Params: []string{"a"},
				DDL: `SELECT STRUCT_PACK(
    x := unnest(a),
    n := generate_series(1, array_length(a))
) AS item`,
			},
		},
	},
	{
		Schema:       "pg_catalog",
		Name:         "pg_get_indexdef",
		IsTableMacro: false,
		Definitions: []MacroDefinition{
			{
				Params: []string{"index_oid"},
				// Do nothing currently
				DDL: `''`,
			},
			{
				Params: []string{"index_oid", "column_no", "pretty_bool"},
				// Do nothing currently
				DDL: `''`,
			},
		},
	},
	{
		Schema:       "pg_catalog",
		Name:         "pg_get_expr",
		IsTableMacro: false,
		Definitions: []MacroDefinition{
			{
				Params: []string{"pg_node_tree", "relation_oid"},
				// Do nothing currently
				DDL: `pg_catalog.pg_get_expr(pg_node_tree, relation_oid)`,
			},
			{
				Params: []string{"pg_node_tree", "relation_oid", "pretty_bool"},
				// Do nothing currently
				DDL: `pg_catalog.pg_get_expr(pg_node_tree, relation_oid)`,
			},
		},
	},
	{
		Schema:       SchemaNameSYS,
		Name:         MacroNameMyListContains,
		IsTableMacro: false,
		Definitions: []MacroDefinition{
			{
				Params: []string{"l", "v"},
				DDL: `CASE
    WHEN typeof(l) = 'VARCHAR' THEN
        list_contains(regexp_split_to_array(l::VARCHAR, '[{},\s]+'), v)
    ELSE
        list_contains(l::text[], v)
    END`,
			},
		},
	},
	{
		Schema:       SchemaNameSYS,
		Name:         MacroNameMySplitListStr,
		IsTableMacro: false,
		Definitions: []MacroDefinition{
			{
				Params: []string{"l"},
				DDL:    `regexp_split_to_array(l::VARCHAR, '[{},\s]+')`,
			},
		},
	},
}
