package catalog

var InitialDataTables = struct {
	PGNamespace [][]any
	PGRange     [][]any
}{
	PGNamespace: [][]any{
		{"99", "pg_toast", "10", ""},
		{"11", "pg_catalog", "10", "{postgres=UC/postgres,=U/postgres}"},
		{"2200", "public", "6171", "{pg_database_owner,=UC/pg_database_owner,=U/pg_database_owner}"},
		{"13219", "information_schema", "10", "{postgres=UC/postgres,=U/postgres}"},
		{"16395", "test_schema", "10", ""},
	},
	PGRange: [][]any{
		{"3904", "23", "4451", "0", "1978", "int4range_canonical", "int4range_subdiff"},
		{"3906", "1700", "4532", "0", "3125", "-", "numrange_subdiff"},
		{"3908", "1114", "4533", "0", "3128", "-", "tsrange_subdiff"},
		{"3910", "1184", "4534", "0", "3127", "-", "tstzrange_subdiff"},
		{"3912", "1082", "4535", "0", "3122", "daterange_canonical", "daterange_subdiff"},
		{"3926", "20", "4536", "0", "3124", "int8range_canonical", "int8range_subdiff"},
	},
}
