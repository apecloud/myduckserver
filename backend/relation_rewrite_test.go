package backend

import "testing"

func TestRewriteSQLRelationsSkipsLiteralsAndComments(t *testing.T) {
	query := "SELECT 'FROM object_table', \"FROM object_table\" AS text FROM object_table AS o /* JOIN object_table */ JOIN local_table l ON l.id = o.id -- FROM object_table\n"
	want := "SELECT 'FROM object_table', \"FROM object_table\" AS text FROM \"__myduck_ducklake\".\"app\".\"object_table\" AS o /* JOIN object_table */ JOIN local_table l ON l.id = o.id -- FROM object_table\n"
	routes := map[string]string{
		"object_table": `"__myduck_ducklake"."app"."object_table"`,
	}
	got, changed := RewriteSQLRelations(query, routes)
	if !changed {
		t.Fatal("expected relation rewrite")
	}
	if got != want {
		t.Fatalf("rewritten query = %q, want %q", got, want)
	}
}

func TestRewriteSQLRelationsPrefersQualifiedRouteAndPreservesLocal(t *testing.T) {
	query := `SELECT * FROM "app"."orders" o JOIN "orders" local ON local.id = o.id`
	routes := map[string]string{
		"app.orders": `"__myduck_ducklake"."app"."orders"`,
	}
	got, changed := RewriteSQLRelations(query, routes)
	want := `SELECT * FROM "__myduck_ducklake"."app"."orders" o JOIN "orders" local ON local.id = o.id`
	if !changed || got != want {
		t.Fatalf("rewrite = (%q, %v), want (%q, true)", got, changed, want)
	}
}

func TestRewriteSQLRelationsHandlesCommaSeparatedRelations(t *testing.T) {
	query := `SELECT * FROM object_table AS object, other_table WHERE object.id = other_table.id`
	routes := map[string]string{
		"object_table": `"__myduck_ducklake"."app"."object_table"`,
		"other_table":  `"__myduck_ducklake"."app"."other_table"`,
	}
	want := `SELECT * FROM "__myduck_ducklake"."app"."object_table" AS object, "__myduck_ducklake"."app"."other_table" WHERE object.id = other_table.id`
	got, changed := RewriteSQLRelations(query, routes)
	if !changed || got != want {
		t.Fatalf("rewrite = (%q, %v), want (%q, true)", got, changed, want)
	}
}

func TestRewriteSQLRelationsHandlesDMLTargets(t *testing.T) {
	routes := map[string]string{"object_table": `"__myduck_ducklake"."app"."object_table"`}
	for _, query := range []string{
		`INSERT INTO object_table VALUES (1)`,
		`UPDATE object_table SET value = 2`,
		`DELETE FROM object_table WHERE id = 1`,
		`SELECT * FROM object_table JOIN other_table ON true`,
	} {
		got, changed := RewriteSQLRelations(query, routes)
		if !changed || got == query {
			t.Errorf("query %q was not rewritten: %q, %v", query, got, changed)
		}
	}
}

func TestRewriteSQLRelationsHandlesDDLAndCopyTargets(t *testing.T) {
	routes := map[string]string{
		"object_table": `"__myduck_ducklake"."app"."object_table"`,
		"other_table":  `"__myduck_ducklake"."app"."other_table"`,
	}
	tests := []struct {
		query string
		want  string
	}{
		{
			query: `TRUNCATE TABLE ONLY object_table, other_table`,
			want:  `TRUNCATE TABLE ONLY "__myduck_ducklake"."app"."object_table", "__myduck_ducklake"."app"."other_table"`,
		},
		{
			query: `DROP TABLE IF EXISTS "app"."object_table", other_table CASCADE`,
			want:  `DROP TABLE IF EXISTS "__myduck_ducklake"."app"."object_table", "__myduck_ducklake"."app"."other_table" CASCADE`,
		},
		{
			query: `ALTER TABLE IF EXISTS ONLY object_table ADD COLUMN n INT`,
			want:  `ALTER TABLE IF EXISTS ONLY "__myduck_ducklake"."app"."object_table" ADD COLUMN n INT`,
		},
		{
			query: `COPY object_table FROM STDIN`,
			want:  `COPY "__myduck_ducklake"."app"."object_table" FROM STDIN`,
		},
		{
			query: `CREATE INDEX idx ON object_table (id)`,
			want:  `CREATE INDEX idx ON "__myduck_ducklake"."app"."object_table" (id)`,
		},
	}
	for _, test := range tests {
		got, changed := RewriteSQLRelations(test.query, routes)
		if !changed || got != test.want {
			t.Errorf("rewrite(%q) = (%q, %v), want (%q, true)", test.query, got, changed, test.want)
		}
	}
}

func TestRewriteSQLRelationsSkipsDollarQuotedBodies(t *testing.T) {
	query := `DO $$ BEGIN INSERT INTO object_table VALUES (1); END $$; SELECT * FROM object_table`
	routes := map[string]string{"object_table": `"__myduck_ducklake"."app"."object_table"`}
	want := `DO $$ BEGIN INSERT INTO object_table VALUES (1); END $$; SELECT * FROM "__myduck_ducklake"."app"."object_table"`
	got, changed := RewriteSQLRelations(query, routes)
	if !changed || got != want {
		t.Fatalf("rewrite = (%q, %v), want (%q, true)", got, changed, want)
	}
}
