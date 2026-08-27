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
