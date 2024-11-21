package pgserver

import (
	"testing"
)

func TestGuessStatementTag(t *testing.T) {
	tests := []struct {
		query string
		want  string
	}{
		{"SELECT * FROM table;", "SELECT"},
		{"insert into table values (1);", "INSERT"},
		{"  UPDATE table SET col = 1;", "UPDATE"},
		{"DELETE FROM table;", "DELETE"},
		{"-- comment\nSELECT * FROM table;", "SELECT"},
		{"/* block comment */ INSERT INTO table VALUES (1);", "INSERT"},
		{"\n\n", ""},
		{"INVALID QUERY", "INVALID"},
		{"SELECT/* comment */FROM table;", "SELECT"},
		{"UPDATE-- comment\n table SET col = 1;", "UPDATE"},
		{"DELETE/* multi\nline\ncomment */FROM table;", "DELETE"},
		{"INSERT/* c1 */-- c2\n/* c3 */INTO table;", "INSERT"},
		{"CREATE/* comment */TABLE t1;", "CREATE"},
		{"select from t", "SELECT"},
		{"", ""},
		{"UPDATE(", "UPDATE"},
		{"DELETE.", "DELETE"},
		{"INSERT\n", "INSERT"},
		{"CREATE[", "CREATE"},
		{"drop_table", "DROP_TABLE"},
		{"select", "SELECT"},
	}

	for _, tt := range tests {
		got := GuessStatementTag(tt.query)
		if got != tt.want {
			t.Errorf("GuessStatementTag(%q) = %q; want %q", tt.query, got, tt.want)
		}
	}
}

func TestRemoveLeadingComments(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  string
	}{
		{
			name:  "basic line comment",
			query: "-- comment\nSELECT * FROM table;",
			want:  "SELECT * FROM table;",
		},
		{
			name:  "basic block comment",
			query: "/* block comment */ SELECT * FROM table;",
			want:  "SELECT * FROM table;",
		},
		{
			name:  "nested block comments",
			query: "/* outer /* inner */ comment */ SELECT * FROM table;",
			want:  "SELECT * FROM table;",
		},
		{
			name:  "multiple leading comments",
			query: "/* c1 */-- c2\n/* c3 */ SELECT * FROM table;",
			want:  "SELECT * FROM table;",
		},
		{
			name:  "only whitespace",
			query: "   \t\n  ",
			want:  "",
		},
		{
			name:  "unclosed block comment",
			query: "/* unclosed comment SELECT 1;",
			want:  "",
		},
		{
			name:  "not a leading comment",
			query: "SELECT /* not leading */ 1;",
			want:  "SELECT /* not leading */ 1;",
		},
		{
			name:  "empty input",
			query: "",
			want:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := RemoveLeadingComments(tt.query)
			if got != tt.want {
				t.Errorf("RemoveLeadingComments(%q) = %q; want %q", tt.query, got, tt.want)
			}
		})
	}
}

func TestRemoveComments(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  string
	}{
		{
			name:  "line comments",
			query: "SELECT 1; -- comment\n-- another comment\nSELECT 2;",
			want:  "SELECT 1; \n\nSELECT 2;",
		},
		{
			name:  "block comments",
			query: "SELECT /* in-line */ 1; /* multi\nline\ncomment */ SELECT 2;",
			want:  "SELECT  1;  SELECT 2;",
		},
		{
			name:  "nested block comments",
			query: "SELECT /* outer /* inner */ rest */ 1;",
			want:  "SELECT  1;",
		},
		{
			name:  "comments in string literals",
			query: "SELECT '-- not a comment' AS c1, '/* also not */ a comment' AS c2;",
			want:  "SELECT '-- not a comment' AS c1, '/* also not */ a comment' AS c2;",
		},
		{
			name:  "comments in quoted identifiers",
			query: `SELECT "-- not /* a */ comment" FROM t1;`,
			want:  `SELECT "-- not /* a */ comment" FROM t1;`,
		},
		{
			name:  "dollar quoted strings",
			query: "SELECT $tag$-- not /* a */ comment$tag$, $$ /* not */ comment $$;",
			want:  "SELECT $tag$-- not /* a */ comment$tag$, $$ /* not */ comment $$;",
		},
		{
			name:  "complex dollar quotes",
			query: "SELECT $a$b$/* not $a$b$ comment */$$a$b$, $tag$$tag$;",
			want:  "SELECT $a$b$/* not $a$b$ comment */$$a$b$, $tag$$tag$;",
		},
		{
			name:  "escaped quotes in strings",
			query: "SELECT 'string with \\'-- not a comment\\' continues';",
			want:  "SELECT 'string with \\'-- not a comment\\' continues';",
		},
		{
			name:  "mixed comments and quotes",
			query: "/* c1 */ SELECT -- c2\n'/* c3 */' /* c4 */ FROM /* c5 */ t1;",
			want:  " SELECT \n'/* c3 */'  FROM  t1;",
		},
		{
			name:  "postgres escape string",
			query: "SELECT E'\\t' /* comment */ AS tab;",
			want:  "SELECT E'\\t'  AS tab;",
		},
		{
			name:  "postgres escape string with embedded comments",
			query: "SELECT E'-- not/*a*/comment\\n' FROM t1;",
			want:  "SELECT E'-- not/*a*/comment\\n' FROM t1;",
		},
		{
			name:  "mixed postgres strings",
			query: "SELECT E'\\t', '\\t', /* comment */ E'\\n';",
			want:  "SELECT E'\\t', '\\t',  E'\\n';",
		},
		{
			name:  "empty input",
			query: "",
			want:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := RemoveComments(tt.query)
			if got != tt.want {
				t.Errorf("RemoveComments(%q) = %q; want %q", tt.query, got, tt.want)
			}
		})
	}
}
