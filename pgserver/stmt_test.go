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
		query string
		want  string
	}{
		{"-- comment\nSELECT * FROM table;", "SELECT * FROM table;"},
		{"/* block comment */ SELECT * FROM table;", "SELECT * FROM table;"},
		{"   \t\nSELECT * FROM table;", "SELECT * FROM table;"},
		{"/* comment */ -- another comment\nSELECT * FROM table;", "SELECT * FROM table;"},
		{"SELECT * FROM table;", "SELECT * FROM table;"},
		{"", ""},
	}

	for _, tt := range tests {
		got := RemoveLeadingComments(tt.query)
		if got != tt.want {
			t.Errorf("RemoveLeadingComments(%q) = %q; want %q", tt.query, got, tt.want)
		}
	}
}
