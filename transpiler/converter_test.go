package transpiler

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Test converting MySQL strings to Postgres strings
func TestNormalizeStrings(t *testing.T) {
	type test struct {
		input    string
		expected string
	}
	tests := []test{
		{
			input:    "SELECT \"foo\" FROM `bar`",
			expected: `SELECT 'foo' FROM "bar"`,
		},
		{
			input:    `SELECT "foo"`,
			expected: `SELECT 'foo'`,
		},
		{
			input:    `SELECT "fo\"o"`,
			expected: `SELECT 'fo"o'`,
		},
		{
			input:    `SELECT "fo\'o"`,
			expected: `SELECT 'fo''o'`,
		},
		{
			input:    `SELECT 'fo\'o'`,
			expected: `SELECT 'fo''o'`,
		},
		{
			input:    `SELECT 'fo\"o'`,
			expected: `SELECT 'fo"o'`,
		},
		{
			input:    `SELECT 'fo\\"o'`,
			expected: `SELECT 'fo\"o'`,
		},
		{
			input:    `SELECT 'fo\\\'o'`,
			expected: `SELECT 'fo\''o'`,
		},
		{
			input:    `SELECT "fo\\'o"`,
			expected: `SELECT 'fo\''o'`,
		},
		{
			input:    `SELECT "fo\\\"o"`,
			expected: `SELECT 'fo\"o'`,
		},
		{
			input:    "SELECT 'fo''o'",
			expected: `SELECT 'fo''o'`,
		},
		{
			input:    "SELECT 'fo''''o'",
			expected: `SELECT 'fo''''o'`,
		},
		{
			input:    `SELECT "fo'o"`,
			expected: `SELECT 'fo''o'`,
		},
		{
			input:    `SELECT "fo''o"`,
			expected: `SELECT 'fo''''o'`,
		},
		{
			input:    `SELECT "fo""o"`,
			expected: `SELECT 'fo"o'`,
		},
		{
			input:    `SELECT "fo""""o"`,
			expected: `SELECT 'fo""o'`,
		},
		{
			input:    `SELECT 'fo""o'`,
			expected: `SELECT 'fo""o'`,
		},
		{
			input:    "SELECT `foo` FROM `bar`",
			expected: `SELECT "foo" FROM "bar"`,
		},
		{
			input:    "SELECT 'foo' FROM `bar`",
			expected: `SELECT 'foo' FROM "bar"`,
		},
		{
			input:    "SELECT `f\"o'o` FROM `ba``r`",
			expected: "SELECT \"f\"o'o\" FROM \"ba`r\"",
		},
		{
			input:    "SELECT \"foo\" from `bar` where `bar`.`baz` = \"qux\"",
			expected: `SELECT 'foo' from "bar" where "bar"."baz" = 'qux'`,
		},
	}

	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			actual := NormalizeStrings(test.input)
			require.Equal(t, test.expected, actual)
		})
	}
}

// Test converting Postgres strings to MySQL strings
func TestDeNormalizeStrings(t *testing.T) {
	type test struct {
		input    string
		expected string
	}
	tests := []test{
		{
			input:    `SELECT 'foo' FROM "bar"`,
			expected: "SELECT 'foo' FROM `bar`",
		},
		{
			input:    `SELECT 'foo'`,
			expected: `SELECT 'foo'`,
		},
		{
			input:    `SELECT 'fo"o'`,
			expected: `SELECT 'fo"o'`,
		},
		{
			input:    `SELECT 'fo''o'`,
			expected: `SELECT 'fo''o'`,
		},
		{
			input:    `SELECT 'fo"o'`,
			expected: `SELECT 'fo"o'`,
		},
		{
			input:    `SELECT 'fo''o'`,
			expected: "SELECT 'fo''o'",
		},
		{
			input:    `SELECT 'fo''''o'`,
			expected: "SELECT 'fo''''o'",
		},
		{
			input:    `SELECT "foo" FROM "bar"`,
			expected: "SELECT `foo` FROM `bar`",
		},
		{
			input:    `SELECT 'foo' FROM "bar"`,
			expected: "SELECT 'foo' FROM `bar`",
		},
		{
			input:    `SELECT 'foo' from "bar" where "bar"."baz" = 'qux'`,
			expected: "SELECT 'foo' from `bar` where `bar`.`baz` = 'qux'",
		},
		{
			input:    `SELECT "fo""o" FROM "bar"`,
			expected: "SELECT `fo\"o` FROM `bar`",
		},
		{
			input:    "SELECT \"fo`o\" FROM \"bar\"",
			expected: "SELECT `fo``o` FROM `bar`",
		},
		{
			input:    `SELECT 'fo""o' FROM "bar"`,
			expected: "SELECT 'fo\"\"o' FROM `bar`",
		},
	}

	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			actual := DenormalizeStrings(test.input)
			require.Equal(t, test.expected, actual)
		})
	}
}
