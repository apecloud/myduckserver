package pgserver

import (
	"testing"
)

func TestParseCopyOptions(t *testing.T) {
	tests := []struct {
		name     string
		options  string
		allowed  map[string]OptionValueType
		expected map[string]any
		wantErr  bool
	}{
		{
			name:    "Valid options with different types",
			options: "OPT1 1, OPT2, OPT3 'v3', OPT4 E'v4', OPT5 3.14",
			allowed: map[string]OptionValueType{
				"OPT1": OptionValueTypeInt,
				"OPT2": OptionValueTypeBool,
				"OPT3": OptionValueTypeString,
				"OPT4": OptionValueTypeString,
				"OPT5": OptionValueTypeFloat,
			},
			expected: map[string]any{
				"OPT1": 1,
				"OPT2": true,
				"OPT3": "v3",
				"OPT4": "v4",
				"OPT5": 3.14,
			},
			wantErr: false,
		},
		{
			name:    "Unsupported option",
			options: "OPT1 1, OPT6 'unsupported'",
			allowed: map[string]OptionValueType{
				"OPT1": OptionValueTypeInt,
			},
			expected: nil,
			wantErr:  true,
		},
		{
			name:    "Invalid int value",
			options: "OPT1 'invalid'",
			allowed: map[string]OptionValueType{
				"OPT1": OptionValueTypeInt,
			},
			expected: nil,
			wantErr:  true,
		},
		{
			name:    "Invalid float value",
			options: "OPT1 'invalid'",
			allowed: map[string]OptionValueType{
				"OPT1": OptionValueTypeFloat,
			},
			expected: nil,
			wantErr:  true,
		},
		{
			name:    "Boolean option with explicit value",
			options: "OPT1 false",
			allowed: map[string]OptionValueType{
				"OPT1": OptionValueTypeBool,
			},
			expected: map[string]any{
				"OPT1": false,
			},
			wantErr: false,
		},
		{
			name:    "Boolean option without value",
			options: "OPT1",
			allowed: map[string]OptionValueType{
				"OPT1": OptionValueTypeBool,
			},
			expected: map[string]any{
				"OPT1": true,
			},
			wantErr: false,
		},
		{
			name:    "String option with escaped quotes",
			options: "OPT1 'value with ''escaped'' quotes'",
			allowed: map[string]OptionValueType{
				"OPT1": OptionValueTypeString,
			},
			expected: map[string]any{
				"OPT1": "value with 'escaped' quotes",
			},
			wantErr: false,
		},
		{
			name:    "String option with E-escaped newlines",
			options: "OPT1 E'line1\\nline2'",
			allowed: map[string]OptionValueType{
				"OPT1": OptionValueTypeString,
			},
			expected: map[string]any{
				"OPT1": "line1\nline2",
			},
			wantErr: false,
		},
		{
			name:    "String option with escaped backslash",
			options: "OPT1 E'\\\\path\\\\to\\\\file'",
			allowed: map[string]OptionValueType{
				"OPT1": OptionValueTypeString,
			},
			expected: map[string]any{
				"OPT1": "\\path\\to\\file",
			},
			wantErr: false,
		},
		{
			name:    "String option with unbalanced quotes",
			options: "OPT1 'unbalanced",
			allowed: map[string]OptionValueType{
				"OPT1": OptionValueTypeString,
			},
			expected: nil,
			wantErr:  true,
		},
		{
			name:    "String option with spaces",
			options: "OPT1 'value with spaces'",
			allowed: map[string]OptionValueType{
				"OPT1": OptionValueTypeString,
			},
			expected: map[string]any{
				"OPT1": "value with spaces",
			},
			wantErr: false,
		},
		{
			name:    "String option with special characters",
			options: "OPT1 'special !@#$%^&*() characters'",
			allowed: map[string]OptionValueType{
				"OPT1": OptionValueTypeString,
			},
			expected: map[string]any{
				"OPT1": "special !@#$%^&*() characters",
			},
			wantErr: false,
		},
		{
			name:    "String option with double backslashes",
			options: "OPT1 E'\\\\\\\\'",
			allowed: map[string]OptionValueType{
				"OPT1": OptionValueTypeString,
			},
			expected: map[string]any{
				"OPT1": "\\\\",
			},
			wantErr: false,
		},
		{
			name:    "Mixed options with quoted and unquoted values",
			options: "OPT1 'string value', OPT2 42, OPT3 E'escaped\\tvalue', OPT4 true",
			allowed: map[string]OptionValueType{
				"OPT1": OptionValueTypeString,
				"OPT2": OptionValueTypeInt,
				"OPT3": OptionValueTypeString,
				"OPT4": OptionValueTypeBool,
			},
			expected: map[string]any{
				"OPT1": "string value",
				"OPT2": 42,
				"OPT3": "escaped\tvalue",
				"OPT4": true,
			},
			wantErr: false,
		},
		{
			name:    "Option with empty string value",
			options: "OPT1 ''",
			allowed: map[string]OptionValueType{
				"OPT1": OptionValueTypeString,
			},
			expected: map[string]any{
				"OPT1": "",
			},
			wantErr: false,
		},
		{
			name:    "Option with only spaces in quotes",
			options: "OPT1 '   '",
			allowed: map[string]OptionValueType{
				"OPT1": OptionValueTypeString,
			},
			expected: map[string]any{
				"OPT1": "   ",
			},
			wantErr: false,
		},
		{
			name:    "String option with escaped single quote",
			options: "OPT1 'it''s a test'",
			allowed: map[string]OptionValueType{
				"OPT1": OptionValueTypeString,
			},
			expected: map[string]any{
				"OPT1": "it's a test",
			},
			wantErr: false,
		},
		{
			name:    "Option with missing value after E prefix",
			options: "OPT1 E",
			allowed: map[string]OptionValueType{
				"OPT1": OptionValueTypeString,
			},
			expected: nil,
			wantErr:  true,
		},
		{
			name:    "Option with invalid escape sequence",
			options: "OPT1 E'\\x'",
			allowed: map[string]OptionValueType{
				"OPT1": OptionValueTypeString,
			},
			expected: nil,
			wantErr:  true,
		},
		{
			name:    "Option with numeric value in quotes",
			options: "OPT1 '123'",
			allowed: map[string]OptionValueType{
				"OPT1": OptionValueTypeInt,
			},
			expected: nil,
			wantErr:  true,
		},
		{
			name:    "Option with float value in quotes",
			options: "OPT1 '123.456'",
			allowed: map[string]OptionValueType{
				"OPT1": OptionValueTypeFloat,
			},
			expected: nil,
			wantErr:  true,
		},
		{
			name:    "Option with multiple consecutive commas",
			options: "OPT1 'value1',, OPT2 'value2'",
			allowed: map[string]OptionValueType{
				"OPT1": OptionValueTypeString,
				"OPT2": OptionValueTypeString,
			},
			expected: map[string]any{
				"OPT1": "value1",
				"OPT2": "value2",
			},
			wantErr: false,
		},
		{
			name:    "Option with missing comma between options",
			options: "OPT1 'value1' OPT2 'value2'",
			allowed: map[string]OptionValueType{
				"OPT1": OptionValueTypeString,
				"OPT2": OptionValueTypeString,
			},
			expected: nil,
			wantErr:  true,
		},
		{
			name:    "Option with excess whitespace",
			options: "   OPT1    'value1'  ,   OPT2    'value2'   ",
			allowed: map[string]OptionValueType{
				"OPT1": OptionValueTypeString,
				"OPT2": OptionValueTypeString,
			},
			expected: map[string]any{
				"OPT1": "value1",
				"OPT2": "value2",
			},
			wantErr: false,
		},
		{
			name:    "Option with tab and newline characters",
			options: "OPT1 E'line1\\nline2\\tend'",
			allowed: map[string]OptionValueType{
				"OPT1": OptionValueTypeString,
			},
			expected: map[string]any{
				"OPT1": "line1\nline2\tend",
			},
			wantErr: false,
		},
		{
			name:    "Option with hexadecimal escape sequence",
			options: "OPT1 E'\\x41\\x42\\x43'",
			allowed: map[string]OptionValueType{
				"OPT1": OptionValueTypeString,
			},
			expected: map[string]any{
				"OPT1": "ABC",
			},
			wantErr: false,
		},
		{
			name:    "Option with Unicode escape sequence",
			options: "OPT1 E'\\u263A'",
			allowed: map[string]OptionValueType{
				"OPT1": OptionValueTypeString,
			},
			expected: map[string]any{
				"OPT1": "☺",
			},
			wantErr: false,
		},
		{
			name:    "Option with null character",
			options: "OPT1 E'null\\0char'",
			allowed: map[string]OptionValueType{
				"OPT1": OptionValueTypeString,
			},
			expected: nil,
			wantErr:  true,
		},
		{
			name:     "Empty options string",
			options:  "",
			allowed:  map[string]OptionValueType{},
			expected: map[string]any{},
			wantErr:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParseCopyOptions(tt.options, tt.allowed)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseCopyOptions() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if len(result) != len(tt.expected) {
					t.Errorf("ParseCopyOptions() got = %v, want %v", result, tt.expected)
				}
				for k, v := range tt.expected {
					if result[k] != v {
						t.Errorf("ParseCopyOptions() got = %v, want %v", result, tt.expected)
					}
				}
			}
		})
	}
}
