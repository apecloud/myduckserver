// Copyright 2024-2025 ApeCloud, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package backend

import (
	"context"
	"strings"
	"unicode"

	"github.com/apecloud/myduckserver/catalog"
	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/vt/sqlparser"
)

// NewEngine constructs the MySQL engine with MyDuck's parser and executor
// compatibility boundaries installed consistently.
func NewEngine(provider *catalog.DatabaseProvider) (*sqle.Engine, *DuckBuilder) {
	registerMySQLCompatibilitySystemVariables()
	parser := &mysqlParser{Parser: sql.NewMysqlParser()}
	overrides := sql.EngineOverrides{
		Builder: sql.BuilderOverrides{Parser: parser},
	}
	engine := sqle.New(analyzer.NewBuilder(provider).AddOverrides(overrides).Build(), nil)
	builder := NewDuckBuilder(engine.Analyzer.ExecBuilder, provider)
	engine.Analyzer.ExecBuilder.PriorityBuilder = builder
	return engine, builder
}

// registerMySQLCompatibilitySystemVariables keeps MyDuck's advertised SQL
// compatibility level stable across GMS upgrades. Clients such as MySQL Shell
// branch on @@version and otherwise probe newer variables MyDuck does not
// implement.
func registerMySQLCompatibilitySystemVariables() {
	const compatibilityVersion = "8.0.23"
	sql.SystemVariables.AddSystemVariables([]sql.SystemVariable{
		&sql.MysqlSystemVariable{
			Name:              "version",
			Scope:             sql.GetMysqlScope(sql.SystemVariableScope_Global),
			Dynamic:           false,
			SetVarHintApplies: false,
			Type:              types.NewSystemStringType("version"),
			Default:           compatibilityVersion,
		},
	})
}

// mysqlParser restores MyDuck syntax and AST contracts that are missing from
// the selected Vitess parser.
type mysqlParser struct {
	sql.Parser
}

func (p *mysqlParser) ParseSimple(query string) (sqlparser.Statement, error) {
	compat := rewriteMySQLCompatibility(query)
	stmt, err := p.Parser.ParseSimple(compat.query)
	return normalizeMySQLStatement(stmt, compat.replacements), err
}

func (p *mysqlParser) Parse(ctx *sql.Context, query string, multi bool) (sqlparser.Statement, string, string, error) {
	compat := rewriteMySQLCompatibility(query)
	stmt, parsed, remainder, err := p.Parser.Parse(ctx, compat.query, multi)
	return normalizeMySQLStatement(stmt, compat.replacements), compat.restoreParsedQuery(parsed), remainder, err
}

func (p *mysqlParser) ParseWithOptions(
	ctx context.Context,
	query string,
	delimiter rune,
	multi bool,
	options sqlparser.ParserOptions,
) (sqlparser.Statement, string, string, error) {
	compat := rewriteMySQLCompatibility(query)
	stmt, parsed, remainder, err := p.Parser.ParseWithOptions(ctx, compat.query, delimiter, multi, options)
	return normalizeMySQLStatement(stmt, compat.replacements), compat.restoreParsedQuery(parsed), remainder, err
}

func (p *mysqlParser) ParseOneWithOptions(
	ctx context.Context,
	query string,
	options sqlparser.ParserOptions,
) (sqlparser.Statement, int, error) {
	compat := rewriteMySQLCompatibility(query)
	stmt, index, err := p.Parser.ParseOneWithOptions(ctx, compat.query, options)
	return normalizeMySQLStatement(stmt, compat.replacements), compat.originalOffset(index), err
}

type mysqlOptionReplacement struct {
	optionIndex int
	start       int
	end         int
	name        string
	alias       string
}

type mysqlParserCompat struct {
	original     string
	query        string
	replacements []mysqlOptionReplacement
}

func (c mysqlParserCompat) restoreParsedQuery(parsed string) string {
	if len(c.replacements) == 0 || parsed == "" {
		return parsed
	}

	trimmed := strings.TrimLeftFunc(c.query, unicode.IsSpace)
	offset := len(c.query) - len(trimmed)
	if !strings.HasPrefix(trimmed, parsed) {
		return parsed
	}

	end := c.originalOffset(offset + len(parsed))
	if end < offset || end > len(c.original) {
		return parsed
	}
	return c.original[offset:end]
}

func (c mysqlParserCompat) originalOffset(rewrittenOffset int) int {
	if len(c.replacements) == 0 || rewrittenOffset <= 0 {
		return rewrittenOffset
	}

	originalPos := 0
	rewrittenPos := 0
	for _, replacement := range c.replacements {
		unchanged := replacement.start - originalPos
		if rewrittenOffset <= rewrittenPos+unchanged {
			return originalPos + rewrittenOffset - rewrittenPos
		}
		rewrittenPos += unchanged
		originalPos = replacement.start

		aliasEnd := rewrittenPos + len(replacement.alias)
		if rewrittenOffset <= aliasEnd {
			inside := rewrittenOffset - rewrittenPos
			if originalLength := replacement.end - replacement.start; inside > originalLength {
				inside = originalLength
			}
			return replacement.start + inside
		}
		rewrittenPos = aliasEnd
		originalPos = replacement.end
	}
	return originalPos + rewrittenOffset - rewrittenPos
}

func (c *mysqlParserCompat) applyReplacements() {
	if len(c.replacements) == 0 {
		return
	}

	var rewritten strings.Builder
	last := 0
	for _, replacement := range c.replacements {
		rewritten.WriteString(c.original[last:replacement.start])
		rewritten.WriteString(replacement.alias)
		last = replacement.end
	}
	rewritten.WriteString(c.original[last:])
	c.query = rewritten.String()
}

func rewriteMySQLCompatibility(query string) mysqlParserCompat {
	compat := rewriteReplicationSourceOptions(query)
	if len(compat.replacements) > 0 {
		return compat
	}
	return rewriteReplicationFilterOptions(query)
}

// rewriteReplicationSourceOptions maps the two file-position options removed
// from the selected Vitess grammar onto same-type options it still accepts.
// The aliases have the same byte width as the original tokens so parser error
// positions and multi-statement offsets remain stable. The AST names are
// restored by normalizeMySQLStatement.
func rewriteReplicationSourceOptions(query string) mysqlParserCompat {
	compat := mysqlParserCompat{original: query, query: query}
	tokenizer := sqlparser.NewStringTokenizer(query)
	prefix := []int{sqlparser.CHANGE, sqlparser.REPLICATION, sqlparser.SOURCE, sqlparser.TO}
	prefixIndex := 0

	for prefixIndex < len(prefix) {
		token, _ := tokenizer.Scan()
		if token == sqlparser.COMMENT {
			continue
		}
		if token != prefix[prefixIndex] {
			return compat
		}
		prefixIndex++
	}

	type parseState uint8
	const (
		expectOption parseState = iota
		expectEquals
		expectValue
		expectSeparator
	)

	state := expectOption
	optionIndex := 0
	var pending *mysqlOptionReplacement
	for {
		token, value := tokenizer.Scan()
		if token == sqlparser.COMMENT {
			continue
		}

		switch state {
		case expectOption:
			if token == 0 || token == ';' {
				return mysqlParserCompat{original: query, query: query}
			}
			name := string(value)
			var alias string
			switch {
			case strings.EqualFold(name, "SOURCE_LOG_FILE"):
				alias = "SOURCE_PASSWORD"
			case strings.EqualFold(name, "SOURCE_LOG_POS"):
				alias = "SOURCE_PORT"
			}
			if alias != "" {
				end := tokenizer.Position - 1
				alias += strings.Repeat(" ", len(value)-len(alias))
				pending = &mysqlOptionReplacement{
					optionIndex: optionIndex,
					start:       end - len(value),
					end:         end,
					name:        name,
					alias:       alias,
				}
			}
			state = expectEquals

		case expectEquals:
			if token != '=' {
				return mysqlParserCompat{original: query, query: query}
			}
			state = expectValue

		case expectValue:
			if pending != nil {
				if strings.EqualFold(pending.name, "SOURCE_LOG_FILE") && token != sqlparser.STRING {
					return mysqlParserCompat{original: query, query: query}
				}
				if strings.EqualFold(pending.name, "SOURCE_LOG_POS") && token != sqlparser.INTEGRAL {
					return mysqlParserCompat{original: query, query: query}
				}
				compat.replacements = append(compat.replacements, *pending)
				pending = nil
			}
			state = expectSeparator

		case expectSeparator:
			switch token {
			case ',':
				optionIndex++
				state = expectOption
			case 0, ';':
				compat.applyReplacements()
				return compat
			default:
				return mysqlParserCompat{original: query, query: query}
			}
		}
	}
}

// rewriteReplicationFilterOptions maps the DB-level filters removed from the
// selected Vitess grammar onto its table-filter productions. Both productions
// parse the same TableNames value; normalizeMySQLStatement restores the option
// name before GMS applies the filter.
func rewriteReplicationFilterOptions(query string) mysqlParserCompat {
	compat := mysqlParserCompat{original: query, query: query}
	tokenizer := sqlparser.NewStringTokenizer(query)
	prefix := []int{sqlparser.CHANGE, sqlparser.REPLICATION, sqlparser.FILTER}
	for _, expected := range prefix {
		token, _ := tokenizer.Scan()
		for token == sqlparser.COMMENT {
			token, _ = tokenizer.Scan()
		}
		if token != expected {
			return compat
		}
	}

	optionIndex := 0
	expectOption := true
	depth := 0
	for {
		token, value := tokenizer.Scan()
		if token == sqlparser.COMMENT {
			continue
		}

		if expectOption {
			if token == 0 || token == ';' {
				return mysqlParserCompat{original: query, query: query}
			}
			name := string(value)
			var alias string
			switch {
			case strings.EqualFold(name, "REPLICATE_DO_DB"):
				alias = "REPLICATE_DO_TABLE"
			case strings.EqualFold(name, "REPLICATE_IGNORE_DB"):
				alias = "REPLICATE_IGNORE_TABLE"
			}
			if alias != "" {
				end := tokenizer.Position - 1
				compat.replacements = append(compat.replacements, mysqlOptionReplacement{
					optionIndex: optionIndex,
					start:       end - len(value),
					end:         end,
					name:        name,
					alias:       alias,
				})
			}
			expectOption = false
			continue
		}

		switch token {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case ',':
			if depth == 0 {
				optionIndex++
				expectOption = true
			}
		case 0, ';':
			compat.applyReplacements()
			return compat
		}
	}
}

func normalizeMySQLStatement(
	stmt sqlparser.Statement,
	replacements []mysqlOptionReplacement,
) sqlparser.Statement {
	ddl, ok := stmt.(*sqlparser.DDL)
	if ok && ddl.ViewSpec != nil && ddl.Table.IsEmpty() {
		ddl.Table = ddl.ViewSpec.ViewName
	}

	changeSource, ok := stmt.(*sqlparser.ChangeReplicationSource)
	if ok {
		for _, replacement := range replacements {
			if replacement.optionIndex < len(changeSource.Options) {
				changeSource.Options[replacement.optionIndex].Name = replacement.name
			}
		}
	}

	changeFilter, ok := stmt.(*sqlparser.ChangeReplicationFilter)
	if ok {
		for _, replacement := range replacements {
			if replacement.optionIndex < len(changeFilter.Options) {
				changeFilter.Options[replacement.optionIndex].Name = replacement.name
			}
		}
	}
	return stmt
}
