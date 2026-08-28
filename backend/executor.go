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
	stdsql "database/sql"
	"fmt"
	"strings"

	"github.com/apecloud/myduckserver/adapter"
	"github.com/apecloud/myduckserver/catalog"
	"github.com/apecloud/myduckserver/transpiler"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
	vectorfn "github.com/dolthub/go-mysql-server/sql/expression/function/vector"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/rowexec"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/sirupsen/logrus"
)

type DuckBuilder struct {
	base sql.NodeExecBuilder

	provider *catalog.DatabaseProvider

	FlushDeltaBuffer func(*sql.Context) error
}

var _ sql.NodeExecBuilder = (*DuckBuilder)(nil)

// mysqlOkResultNode preserves the OK-result schema expected by MyDuck's DML
// iterator. Newer GMS exposes UPDATE and DELETE child schemas through the DML
// node instead.
type mysqlOkResultNode struct {
	sql.Node
}

func (*mysqlOkResultNode) Schema(*sql.Context) sql.Schema {
	return types.OkResultSchema
}

func withMySQLOkResultSchema(node sql.Node) sql.Node {
	if _, ok := node.(*mysqlOkResultNode); ok {
		return node
	}
	return &mysqlOkResultNode{Node: node}
}

func NewDuckBuilder(base *rowexec.BaseBuilder, provider *catalog.DatabaseProvider) *DuckBuilder {
	fallback := *base
	fallback.PriorityBuilder = nil
	return &DuckBuilder{
		base:     &fallback,
		provider: provider,
	}
}

func (b *DuckBuilder) Provider() *catalog.DatabaseProvider {
	return b.provider
}

func (b *DuckBuilder) Build(ctx *sql.Context, root sql.Node, r sql.Row) (iter sql.RowIter, err error) {
	// The engine passes a nil input row for the top-level result iterator.
	// Subquery iterators must not count intermediate rows against the limit.
	if r == nil {
		defer func() {
			if err == nil && iter != nil {
				iter = ApplyQueryRowLimit(ctx, root.Schema(ctx), iter)
			}
		}()
	}

	// Flush the delta buffer before executing the query. Replication controller
	// commands must remain able to stop an applier while it is connecting and
	// therefore cannot wait on the applier-owned query barrier.
	// TODO(fan): Be fine-grained and flush only when the replicated tables are touched.
	if err := b.flushDeltaBuffer(ctx, root); err != nil {
		return nil, err
	}

	n := root

	if log := ctx.GetLogger(); log.Logger.IsLevelEnabled(logrus.TraceLevel) {
		log.WithFields(logrus.Fields{
			"Query":    ctx.Query(),
			"NodeType": fmt.Sprintf("%T", n),
		}).Traceln("Building node:", n)
	}

	// TODO; find a better way to fallback to the base builder
	switch n.(type) {
	case *plan.CreateDB, *plan.DropDB, *plan.DropTable, *plan.RenameTable,
		*plan.CreateTable, *plan.AddColumn, *plan.RenameColumn, *plan.DropColumn, *plan.ModifyColumn,
		*plan.Truncate,
		*plan.CreateIndex, *plan.DropIndex, *plan.AlterIndex, *plan.ShowIndexes,
		*plan.ShowTables, *plan.ShowCreateTable, *plan.ShowColumns,
		*plan.ShowBinlogs, *plan.ShowBinlogStatus, *plan.ShowWarnings,
		*plan.StartTransaction, *plan.Commit, *plan.Rollback,
		*plan.Set, *plan.ShowVariables,
		*plan.AlterDefaultSet, *plan.AlterDefaultDrop:
		return b.base.Build(ctx, root, r)
	case *plan.InsertInto:
		insert := n.(*plan.InsertInto)

		// The handling of auto_increment reset and check constraints is not supported by DuckDB.
		// We need to fallback to the framework for these cases.
		// But we want to rewrite LOAD DATA to be handled by DuckDB,
		// as it is a common way to import data into the database.
		// Therefore, we ignoring auto_increment and check constraints for LOAD DATA.
		// So rewriting LOAD DATA is done eagerly here.
		src := insert.Source
		if proj, ok := src.(*plan.Project); ok {
			src = proj.Child
		}
		if load, ok := src.(*plan.LoadData); ok {
			if dst, err := plan.GetInsertable(insert.Destination); err == nil && isRewritableLoadData(load) {
				return b.buildLoadData(ctx, root, insert, dst, load)
			}
			return b.base.Build(ctx, root, r)
		}

		if dst, err := plan.GetInsertable(insert.Destination); err == nil {
			// For AUTO_INCREMENT column, we fallback to the framework if the column is specified.
			// if dst.Schema().HasAutoIncrement() && (0 == len(insert.ColumnNames) || len(insert.ColumnNames) == len(dst.Schema())) {
			if dst.Schema(ctx).HasAutoIncrement() {
				return b.base.Build(ctx, root, r)
			}
			// For table with check constraints, we fallback to the framework.
			if ct, ok := dst.(sql.CheckTable); ok {
				if checks, err := ct.GetChecks(ctx); err == nil && len(checks) > 0 {
					return b.base.Build(ctx, root, r)
				}
			}
		}
	}

	// Fallback to the base builder if the plan contains system/user variables or is not a pure data query.
	tree := n
	switch n := n.(type) {
	case *plan.TableCopier:
		tree = n.Source
	}
	if containsVariable(ctx, tree) || (!IsPureDataQuery(ctx, tree) && !isStandaloneVectorConversion(ctx, tree)) {
		ctx.GetLogger().Traceln("Falling back to the base builder")
		return b.base.Build(ctx, root, r)
	}

	conn, err := b.provider.Pool().GetConnForSchema(ctx, ctx.ID(), ctx.GetCurrentDatabase())
	if err != nil {
		return nil, err
	}

	switch node := n.(type) {
	case *plan.Use:
		useStmt := "USE " + catalog.FullSchemaName(adapter.GetCurrentCatalog(ctx), node.Database().Name())
		if _, err := conn.ExecContext(ctx.Context, useStmt); err != nil {
			if catalog.IsDuckDBSetSchemaNotFoundError(err) {
				return nil, sql.ErrDatabaseNotFound.New(node.Database().Name())
			}
			return nil, err
		}
		return b.base.Build(ctx, root, r)
	// ResolvedTable is for `SELECT * FROM table` and `TABLE table`
	// SubqueryAlias is for `SELECT * FROM view`
	case *plan.ResolvedTable, *plan.SubqueryAlias, *plan.TableAlias:
		return b.executeQuery(ctx, node, adapter.SQLExecutorForConn(ctx, conn))
	case *plan.Distinct, *plan.OrderedDistinct:
		return b.executeQuery(ctx, node, adapter.SQLExecutorForConn(ctx, conn))
	case *plan.TableCopier:
		// We preserve the table schema in a best-effort manner.
		// For simple `CREATE TABLE t AS SELECT * FROM t`,
		// we fall back to the framework to create the table and copy the data.
		// For more complex cases, we directly execute the CTAS statement in DuckDB.
		if _, ok := node.Source.(*plan.ResolvedTable); ok {
			return b.base.Build(ctx, root, r)
		}
		return b.executeDML(ctx, node, adapter.SQLExecutorForConn(ctx, conn))
	case *plan.DeleteFrom:
		if node.Returning != nil {
			return b.executeQuery(ctx, node, adapter.SQLExecutorForConn(ctx, conn))
		}
		node.Child = withMySQLOkResultSchema(node.Child)
		return b.executeDML(ctx, node, adapter.SQLExecutorForConn(ctx, conn))
	case sql.Expressioner:
		return b.executeExpressioner(ctx, node, adapter.SQLExecutorForConn(ctx, conn))
	case *plan.Truncate:
		return b.executeDML(ctx, node, adapter.SQLExecutorForConn(ctx, conn))
	default:
		return b.base.Build(ctx, n, r)
	}
}

func (b *DuckBuilder) flushDeltaBuffer(ctx *sql.Context, root sql.Node) error {
	if b.FlushDeltaBuffer == nil || !shouldFlushDeltaBuffer(root) {
		return nil
	}
	return b.FlushDeltaBuffer(ctx)
}

func shouldFlushDeltaBuffer(root sql.Node) bool {
	_, replicationCommand := root.(plan.BinlogReplicaControllerCommand)
	return !replicationCommand
}

// isStandaloneVectorConversion routes SELECT STRING_TO_VECTOR(...) without a
// data table through DuckDB's guarded UDF. The selected GMS version panics when
// its own scalar evaluator encodes an empty vector.
func isStandaloneVectorConversion(ctx *sql.Context, n sql.Node) bool {
	c := &tableAndFuncCollector{ctx: ctx}
	transform.Walk(c, n)

	for _, table := range c.tables {
		if !plan.IsDualTable(table.UnderlyingTable()) {
			return false
		}
	}

	found := false
	for _, fn := range c.functions {
		if _, ok := fn.(*vectorfn.StringToVector); !ok {
			return false
		}
		found = true
	}
	return found
}

func (b *DuckBuilder) executeExpressioner(ctx *sql.Context, n sql.Expressioner, execer adapter.SQLExecutor) (sql.RowIter, error) {
	node := n.(sql.Node)
	switch n := n.(type) {
	case *plan.InsertInto:
		if n.Returning != nil {
			return b.executeQuery(ctx, node, execer)
		}
		return b.executeDML(ctx, node, execer)
	case *plan.Update:
		if n.Returning == nil {
			n.Child = withMySQLOkResultSchema(n.Child)
		}
		return b.executeDML(ctx, node, execer)
	default:
		return b.executeQuery(ctx, node, execer)
	}
}

func (b *DuckBuilder) executeQuery(ctx *sql.Context, n sql.Node, execer adapter.SQLExecutor) (sql.RowIter, error) {
	ctx.GetLogger().Trace("Executing Query...")

	var (
		duckSQL string
		err     error
	)

	// Translate the MySQL query to a DuckDB query
	switch n := n.(type) {
	case *plan.ShowTables:
		duckSQL = ctx.Query()
	case *plan.ResolvedTable:
		// SQLGlot cannot translate MySQL's `TABLE t` into DuckDB's `FROM t` - it produces `"table" AS t` instead.
		duckSQL = `FROM ` + catalog.ConnectIdentifiersANSI(n.Database().Name(), n.Name())
	default:
		duckSQL, err = transpiler.TranslateWithSQLGlot(queryForTranslation(ctx))
	}
	if err != nil {
		return nil, catalog.ErrTranspiler.New(err)
	}
	duckSQL = QueryForJSONScan(duckSQL, n.Schema(ctx))
	duckSQL, err = b.rewriteObjectRelations(ctx, n, duckSQL)
	if err != nil {
		return nil, err
	}

	if log := ctx.GetLogger(); log.Logger.IsLevelEnabled(logrus.TraceLevel) {
		log.WithFields(logrus.Fields{
			"Query":   ctx.Query(),
			"DuckSQL": duckSQL,
		}).Trace("Executing Query...")
	}

	// Execute the DuckDB query
	rows, err := execer.QueryContext(ctx.Context, duckSQL)
	if err != nil {
		return nil, err
	}

	return NewSQLRowIter(rows, n.Schema(ctx))
}

func (b *DuckBuilder) executeDML(ctx *sql.Context, n sql.Node, execer adapter.SQLExecutor) (sql.RowIter, error) {
	// Translate the MySQL query to a DuckDB query
	duckSQL, err := transpiler.TranslateWithSQLGlot(queryForTranslation(ctx))
	if err != nil {
		return nil, catalog.ErrTranspiler.New(err)
	}
	duckSQL, err = b.rewriteObjectRelations(ctx, n, duckSQL)
	if err != nil {
		return nil, err
	}

	if log := ctx.GetLogger(); log.Logger.IsLevelEnabled(logrus.TraceLevel) {
		log.WithFields(logrus.Fields{
			"Query":   ctx.Query(),
			"DuckSQL": duckSQL,
		}).Trace("Executing DML...")
	}

	var affected, insertID int64
	if _, ok := n.(*plan.TableCopier); ok {
		// DuckDB returns the CTAS insert count as a one-row result. Its C API
		// rows-changed value is defined only for INSERT, UPDATE, and DELETE.
		err = execer.QueryRowContext(ctx.Context, duckSQL).Scan(&affected)
	} else {
		var result stdsql.Result
		result, err = execer.ExecContext(ctx.Context, duckSQL)
		if err == nil {
			affected, err = result.RowsAffected()
		}
		if err == nil {
			insertID, err = result.LastInsertId()
		}
	}
	if err != nil {
		if yes, column := catalog.IsDuckDBNotNullConstraintViolationError(err); yes {
			return nil, sql.ErrInsertIntoNonNullableProvidedNull.New(column)
		}
		return nil, err
	}

	var info fmt.Stringer
	if _, ok := n.(*plan.Update); ok {
		// MySQL clients and the engine tests expect UpdateInfo even when
		// CLIENT_FOUND_ROWS is not set. DuckDB only reports rows affected.
		info = plan.UpdateInfo{
			Matched: int(affected),
			Updated: int(affected),
		}
	}

	return sql.RowsToRowIter(sql.NewRow(types.OkResult{
		RowsAffected: uint64(affected),
		InsertID:     uint64(insertID),
		Info:         info,
	})), nil
}

// rewriteObjectRelations keeps GMS's logical shadow tables available for
// analysis while routing the executed SQL to the durable DuckLake relation.
// The plan is the source of truth for relation identity, so a bare table name
// is only routed when it is unambiguous within this statement.
func (b *DuckBuilder) rewriteObjectRelations(ctx *sql.Context, root sql.Node, query string) (string, error) {
	if b.provider == nil || root == nil {
		return query, nil
	}
	collector := &tableAndFuncCollector{ctx: ctx}
	transform.Walk(collector, root)
	routes := make(map[string]string)
	bareRoutes := make(map[string]string)
	bareHasLocal := make(map[string]bool)
	ambiguousBare := make(map[string]bool)
	for _, node := range collector.tables {
		var table *catalog.Table
		switch underlying := node.UnderlyingTable().(type) {
		case *catalog.Table:
			table = underlying
		case *catalog.IndexedTable:
			table = underlying.Table
		}
		if table == nil {
			continue
		}
		physical, object, err := b.provider.PhysicalTableNameForTable(ctx, table)
		if err != nil {
			return "", err
		}
		if !object {
			bareHasLocal[strings.ToLower(table.Name())] = true
			continue
		}
		schema := ""
		if db := node.Database(); db != nil {
			schema = db.Name()
		}
		name := strings.ToLower(table.Name())
		if schema != "" {
			routes[strings.ToLower(schema)+"."+name] = physical
		}
		if ambiguousBare[name] {
			continue
		}
		if previous, exists := bareRoutes[name]; exists && previous != physical {
			delete(bareRoutes, name)
			ambiguousBare[name] = true
			continue
		}
		bareRoutes[name] = physical
	}
	for name, physical := range bareRoutes {
		if !bareHasLocal[name] && !ambiguousBare[name] {
			routes[name] = physical
		}
	}
	rewritten, _ := RewriteSQLRelations(query, routes)
	return rewritten, nil
}

// queryForTranslation canonicalizes ANSI-quoted identifiers before SQLGlot
// reparses the original MySQL query with its default dialect settings.
func queryForTranslation(ctx *sql.Context) string {
	query := ctx.Query()
	sqlMode := sql.LoadSqlMode(ctx)
	if !sqlMode.AnsiQuotes() {
		return query
	}

	parsed, err := sqlparser.ParseWithOptions(ctx, query, sqlMode.ParserOptions())
	if err != nil {
		return query
	}
	return sqlparser.String(parsed)
}

// containsVariable inspects if the plan contains a system or user variable.
func containsVariable(ctx *sql.Context, n sql.Node) bool {
	found := false
	transform.InspectExpressions(ctx, n, func(_ *sql.Context, e sql.Expression) bool {
		switch e.(type) {
		case *expression.SystemVar, *expression.UserVar:
			found = true
			return false
		}
		return true
	})
	return found
}

// IsPureDataQuery inspects if the plan is a pure data query,
// i.e., it operates on (>=1) data tables and does not touch any system tables.
// The following examples are NOT pure data queries:
// - `SELECT * FROM mysql.*`
// - `TRUNCATE mysql.user`
// - `SELECT DATABASE()`
func IsPureDataQuery(ctx *sql.Context, n sql.Node) bool {
	c := &tableAndFuncCollector{ctx: ctx}
	transform.Walk(c, n)

	hasDataTable := false
	for _, tn := range c.tables {
		switch tn.Database().Name() {
		case "mysql", "information_schema", "sys":
			return false
		case "performance_schema":
			// performance_schema is materialized in DuckDB, so it's fine to query it.
		}
		switch tn.UnderlyingTable().(type) {
		case *catalog.Table, *catalog.IndexedTable:
			hasDataTable = true
		}
	}
	if !hasDataTable {
		return false
	}

	for _, fe := range c.functions {
		if _, ok := fe.(*function.Database); ok {
			return false
		}
	}
	return true
}

type tableAndFuncCollector struct {
	ctx       *sql.Context
	functions []sql.FunctionExpression
	tables    []sql.TableNode
}

type exprVisitor tableAndFuncCollector

func (v *exprVisitor) Visit(ctx *sql.Context, expr sql.Expression) sql.Visitor {
	if expr == nil {
		return nil
	} else if fe, ok := expr.(sql.FunctionExpression); ok {
		v.functions = append(v.functions, fe)
	}

	// Visit subquery nodes to collect any nested table references
	if en, ok := expr.(sql.ExpressionWithNodes); ok {
		for _, child := range en.NodeChildren() {
			collector := (*tableAndFuncCollector)(v)
			collector.ctx = ctx
			transform.Walk(collector, child)
		}
	}

	return v
}

func (c *tableAndFuncCollector) Visit(n sql.Node) transform.Visitor {
	if n == nil {
		return nil
	} else if tn, ok := n.(sql.TableNode); ok {
		c.tables = append(c.tables, tn)
	}

	// Visit expressions to find functions e.g. database() and walk subquery nodes to collect any nested table references
	if en, ok := n.(sql.Expressioner); ok {
		for _, e := range en.Expressions() {
			sql.Walk(c.ctx, (*exprVisitor)(c), e)
		}
	}

	return c
}
