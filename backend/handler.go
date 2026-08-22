// Copyright 2024-2025 ApeCloud, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package backend

import (
	"context"
	"fmt"

	"github.com/apecloud/myduckserver/catalog"
	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/sqltypes"
	querypb "github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/sirupsen/logrus"
)

type MyHandler struct {
	*server.Handler
	provider *catalog.DatabaseProvider
	engine   *sqle.Engine
	readOnly bool
}

func (h *MyHandler) ConnectionClosed(c *mysql.Conn) {
	h.provider.Pool().CloseConn(c.ConnectionID)
	h.Handler.ConnectionClosed(c)
}

func (h *MyHandler) ComInitDB(c *mysql.Conn, schemaName string) error {
	_, err := h.provider.Pool().GetConnForSchema(context.Background(), c.ConnectionID, schemaName)
	if err != nil {
		return err
	}
	return h.Handler.ComInitDB(c, schemaName)
}

func wrapResultCallback(callback mysql.ResultSpoolFn, modifiers ...ResultModifier) mysql.ResultSpoolFn {
	return func(res *sqltypes.Result, more bool) error {
		// Apply all modifiers in sequence
		result := res
		for _, modifier := range modifiers {
			result = modifier(result)
		}
		return callback(result, more)
	}
}

func (h *MyHandler) ComMultiQuery(
	ctx context.Context,
	c *mysql.Conn,
	query string,
	callback mysql.ResultSpoolFn,
) (string, error) {
	if err := h.rejectReadOnly(ctx, c, query); err != nil {
		return query, err
	}

	var modifiers []ResultModifier
	query, modifiers = applyRequestModifiers(query, defaultRequestModifiers)

	called := false
	cb := func(res *sqltypes.Result, more bool) error {
		called = true
		return wrapResultCallback(callback, modifiers...)(res, more)
	}
	rest, err := h.Handler.ComMultiQuery(ctx, c, query, cb)
	if err != nil && !called && shouldIgnoreFailedView(query, isReplicaLoadingSnapshot()) {
		logrus.WithError(err).Warn("skipping CREATE VIEW during replica snapshot")
		return rest, callback(&sqltypes.Result{}, false)
	}
	return rest, err
}

// Naive query rewriting. This is just a temporary solution
// and should be replaced with a more robust implementation.
func (h *MyHandler) ComQuery(
	ctx context.Context,
	c *mysql.Conn,
	query string,
	callback mysql.ResultSpoolFn,
) error {
	if err := h.rejectReadOnly(ctx, c, query); err != nil {
		return err
	}

	var modifiers []ResultModifier
	query, modifiers = applyRequestModifiers(query, defaultRequestModifiers)

	called := false
	cb := func(res *sqltypes.Result, more bool) error {
		called = true
		return wrapResultCallback(callback, modifiers...)(res, more)
	}
	err := h.Handler.ComQuery(ctx, c, query, cb)
	if err != nil && !called && shouldIgnoreFailedView(query, isReplicaLoadingSnapshot()) {
		logrus.WithError(err).Warn("skipping CREATE VIEW during replica snapshot")
		return callback(&sqltypes.Result{}, false)
	}
	return err
}

func (h *MyHandler) ComPrepare(ctx context.Context, c *mysql.Conn, query string, prepare *mysql.PrepareData) ([]*querypb.Field, error) {
	if err := h.rejectReadOnly(ctx, c, query); err != nil {
		return nil, err
	}
	return h.Handler.ComPrepare(ctx, c, query, prepare)
}

func (h *MyHandler) ComPrepareParsed(ctx context.Context, c *mysql.Conn, query string, parsed sqlparser.Statement, prepare *mysql.PrepareData) (mysql.ParsedQuery, []*querypb.Field, error) {
	if err := h.rejectReadOnly(ctx, c, query); err != nil {
		return nil, nil, err
	}
	return h.Handler.ComPrepareParsed(ctx, c, query, parsed, prepare)
}

func (h *MyHandler) ComBind(ctx context.Context, c *mysql.Conn, query string, parsedQuery mysql.ParsedQuery, prepare *mysql.PrepareData) (mysql.BoundQuery, []*querypb.Field, error) {
	if err := h.rejectReadOnly(ctx, c, query); err != nil {
		return nil, nil, err
	}
	return h.Handler.ComBind(ctx, c, query, parsedQuery, prepare)
}

func (h *MyHandler) ComExecuteBound(ctx context.Context, c *mysql.Conn, query string, boundQuery mysql.BoundQuery, callback mysql.ResultSpoolFn) error {
	if err := h.rejectReadOnly(ctx, c, query); err != nil {
		return err
	}
	return h.Handler.ComExecuteBound(ctx, c, query, boundQuery, callback)
}

func (h *MyHandler) ComParsedQuery(ctx context.Context, c *mysql.Conn, query string, parsed sqlparser.Statement, callback mysql.ResultSpoolFn) error {
	if err := h.rejectReadOnly(ctx, c, query); err != nil {
		return err
	}
	return h.Handler.ComParsedQuery(ctx, c, query, parsed, callback)
}

func (h *MyHandler) rejectReadOnly(ctx context.Context, c *mysql.Conn, query string) error {
	if !h.readOnly {
		return nil
	}

	sqlCtx, err := h.Handler.NewContext(ctx, c, query)
	if err == nil && h.engine != nil {
		node, analyzeErr := h.engine.AnalyzeQuery(sqlCtx, query)
		if analyzeErr == nil {
			if !plan.IsReadOnly(node) {
				return sql.ErrReadOnly.New()
			}
		}
	}

	if IsWriteQueryText(query) {
		return sql.ErrReadOnly.New()
	}
	return nil
}

func isReplicaLoadingSnapshot() bool {
	_, vv, ok := sql.SystemVariables.GetGlobal("replica_is_loading_snapshot")
	if !ok {
		return false
	}
	switch v := vv.(type) {
	case int8:
		return v != 0
	case bool:
		return v
	default:
		return false
	}
}

func WrapHandler(provider *catalog.DatabaseProvider, engine *sqle.Engine, readOnly bool) server.HandlerWrapper {
	return func(h mysql.Handler) (mysql.Handler, error) {
		handler, ok := h.(*server.Handler)
		if !ok {
			return nil, fmt.Errorf("expected *server.Handler, got %T", h)
		}

		return &MyHandler{
			Handler:  handler,
			provider: provider,
			engine:   engine,
			readOnly: readOnly,
		}, nil
	}
}
